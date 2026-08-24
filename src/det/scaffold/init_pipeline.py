from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from det.logging import get_logger
from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config
from det.runtime.discovery import PluginLoadError
from det.runtime.ids import (
    default_schema_path,
    fs_dataset_parts,
    parse_canonical_id,
    validate_canonical_id,
)
from det.runtime.lake import DEFAULT_LAKE_REL
from det.runtime.registry import get_source, list_sources
from det.runtime.secrets import looks_like_passwordful_uri, looks_like_secret_name
from det.scaffold.dbt import ScaffoldAction, ScaffoldResult, scaffold_dbt

logger = get_logger(__name__)


@dataclass
class InitPipelineResult:
    name: str
    pipeline_path: Path
    schema_path: Path
    actions: list[ScaffoldAction] = field(default_factory=list)
    scaffold: ScaffoldResult | None = None


_MINIMAL_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "description": "Bronze contract stub — extend properties before production use.",
    "type": "object",
    "required": ["id"],
    "properties": {
        "id": {"type": "integer"},
        "payload": {"type": ["string", "null"]},
    },
    "additionalProperties": False,
}


def _postgres_connection_entry(connection: str) -> dict[str, str]:
    """
    Scaffolded Postgres points at a secret name, never a DSN with a password.

    ``--connection DET_POSTGRES_DSN`` writes ``connection_env``; a passwordless
    local DSN is still allowed as a literal.
    """
    value = connection.strip()
    if looks_like_secret_name(value):
        return {"connection_env": value}
    if looks_like_passwordful_uri(value):
        raise ValueError(
            "refusing to write a Postgres DSN with a password into pipeline YAML. "
            "Export it (e.g. DET_POSTGRES_DSN=...) and pass "
            "--connection DET_POSTGRES_DSN"
        )
    return {"connection": value}


def init_pipeline(
    *,
    name: str,
    source_type: str,
    project_root: Path,
    force: bool = False,
    dry_run: bool = False,
    skip_dbt: bool = False,
    destination_type: str = "iceberg",
    lake_path: str | None = None,
    connection: str | None = None,
) -> InitPipelineResult:
    """
    Greenfield pipeline: YAML + minimal schema + optional scaffold-dbt.

    ``name`` and ``source_type`` must be the same canonical ``provider.source`` id.
    """
    load_plugins()
    name = validate_canonical_id(name)
    source_type = validate_canonical_id(source_type)
    if name != source_type:
        raise ValueError(
            f"name {name!r} must equal source_type {source_type!r} "
            "(canonical provider.source id)"
        )
    root = project_root.resolve()
    known = set(list_sources(project_root=root))
    if source_type not in known:
        raise ValueError(
            f"Unknown source type {source_type!r}. Registered: {sorted(known)}"
        )
    try:
        get_source(source_type, project_root=root)
    except PluginLoadError as exc:
        raise ValueError(str(exc)) from exc

    provider, source_name = parse_canonical_id(name)
    parts = fs_dataset_parts(name)
    pipeline_path = root.joinpath("configs", "pipelines", *parts[:-1], f"{parts[-1]}.yaml")
    schema_rel = default_schema_path(name)
    schema_path = root / schema_rel
    actions: list[ScaffoldAction] = []

    dest: dict = {"type": destination_type}
    if destination_type == "iceberg":
        # Default extract_run (identity on __extract_run_datetime). Small /
        # reference sources should set partition: none.
        dest["partition"] = "extract_run"
    if lake_path and lake_path.strip() and lake_path.strip() not in {
        DEFAULT_LAKE_REL,
        "data/lake",
    }:
        dest["path"] = lake_path.strip()
    if destination_type in {"duckdb", "postgres"}:
        if not connection:
            raise ValueError(
                f"connection is required when destination_type={destination_type}"
            )
        if destination_type == "postgres":
            dest.update(_postgres_connection_entry(connection))
        else:
            dest["connection"] = connection
        dest["dataset"] = "bronze"  # medallion prefix → SQL schema bronze_{provider}

    pipeline_doc = {
        "name": name,
        "source": {"type": source_type},
        # schema omitted → default_schema_path
        "validation": {"engine": "jsonschema"},
        "ingestion": {"library": "det"},
        "destination": dest,
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
        # Bump only on true wire breaks (lake id becomes {name}_vN automatically)
        "wire_version": 1,
        "dbt": {
            "silver": {
                "materialized": "table",
                "unique_key": ["__row_hash"],
                "order_by": ["__extract_run_datetime desc"],
            },
            # Optional stg adaptations (coalesce/rename/…); see det-dbt skill
            "stg": {},
        },
    }
    schema_doc = {**_MINIMAL_SCHEMA, "$title": f"{provider}.{source_name}"}

    for path, content, kind in (
        (pipeline_path, yaml.safe_dump(pipeline_doc, sort_keys=False), "pipeline"),
        (schema_path, yaml.safe_dump(schema_doc, sort_keys=False), "schema"),
    ):
        if kind == "pipeline" and destination_type == "iceberg":
            content = content.replace(
                "  partition: extract_run\n",
                "  # Small / reference sources: set partition: none\n"
                "  partition: extract_run\n",
                1,
            )
        exists = path.exists()
        if exists and not force:
            actions.append(ScaffoldAction(path=path, action="skip", detail=f"{kind} exists"))
            continue
        detail = "overwrite" if exists else "create"
        if dry_run:
            actions.append(ScaffoldAction(path=path, action="would_write", detail=detail))
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        actions.append(ScaffoldAction(path=path, action="write", detail=detail))
        logger.info("init-pipeline wrote", path=str(path), kind=kind)

    scaffold_result = None
    if not skip_dbt:
        if dry_run:
            actions.append(
                ScaffoldAction(
                    path=root / "dbt" / "models" / "silver",
                    action="would_write",
                    detail="scaffold-dbt",
                )
            )
        else:
            config = load_pipeline_config(pipeline_path)
            scaffold_result = scaffold_dbt(
                config, project_root=root, force=force, dry_run=False
            )
            actions.extend(scaffold_result.actions)

    return InitPipelineResult(
        name=name,
        pipeline_path=pipeline_path,
        schema_path=schema_path,
        actions=actions,
        scaffold=scaffold_result,
    )
