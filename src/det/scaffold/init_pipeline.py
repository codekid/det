from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from det.logging import get_logger
from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config
from det.runtime.ids import (
    default_schema_path,
    fs_dataset_parts,
    parse_canonical_id,
    validate_canonical_id,
)
from det.runtime.registry import list_sources
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


def init_pipeline(
    *,
    name: str,
    source_type: str,
    project_root: Path,
    force: bool = False,
    dry_run: bool = False,
    skip_dbt: bool = False,
    destination_type: str = "filesystem",
    lake_path: str = "./data/lake",
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
    known = set(list_sources())
    if source_type not in known:
        raise ValueError(
            f"Unknown source type {source_type!r}. Registered: {sorted(known)}"
        )

    root = project_root.resolve()
    provider, source_name = parse_canonical_id(name)
    parts = fs_dataset_parts(name)
    pipeline_path = root.joinpath("configs", "pipelines", *parts[:-1], f"{parts[-1]}.yaml")
    schema_rel = default_schema_path(name)
    schema_path = root / schema_rel
    actions: list[ScaffoldAction] = []

    dest: dict = {"type": destination_type, "path": lake_path}
    if destination_type in {"duckdb", "postgres"}:
        if not connection:
            raise ValueError(
                f"connection is required when destination_type={destination_type}"
            )
        dest["connection"] = connection
        dest["dataset"] = "bronze"  # medallion prefix → SQL schema bronze_{provider}

    pipeline_doc = {
        "name": name,
        "source": {"type": source_type},
        # schema omitted → default_schema_path
        "validation": {"engine": "jsonschema"},
        "ingestion": {"library": "dlt"},
        "destination": dest,
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
        "dbt": {
            "silver": {
                "materialized": "table",
                "unique_key": ["__row_hash"],
                "order_by": ["__extract_run_datetime desc"],
            }
        },
    }
    schema_doc = {**_MINIMAL_SCHEMA, "$title": f"{provider}.{source_name}"}

    for path, content, kind in (
        (pipeline_path, yaml.safe_dump(pipeline_doc, sort_keys=False), "pipeline"),
        (schema_path, yaml.safe_dump(schema_doc, sort_keys=False), "schema"),
    ):
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
