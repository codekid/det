from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

from det.logging import get_logger
from det.runtime.config import DbtSilverConfig, PipelineConfig, resolve_path
from det.runtime.ids import dbt_model_slug, parse_canonical_id, sql_names_for_config
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)

_META_COLUMNS = [
    "__row_hash",
    "__filename",
    "__extract_run_datetime",
    "__interval_start_datetime",
    "__interval_end_datetime",
    "__data_interval_date",
]

_TEMPLATES = Path(__file__).resolve().parent / "templates"


@dataclass
class ScaffoldAction:
    path: Path
    action: str  # write | skip | would_write | would_patch
    detail: str = ""


@dataclass
class ScaffoldResult:
    dataset: str
    actions: list[ScaffoldAction] = field(default_factory=list)

    @property
    def written(self) -> list[Path]:
        return [a.path for a in self.actions if a.action == "write"]


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(_TEMPLATES)),
        autoescape=select_autoescape(enabled_extensions=()),
        keep_trailing_newline=True,
    )


def _allowed_types(prop: dict[str, Any]) -> set[str]:
    raw = prop.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {t for t in raw if isinstance(t, str)}
    return set()


def _stg_column_expr(name: str, prop: dict[str, Any]) -> str:
    allowed = _allowed_types(prop)
    # Prefer string cleaning when string is among allowed types.
    if "string" in allowed:
        return f"nullif(trim(cast({name} as varchar)), '') as {name}"
    return name


def stg_columns_from_schema(schema: dict[str, Any]) -> list[dict[str, str]]:
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}
    columns: list[dict[str, str]] = []
    for name, prop in props.items():
        if not isinstance(name, str):
            continue
        if not isinstance(prop, dict):
            columns.append({"name": name, "expr": name})
            continue
        columns.append({"name": name, "expr": _stg_column_expr(name, prop)})
    for meta in _META_COLUMNS:
        columns.append({"name": meta, "expr": meta})
    return columns


def _render(name: str, **ctx: Any) -> str:
    return _env().get_template(name).render(**ctx)


def _write_or_skip(
    path: Path,
    content: str,
    *,
    force: bool,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    path = path.resolve()
    exists = path.exists()
    if exists and not force:
        actions.append(ScaffoldAction(path=path, action="skip", detail="exists"))
        return
    if dry_run:
        actions.append(
            ScaffoldAction(
                path=path,
                action="would_write",
                detail="overwrite" if exists else "create",
            )
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    actions.append(
        ScaffoldAction(
            path=path,
            action="write",
            detail="overwrite" if exists else "create",
        )
    )
    logger.info("scaffolded file", path=str(path), detail=actions[-1].detail)


def _table_entry_yaml(table: str, required: list[str], *, description: str) -> str:
    columns: list[dict[str, Any]] = [{"name": "__row_hash", "tests": ["not_null"]}]
    for col in required:
        if col == "__row_hash":
            continue
        columns.append({"name": col, "tests": ["not_null"]})
    entry = {
        "name": table,
        "description": description,
        "columns": columns,
    }
    # Indent as a list item under `tables:`.
    dumped = yaml.safe_dump([entry], sort_keys=False, default_flow_style=False).rstrip()
    return "\n".join("      " + line if line else line for line in dumped.splitlines())


def _sources_has_table(text: str, table: str) -> bool:
    return re.search(rf"(?m)^\s+- name:\s*{re.escape(table)}\s*$", text) is not None


def _sources_has_source(text: str, source_name: str) -> bool:
    return re.search(
        rf"(?m)^\s+- name:\s*{re.escape(source_name)}\s*$", text
    ) is not None


def _merge_sources_table(
    sources_path: Path,
    *,
    source_name: str,
    provider: str,
    table: str,
    required: list[str],
    force: bool,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    """
    Merge a bronze table into sources.yml under ``bronze_{provider}``.

    sources.yml may contain dbt Jinja, so we avoid a full YAML round-trip and
    append/replace table blocks as text.
    """
    table_yaml = _table_entry_yaml(
        table,
        required,
        description=f"DET bronze {provider}.{table}",
    )

    if not sources_path.exists():
        content = _render(
            "sources.yml.j2",
            source_name=source_name,
            provider=provider,
            tables_yaml=table_yaml + "\n",
        )
        detail = "create sources.yml"
        if dry_run:
            actions.append(ScaffoldAction(path=sources_path, action="would_write", detail=detail))
            return
        sources_path.parent.mkdir(parents=True, exist_ok=True)
        sources_path.write_text(content, encoding="utf-8")
        actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
        logger.info("scaffolded sources.yml", path=str(sources_path), detail=detail)
        return

    text = sources_path.read_text(encoding="utf-8")
    if not _sources_has_source(text, source_name):
        block = _render(
            "sources.yml.j2",
            source_name=source_name,
            provider=provider,
            tables_yaml=table_yaml + "\n",
        )
        # Append only the source entry (skip version: 2 / sources: header).
        source_only = block.split("sources:\n", 1)[-1]
        text = text.rstrip() + "\n" + source_only
        detail = f"add source {source_name}"
        if dry_run:
            actions.append(ScaffoldAction(path=sources_path, action="would_patch", detail=detail))
            return
        sources_path.write_text(text, encoding="utf-8")
        actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
        return

    if _sources_has_table(text, table):
        if not force:
            actions.append(
                ScaffoldAction(path=sources_path, action="skip", detail=f"table {table} exists")
            )
            return
        pattern = re.compile(
            rf"(?ms)^(?P<indent>\s*)- name:\s*{re.escape(table)}\s*\n"
            rf"(?:(?!\s*- name:).*\n)*"
        )
        new_text, n = pattern.subn(table_yaml + "\n", text, count=1)
        if n == 0:
            actions.append(
                ScaffoldAction(
                    path=sources_path,
                    action="skip",
                    detail=f"could not replace table {table}",
                )
            )
            return
        detail = "replace table"
        text = new_text
    else:
        if not re.search(r"(?m)^\s*tables:\s*$", text):
            text = text.rstrip() + "\n    tables:\n"
        text = text.rstrip() + "\n" + table_yaml + "\n"
        detail = "add table"

    if dry_run:
        actions.append(ScaffoldAction(path=sources_path, action="would_patch", detail=detail))
        return

    sources_path.write_text(text, encoding="utf-8")
    actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
    logger.info("scaffolded sources.yml", path=str(sources_path), detail=detail)


def _merge_silver_models_yml(
    models_yml: Path,
    *,
    dataset: str,
    unique_key: list[str],
    required: list[str],
    force: bool,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    model_name = f"silver_{dataset}"
    if models_yml.exists():
        doc = yaml.safe_load(models_yml.read_text(encoding="utf-8")) or {}
    else:
        doc = {"version": 2, "models": []}

    models = doc.setdefault("models", [])
    existing = next(
        (m for m in models if isinstance(m, dict) and m.get("name") == model_name), None
    )

    columns: list[dict[str, Any]] = []
    for key in unique_key:
        tests = ["not_null"]
        if len(unique_key) == 1:
            tests.insert(0, "unique")
        columns.append({"name": key, "tests": tests})
    for col in required:
        if col in unique_key:
            continue
        columns.append({"name": col, "tests": ["not_null"]})
    if "__row_hash" not in unique_key and "__row_hash" not in required:
        columns.append({"name": "__row_hash", "tests": ["not_null"]})

    entry = {
        "name": model_name,
        "description": f"Cleaned, deduped {dataset} (silver)",
        "columns": columns,
    }

    if existing is not None and not force:
        actions.append(
            ScaffoldAction(path=models_yml, action="skip", detail=f"model {model_name} exists")
        )
        return

    if existing is None:
        models.append(entry)
        detail = "add model"
    else:
        models[models.index(existing)] = entry
        detail = "replace model"

    if dry_run:
        actions.append(ScaffoldAction(path=models_yml, action="would_patch", detail=detail))
        return

    models_yml.parent.mkdir(parents=True, exist_ok=True)
    models_yml.write_text(
        yaml.safe_dump(doc, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    actions.append(ScaffoldAction(path=models_yml, action="write", detail=detail))


def scaffold_dbt(
    config: PipelineConfig,
    *,
    project_root: Path,
    force: bool = False,
    dry_run: bool = False,
    dbt_models_dir: Path | None = None,
) -> ScaffoldResult:
    """
    Emit/merge dbt bronze source + stg + silver for a pipeline dataset.

    Create-if-missing by default; `--force` overwrites generated SQL and refreshes
    YAML entries for the dataset.
    """
    root = project_root.resolve()
    canonical = config.bronze_dataset()
    model_slug = dbt_model_slug(canonical)
    provider, _ = parse_canonical_id(canonical)
    sql_schema, sql_table = sql_names_for_config(config)
    silver: DbtSilverConfig = config.dbt.silver
    schema = load_json_schema(resolve_path(root, config.schema_path))
    required = [c for c in (schema.get("required") or []) if isinstance(c, str)]
    columns = stg_columns_from_schema(schema)

    models_dir = (dbt_models_dir or (root / "dbt" / "models" / "silver")).resolve()
    actions: list[ScaffoldAction] = []

    stg_sql = _render(
        "stg.sql.j2",
        sql_table=sql_table,
        columns=columns,
    )
    silver_sql = _render(
        "silver.sql.j2",
        model_slug=model_slug,
        silver=silver,
    )

    _write_or_skip(
        models_dir / f"stg_{model_slug}.sql",
        stg_sql,
        force=force,
        dry_run=dry_run,
        actions=actions,
    )
    _write_or_skip(
        models_dir / f"silver_{model_slug}.sql",
        silver_sql,
        force=force,
        dry_run=dry_run,
        actions=actions,
    )
    _merge_sources_table(
        models_dir / "sources.yml",
        source_name=sql_schema,
        provider=provider,
        table=sql_table,
        required=required,
        force=force,
        dry_run=dry_run,
        actions=actions,
    )
    _merge_silver_models_yml(
        models_dir / "_silver__models.yml",
        dataset=model_slug,
        unique_key=list(silver.unique_key),
        required=required,
        force=force,
        dry_run=dry_run,
        actions=actions,
    )

    return ScaffoldResult(dataset=canonical, actions=actions)
