from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

from det.logging import get_logger
from det.runtime.config import (
    DbtSilverConfig,
    DbtStgConfig,
    PipelineConfig,
    resolve_path,
)
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

# DuckDB types for DET meta columns in schema-aware read_json(... columns={}).
_META_DUCKDB_TYPES = {
    "__row_hash": "VARCHAR",
    "__filename": "VARCHAR",
    "__extract_run_datetime": "TIMESTAMP",
    "__interval_start_datetime": "TIMESTAMP",
    "__interval_end_datetime": "TIMESTAMP",
    "__data_interval_date": "DATE",
}

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


def duckdb_type_for_prop(prop: dict[str, Any]) -> str:
    """Map a JSON Schema property to a DuckDB read_json column type."""
    allowed = _allowed_types(prop)
    if "integer" in allowed:
        return "INTEGER"
    if "number" in allowed:
        return "DOUBLE"
    if "boolean" in allowed:
        return "BOOLEAN"
    if "string" in allowed:
        return "VARCHAR"
    return "VARCHAR"


def read_json_columns_from_schema(schema: dict[str, Any]) -> dict[str, str]:
    """Build DuckDB ``columns={...}`` map from a DET bronze JSON Schema."""
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}
    columns: dict[str, str] = {}
    for name, prop in props.items():
        if not isinstance(name, str):
            continue
        if isinstance(prop, dict):
            columns[name] = duckdb_type_for_prop(prop)
        else:
            columns[name] = "VARCHAR"
    columns.update(_META_DUCKDB_TYPES)
    return columns


def widen_read_json_columns(
    schema: dict[str, Any],
    stg: DbtStgConfig,
) -> dict[str, str]:
    """
    Schema columns plus historical names from ``dbt.stg`` so coalesce/exclude
    sources remain visible to ``read_json(..., columns={...})``.
    """
    columns = read_json_columns_from_schema(schema)
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}

    def _dtype_for(canonical: str, name: str) -> str:
        if name in columns:
            return columns[name]
        prop = props.get(canonical)
        if isinstance(prop, dict):
            return duckdb_type_for_prop(prop)
        prop = props.get(name)
        if isinstance(prop, dict):
            return duckdb_type_for_prop(prop)
        return "VARCHAR"

    for canonical, sources in stg.coalesce.items():
        for src in sources:
            columns.setdefault(src, _dtype_for(canonical, src))
        columns.setdefault(canonical, _dtype_for(canonical, canonical))
    for name in stg.exclude:
        columns.setdefault(name, _dtype_for(name, name))
    for name in stg.null_sentinels:
        columns.setdefault(name, _dtype_for(name, name))
    for name in stg.map:
        columns.setdefault(name, _dtype_for(name, name))
    for name in stg.rename:
        columns.setdefault(name, _dtype_for(name, name))
    columns.update(_META_DUCKDB_TYPES)
    return columns


def format_read_json_columns(columns: dict[str, str]) -> str:
    """Format a DuckDB columns struct literal: ``{'a': 'INTEGER', ...}``."""
    inner = ", ".join(f"'{key}': '{dtype}'" for key, dtype in columns.items())
    return "{" + inner + "}"


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _cast_macro_call(name: str, prop: dict[str, Any] | None) -> str:
    """dbt Jinja cast macro call without alias (``{{ det_as_string('x') }}``)."""
    allowed = _allowed_types(prop) if prop else set()
    if "integer" in allowed:
        return f"{{{{ det_as_integer('{name}') }}}}"
    if "number" in allowed:
        return f"{{{{ det_as_double('{name}') }}}}"
    if "boolean" in allowed:
        return f"{{{{ det_as_boolean('{name}') }}}}"
    if "string" in allowed or not allowed:
        return f"{{{{ det_as_string('{name}') }}}}"
    return f"{{{{ det_as_string('{name}') }}}}"


def _stg_column_expr(name: str, prop: dict[str, Any]) -> str:
    """
    Typed stg select expr using det_as_* macros (JSON-safe casts).

    Emitted as dbt Jinja (``{{ det_as_string(col) }}``) so macros expand at
    compile time.
    """
    return f"{_cast_macro_call(name, prop)} as {name}"


def _apply_null_sentinels(expr: str, sentinels: list[Any]) -> str:
    out = expr
    for sentinel in sentinels:
        if sentinel is None:
            continue
        if isinstance(sentinel, bool):
            lit = "true" if sentinel else "false"
        elif isinstance(sentinel, (int, float)) and not isinstance(sentinel, bool):
            lit = str(sentinel)
        else:
            lit = _sql_string_literal(str(sentinel))
        out = f"nullif({out}, {lit})"
    return out


def _apply_value_map(expr: str, mapping: dict[str, str]) -> str:
    whens = [
        f"when ({expr}) = {_sql_string_literal(src)} then {_sql_string_literal(dest)}"
        for src, dest in mapping.items()
    ]
    body = " ".join(whens)
    return f"case {body} else ({expr}) end"


def stg_columns_from_schema(
    schema: dict[str, Any],
    stg: DbtStgConfig | None = None,
) -> list[dict[str, str]]:
    """
    Build stg select column exprs from schema + optional ``dbt.stg`` adaptations.
    """
    stg = stg or DbtStgConfig()
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}

    # Business columns: schema props ∪ coalesce canonicals, minus exclude.
    names: list[str] = []
    seen: set[str] = set()
    for name in props:
        if isinstance(name, str) and name not in seen:
            names.append(name)
            seen.add(name)
    for canonical in stg.coalesce:
        if canonical not in seen:
            names.append(canonical)
            seen.add(canonical)

    exclude = set(stg.exclude)
    columns: list[dict[str, str]] = []
    for name in names:
        if name in exclude:
            continue
        prop = props.get(name) if isinstance(props.get(name), dict) else None
        sources = stg.coalesce.get(name)
        if sources:
            cast_prop = prop
            if cast_prop is None:
                for src in sources:
                    if isinstance(props.get(src), dict):
                        cast_prop = props[src]  # type: ignore[assignment]
                        break
            parts = [
                _cast_macro_call(
                    src,
                    props.get(src) if isinstance(props.get(src), dict) else cast_prop,
                )
                for src in sources
            ]
            expr = f"coalesce({', '.join(parts)})"
        elif isinstance(prop, dict):
            expr = _cast_macro_call(name, prop)
        else:
            expr = _cast_macro_call(name, {"type": "string"})

        if name in stg.null_sentinels:
            expr = _apply_null_sentinels(expr, stg.null_sentinels[name])
        if name in stg.map:
            expr = _apply_value_map(expr, stg.map[name])
        out_name = stg.rename.get(name, name)
        columns.append({"name": out_name, "expr": f"{expr} as {out_name}"})

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


def _table_entry_yaml(
    table: str,
    required: list[str],
    *,
    description: str,
    provider: str,
    columns_struct: str,
) -> str:
    """
    Emit a sources.yml table block as text (not yaml.dump).

    ``external_location`` embeds dbt Jinja; a YAML round-trip mangles quotes.
    """
    columns: list[dict[str, Any]] = [{"name": "__row_hash", "tests": ["not_null"]}]
    for col in required:
        if col == "__row_hash":
            continue
        columns.append({"name": col, "tests": ["not_null"]})
    cols_yaml = yaml.safe_dump(
        columns, sort_keys=False, default_flow_style=False
    ).rstrip()
    cols_indented = "\n".join(
        "        " + line if line else line for line in cols_yaml.splitlines()
    )

    # Match existing lake path Jinja; path is concrete per table (not {name}).
    lake_jinja = '{{ env_var("DET_LAKE_PATH", "../data/lake") }}'
    lines = [
        f"      - name: {table}",
        f"        description: {description}",
        "        meta:",
        "          # Schema-aware columns avoid read_json_auto promoting mixed",
        "          # partitions to JSON (values that look like \"48\" / \"\").",
        "          # formatter=template: columns={...} braces must not go through",
        "          # str.format_map (dbt-duckdb default newstyle).",
        "          # hive_partitioning=false: partition keys are also payload cols.",
        "          formatter: template",
        "          external_location: >-",
        "            read_json(",
        f"              '{lake_jinja}/bronze/{provider}/{table}/**/data.jsonl',",
        f"              columns={columns_struct},",
        "              union_by_name=true,",
        "              hive_partitioning=false",
        "            )",
        "        columns:",
        cols_indented,
    ]
    return "\n".join(lines)


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
    columns_struct: str,
    force: bool,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    """
    Merge a bronze table into sources.yml under ``bronze_{provider}``.

    sources.yml may contain dbt Jinja, so we avoid a full YAML round-trip and
    append/replace table blocks as text. Each table gets a schema-aware
    ``read_json(..., columns={...})`` external_location.
    """
    table_yaml = _table_entry_yaml(
        table,
        required,
        description=f"DET bronze {provider}.{table}",
        provider=provider,
        columns_struct=columns_struct,
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
    # Drop legacy source-level read_json_auto once tables carry typed locations.
    if "external_location:" in text and "read_json_auto(" in text:
        text = re.sub(
            r"(?ms)^(?P<indent>\s*)meta:\n"
            r"(?:(?P=indent)  .*\n)*?"
            r"(?P=indent)  external_location:.*\n"
            r"(?:(?P=indent)  .*\n)*",
            "",
            text,
            count=1,
        )
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
        # Stop at the next peer table (same indent) or a less-indented source
        # entry — not at more-indented ``columns: - name:`` items.
        start_m = re.search(
            rf"(?m)^(?P<indent>\s*)- name:\s*{re.escape(table)}\s*$", text
        )
        if start_m is None:
            actions.append(
                ScaffoldAction(
                    path=sources_path,
                    action="skip",
                    detail=f"could not replace table {table}",
                )
            )
            return
        indent = start_m.group("indent")
        indent_len = len(indent)
        pattern = re.compile(
            rf"(?ms)^{re.escape(indent)}- name:\s*{re.escape(table)}\s*\n"
            rf"(?:(?!^\s{{0,{indent_len}}}- name:).*\n)*"
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


def _add_col_test(by_col: dict[str, list[Any]], name: str, test: Any) -> None:
    tests = by_col.setdefault(name, [])
    if test == "unique":
        if "unique" not in tests:
            tests.insert(0, "unique")
        return
    if test == "not_null":
        if "not_null" not in tests:
            tests.append("not_null")
        return
    tests.append(test)


def _merge_silver_models_yml(
    models_yml: Path,
    *,
    dataset: str,
    silver: DbtSilverConfig,
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

    by_col: dict[str, list[Any]] = {}
    unique_key = list(silver.unique_key)
    for key in unique_key:
        _add_col_test(by_col, key, "not_null")
        if len(unique_key) == 1:
            _add_col_test(by_col, key, "unique")
    for col in required:
        _add_col_test(by_col, col, "not_null")
    if "__row_hash" not in by_col:
        _add_col_test(by_col, "__row_hash", "not_null")
    for col in silver.not_null:
        _add_col_test(by_col, col, "not_null")
    for col in silver.unique:
        _add_col_test(by_col, col, "unique")
    for col, values in silver.accepted_values.items():
        _add_col_test(
            by_col, col, {"accepted_values": {"values": list(values)}}
        )

    entry = {
        "name": model_name,
        "description": f"Cleaned, deduped {dataset} (silver)",
        "columns": [{"name": name, "tests": tests} for name, tests in by_col.items()],
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
    stg_cfg: DbtStgConfig = config.dbt.stg
    schema = load_json_schema(resolve_path(root, config.schema_path))
    required = [c for c in (schema.get("required") or []) if isinstance(c, str)]
    columns = stg_columns_from_schema(schema, stg_cfg)
    columns_struct = format_read_json_columns(
        widen_read_json_columns(schema, stg_cfg)
    )

    models_dir = (dbt_models_dir or (root / "dbt" / "models" / "silver")).resolve()
    actions: list[ScaffoldAction] = []

    stg_sql = _render(
        "stg.sql.j2",
        sql_table=sql_table,
        sql_schema=sql_schema,
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
        columns_struct=columns_struct,
        force=force,
        dry_run=dry_run,
        actions=actions,
    )
    _merge_silver_models_yml(
        models_dir / "_silver__models.yml",
        dataset=model_slug,
        silver=silver,
        required=required,
        force=force,
        dry_run=dry_run,
        actions=actions,
    )

    return ScaffoldResult(dataset=canonical, actions=actions)
