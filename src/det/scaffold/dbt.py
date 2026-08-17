from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

from det.logging import get_logger
from det.runtime.config import (
    DbtDocsConfig,
    DbtSilverConfig,
    DbtStgConfig,
    PipelineConfig,
    RelationConfig,
    resolve_path,
)
from det.runtime.ids import dbt_model_slug, parse_canonical_id, sql_names_for_config
from det.runtime.sql_types import (
    META_SQL_TYPES_DUCKDB,
    bronze_sql_columns,
    duckdb_type_for_prop,
)
from det.scaffold.adapt_scope import (
    FlatAdapt,
    compile_relation_adapt,
    compile_stg_fields,
    flat_adapt_from_root_stg,
    merge_flat_adapt,
)
from det.scaffold.flatten import (
    FlattenLeaf,
    detect_flatten_collisions,
    flattened_roots,
    is_array_prop,
    is_object_prop,
    iter_relation_paths,
    plan_flatten,
    relation_item_schema_at,
)
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)

_META_COLUMNS = [
    "__row_hash",
    "__filename",
    "__extract_run_datetime",
    "__bronze_loaded_at",
    "__interval_start_datetime",
    "__interval_end_datetime",
    "__data_interval_date",
]

# DuckDB types for DET meta columns in schema-aware read_json(... columns={}).
_META_DUCKDB_TYPES = META_SQL_TYPES_DUCKDB

_META_COLUMN_DESCRIPTIONS = {
    "__row_hash": "DET content hash used for silver dedupe.",
    "__filename": "Source artifact filename when extract recorded one.",
    "__extract_run_datetime": "Extract run timestamp (UTC).",
    "__bronze_loaded_at": "When the row was written to bronze (UTC).",
    "__interval_start_datetime": "Pipeline interval start (UTC).",
    "__interval_end_datetime": "Pipeline interval end (UTC).",
    "__data_interval_date": "Calendar date of the interval start.",
}

_TEMPLATES = Path(__file__).resolve().parent / "templates"


def schema_root_description(schema: dict[str, Any]) -> str | None:
    raw = schema.get("description")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def schema_column_descriptions(schema: dict[str, Any]) -> dict[str, str]:
    """Map bronze property name → description from JSON Schema properties."""
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        return {}
    out: dict[str, str] = {}
    for name, prop in props.items():
        if not isinstance(name, str) or not isinstance(prop, dict):
            continue
        desc = prop.get("description")
        if isinstance(desc, str) and desc.strip():
            out[name] = desc.strip()
    return out


def post_stg_description_map(
    schema_descs: dict[str, str],
    stg: DbtStgConfig,
) -> dict[str, str]:
    """
    Remap schema descriptions onto post-stg column names (top-level only).

    Coalesce: ensure the coalesce key has a description (prefer its own, else
    first source with a desc). Rename: move desc from source → target. Exclude:
    drop aliases from the map.
    """
    out = dict(schema_descs)
    for canonical, sources in stg.coalesce.items():
        if canonical in out:
            continue
        for src in sources:
            if src in out:
                out[canonical] = out[src]
                break
    for src, dest in stg.rename.items():
        if src in out:
            out[dest] = out.pop(src)
    for name in stg.exclude:
        out.pop(name, None)
    return out


def _yaml_block(mapping: dict[str, Any], *, indent: int) -> str:
    """Dump a small mapping and indent every line for embedding in sources.yml."""
    raw = yaml.safe_dump(
        mapping,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=1000,
    ).rstrip()
    prefix = " " * indent
    return "\n".join(prefix + line if line else line for line in raw.splitlines())


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


def read_json_columns_from_schema(schema: dict[str, Any]) -> dict[str, str]:
    """Build DuckDB ``columns={...}`` map from a DET bronze JSON Schema."""
    return dict(bronze_sql_columns(schema, "duckdb"))


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
    if prop is not None and (is_object_prop(prop) or is_array_prop(prop)):
        return name
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


def _path_cast_macro_call(
    root_col: str, extract_path: str, prop: dict[str, Any] | None
) -> str:
    """dbt Jinja path extract + cast (``{{ det_json_path_string('a', '$.b') }}``)."""
    allowed = _allowed_types(prop) if prop else set()
    path_lit = extract_path.replace("'", "''")
    if "integer" in allowed:
        return f"{{{{ det_json_path_integer('{root_col}', '{path_lit}') }}}}"
    if "number" in allowed:
        return f"{{{{ det_json_path_double('{root_col}', '{path_lit}') }}}}"
    if "boolean" in allowed:
        return f"{{{{ det_json_path_boolean('{root_col}', '{path_lit}') }}}}"
    return f"{{{{ det_json_path_string('{root_col}', '{path_lit}') }}}}"


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


def _adapt_column_expr(
    logical_name: str,
    expr: str,
    adapt: FlatAdapt,
) -> tuple[str, str]:
    """Apply null_sentinels → map → rename; return (out_name, full select expr)."""
    if logical_name in adapt.null_sentinels:
        expr = _apply_null_sentinels(expr, adapt.null_sentinels[logical_name])
    if logical_name in adapt.map:
        expr = _apply_value_map(expr, adapt.map[logical_name])
    out_name = adapt.rename.get(logical_name, logical_name)
    return out_name, f"{expr} as {out_name}"


def _parent_flat_adapt(stg: DbtStgConfig) -> FlatAdapt:
    """Compiled ``dbt.stg.fields`` scopes, then root scalar knobs (root wins ties)."""
    return merge_flat_adapt(
        compile_stg_fields(stg.fields),
        flat_adapt_from_root_stg(
            coalesce=stg.coalesce,
            null_sentinels=stg.null_sentinels,
            map=stg.map,
            rename=stg.rename,
            exclude=stg.exclude,
        ),
    )


def default_parent_key(
    schema: dict[str, Any],
    silver: DbtSilverConfig,
    explicit: str | None,
) -> str:
    if explicit:
        return explicit
    for key in silver.unique_key:
        if isinstance(key, str) and not key.startswith("__"):
            return key
    for col in schema.get("required") or []:
        if isinstance(col, str) and not col.startswith("__"):
            return col
    raise ValueError(
        "cannot determine relations parent_key; set dbt.stg.relations.*.parent_key "
        "or a non-meta dbt.silver.unique_key"
    )


def _leaf_or_top_expr(
    name: str,
    *,
    leaf_by_name: dict[str, FlattenLeaf],
    props: dict[str, Any],
) -> str:
    leaf = leaf_by_name.get(name)
    if leaf is not None:
        return _path_cast_macro_call(leaf.root, leaf.extract_path, leaf.prop)
    prop = props.get(name) if isinstance(props.get(name), dict) else None
    return _cast_macro_call(name, prop or {"type": "string"})


def stg_columns_from_schema(
    schema: dict[str, Any],
    stg: DbtStgConfig | None = None,
) -> list[dict[str, str]]:
    """
    Build stg select column exprs from schema + optional ``dbt.stg`` adaptations.

    Order: flatten → coalesce → null_sentinels → map → rename → exclude.
    """
    stg = stg or DbtStgConfig()
    adapt = _parent_flat_adapt(stg)
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}

    relation_paths = {rel.path or name for name, rel in stg.relations.items()}
    leaves = plan_flatten(
        schema,
        stg.flatten,
        relation_paths=relation_paths,
    )
    flat_roots = flattened_roots(leaves)
    reserved: set[str] = set()
    for name, prop in props.items():
        if not isinstance(name, str):
            continue
        if name in relation_paths or name in flat_roots:
            continue
        reserved.add(name)
    leaf_by_name = {leaf.column_name: leaf for leaf in leaves}
    # Coalesce onto an existing flatten leaf is an adaptation, not a name collision.
    for canonical in adapt.coalesce:
        if canonical not in leaf_by_name:
            reserved.add(canonical)
    detect_flatten_collisions(leaves, reserved=reserved)

    columns: list[dict[str, str]] = []
    seen_out: set[str] = set()

    def _append(logical_name: str, expr: str) -> None:
        if logical_name in adapt.exclude:
            return
        out_name, full = _adapt_column_expr(logical_name, expr, adapt)
        if out_name in seen_out:
            return
        seen_out.add(out_name)
        columns.append({"name": out_name, "expr": full})

    # Top-level scalars / undeclared arrays / coalesce canonicals (not flattened roots).
    names: list[str] = []
    seen: set[str] = set()
    for name in props:
        if isinstance(name, str) and name not in seen:
            names.append(name)
            seen.add(name)
    for canonical in adapt.coalesce:
        if canonical not in seen and canonical not in leaf_by_name:
            names.append(canonical)
            seen.add(canonical)

    for name in names:
        if name in relation_paths or name in flat_roots:
            continue
        prop = props.get(name) if isinstance(props.get(name), dict) else None
        sources = adapt.coalesce.get(name)
        if sources:
            cast_prop = prop
            if cast_prop is None:
                for src in sources:
                    if isinstance(props.get(src), dict):
                        cast_prop = props[src]  # type: ignore[assignment]
                        break
            parts = [
                _leaf_or_top_expr(src, leaf_by_name=leaf_by_name, props=props)
                for src in sources
            ]
            expr = f"coalesce({', '.join(parts)})"
        elif isinstance(prop, dict):
            expr = _cast_macro_call(name, prop)
        else:
            expr = _cast_macro_call(name, {"type": "string"})
        _append(name, expr)

    # Flattened nested scalars (after top-level; adaptations use default __ names).
    for leaf in leaves:
        sources = adapt.coalesce.get(leaf.column_name)
        if sources:
            parts = [
                _leaf_or_top_expr(src, leaf_by_name=leaf_by_name, props=props)
                for src in sources
            ]
            expr = f"coalesce({', '.join(parts)})"
        else:
            expr = _path_cast_macro_call(leaf.root, leaf.extract_path, leaf.prop)
        _append(leaf.column_name, expr)

    for meta in _META_COLUMNS:
        columns.append({"name": meta, "expr": meta})
    return columns


def relation_index_columns(depth: int) -> list[str]:
    """Index column names for a relation path_chain of length ``depth``."""
    if depth < 1:
        raise ValueError("relation depth must be >= 1")
    if depth == 1:
        return ["__rel_index"]
    return [f"__rel_index_{i}" for i in range(depth)]


def relation_stg_columns(
    schema: dict[str, Any],
    *,
    path_chain: list[str],
    relation: RelationConfig,
    parent_key: str,
    stg: DbtStgConfig,
    index_columns: list[str] | None = None,
) -> list[dict[str, str]]:
    """Column exprs for a relation stg model (parent_key + indexes + flatten + meta)."""
    item_schema = relation_item_schema_at(schema, path_chain)
    nested_paths = {child.path or name for name, child in relation.relations.items()}
    leaves = plan_flatten(
        item_schema,
        relation.flatten,
        relation_paths=nested_paths,
        include_root_scalars=True,
        relative_extract=True,
    )
    idx_cols = index_columns or relation_index_columns(len(path_chain))
    reserved = {parent_key, *idx_cols, *_META_COLUMNS}
    detect_flatten_collisions(leaves, reserved=reserved)

    adapt = compile_relation_adapt(relation)
    # Parent key may still use root rename/sentinels (top-level id).
    root_adapt = flat_adapt_from_root_stg(
        coalesce=stg.coalesce,
        null_sentinels=stg.null_sentinels,
        map=stg.map,
        rename=stg.rename,
        exclude=stg.exclude,
    )
    leaf_by_name = {leaf.column_name: leaf for leaf in leaves}
    item_props = item_schema.get("properties") or {}
    if not isinstance(item_props, dict):
        item_props = {}

    columns: list[dict[str, str]] = []
    props = schema.get("properties") or {}
    parent_prop = props.get(parent_key) if isinstance(props.get(parent_key), dict) else None
    pk_expr = _cast_macro_call(parent_key, parent_prop or {"type": "string"})
    if parent_key not in root_adapt.exclude and parent_key not in adapt.exclude:
        out_pk, pk_full = _adapt_column_expr(parent_key, pk_expr, root_adapt)
        columns.append({"name": out_pk, "expr": pk_full})

    for idx in idx_cols:
        columns.append({"name": idx, "expr": idx})

    for leaf in leaves:
        if leaf.column_name in adapt.exclude:
            continue
        sources = adapt.coalesce.get(leaf.column_name)
        if sources:
            parts = [
                _leaf_or_top_expr(src, leaf_by_name=leaf_by_name, props=item_props)
                for src in sources
            ]
            expr = f"coalesce({', '.join(parts)})"
        else:
            expr = _path_cast_macro_call("_rel", leaf.extract_path, leaf.prop)
        out_name, full = _adapt_column_expr(leaf.column_name, expr, adapt)
        columns.append({"name": out_name, "expr": full})

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


def _write_slo_seed(
    project_root: Path,
    *,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    """Always regenerate the ops SLO seed from all pipelines (derived; ignores --force)."""
    from det.runtime.slo import SLO_SEED_RELPATH, render_slo_seed_for_project

    path = (project_root / SLO_SEED_RELPATH).resolve()
    content = render_slo_seed_for_project(project_root)
    exists = path.exists()
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
    column_descriptions: dict[str, str] | None = None,
    schema_properties: dict[str, Any] | None = None,
    bronze_source: str = "filesystem",
) -> str:
    """
    Emit a sources.yml table block as text (not yaml.dump).

    ``external_location`` embeds dbt Jinja; a YAML round-trip mangles quotes.
    """
    descs = column_descriptions or {}
    props = schema_properties or {}

    ordered_names: list[str] = []

    def _add(name: str) -> None:
        if name not in ordered_names:
            ordered_names.append(name)

    _add("__row_hash")
    for col in required:
        _add(col)
    for name in props:
        if name in descs:
            _add(name)

    columns: list[dict[str, Any]] = []
    for name in ordered_names:
        entry: dict[str, Any] = {"name": name}
        if name in _META_COLUMN_DESCRIPTIONS:
            entry["description"] = _META_COLUMN_DESCRIPTIONS[name]
        elif name in descs:
            entry["description"] = descs[name]
        if name == "__row_hash" or name in required:
            entry["tests"] = ["not_null"]
        columns.append(entry)

    cols_yaml = yaml.safe_dump(
        columns, sort_keys=False, default_flow_style=False, allow_unicode=True, width=1000
    ).rstrip()
    cols_indented = "\n".join(
        "        " + line if line else line for line in cols_yaml.splitlines()
    )
    desc_indented = _yaml_block({"description": description}, indent=8)

    # Match existing lake path Jinja; path is concrete per table (not {name}).
    lake_jinja = '{{ env_var("DET_LAKE_PATH", "../data/lake") }}'
    if bronze_source == "iceberg":
        lines = [
            f"      - name: {table}",
            desc_indented,
            "        meta:",
            "          # Iceberg bronze: DuckDB iceberg_scan of the table location",
            "          # (Hadoop catalog on the lake). Not a JSONL hive glob.",
            "          formatter: template",
            "          external_location: >-",
            f"            iceberg_scan('{lake_jinja}/bronze/{provider}/{table}')",
            "        columns:",
            cols_indented,
        ]
        return "\n".join(lines)
    lines = [
        f"      - name: {table}",
        desc_indented,
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


def _insert_source_block(text: str, source_only: str, *, source_name: str) -> str:
    """
    Insert a top-level source block in alphabetical order by ``- name:``.

    Table force-replace stays in-place; this only runs when adding a *new* source
    so provider order stays stable across scaffolds.
    """
    matches = list(re.finditer(r"(?m)^(?P<indent>\s*)- name:\s*(?P<name>\S+)\s*$", text))
    # Only consider top-level source entries (indent under ``sources:``, typically 2 spaces).
    source_headers = [
        m
        for m in matches
        if m.group("indent") in {"  ", "\t"}
        or (len(m.group("indent")) == 2 and m.group("indent").isspace())
    ]
    insert_at: int | None = None
    for m in source_headers:
        if m.group("name") > source_name:
            insert_at = m.start()
            break
    if insert_at is None:
        return text.rstrip() + "\n" + source_only
    return text[:insert_at] + source_only + text[insert_at:]


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
    table_description: str | None = None,
    column_descriptions: dict[str, str] | None = None,
    schema_properties: dict[str, Any] | None = None,
    bronze_source: str = "filesystem",
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
        description=table_description or f"DET bronze {provider}.{table}",
        provider=provider,
        columns_struct=columns_struct,
        column_descriptions=column_descriptions,
        schema_properties=schema_properties,
        bronze_source=bronze_source,
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
        source_only = block.split("sources:\n", 1)[-1].rstrip() + "\n"
        text = _insert_source_block(text, source_only, source_name=source_name)
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
            rf"(?m)^{re.escape(indent)}- name:\s*{re.escape(table)}\s*\n"
            rf"(?:(?!^\s{{0,{indent_len}}}- name:)[^\n]*\n)*"
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
        text = _append_table_under_source(text, source_name, table_yaml)
        detail = "add table"

    if dry_run:
        actions.append(ScaffoldAction(path=sources_path, action="would_patch", detail=detail))
        return

    sources_path.write_text(text, encoding="utf-8")
    actions.append(ScaffoldAction(path=sources_path, action="write", detail=detail))
    logger.info("scaffolded sources.yml", path=str(sources_path), detail=detail)


def _append_table_under_source(text: str, source_name: str, table_yaml: str) -> str:
    """Append a table YAML block under the matching top-level source entry."""
    start = re.search(
        rf"(?m)^(?P<indent>\s*)- name:\s*{re.escape(source_name)}\s*$",
        text,
    )
    if start is None:
        if not re.search(r"(?m)^\s*tables:\s*$", text):
            text = text.rstrip() + "\n    tables:\n"
        return text.rstrip() + "\n" + table_yaml + "\n"

    indent = start.group("indent")
    rest = text[start.end() :]
    next_source = re.search(rf"(?m)^{re.escape(indent)}- name:\s*\S+\s*$", rest)
    insert_at = start.end() + (next_source.start() if next_source else len(rest))
    before = text[:insert_at].rstrip()
    after = text[insert_at:]
    if "tables:" not in before[start.start() :]:
        before = before + "\n    tables:"
    return before + "\n" + table_yaml + "\n" + after


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
    model_description: str | None = None,
    column_descriptions: dict[str, str] | None = None,
    docs: DbtDocsConfig | None = None,
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

    mapped = column_descriptions or {}
    docs_cols = dict(docs.columns) if docs is not None else {}

    def _desc_for(name: str) -> str | None:
        if name in docs_cols:
            return docs_cols[name]
        if name in _META_COLUMN_DESCRIPTIONS:
            return _META_COLUMN_DESCRIPTIONS[name]
        if name in mapped:
            return mapped[name]
        return None

    columns_out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name, tests in by_col.items():
        entry: dict[str, Any] = {"name": name}
        desc = _desc_for(name)
        if desc:
            entry["description"] = desc
        entry["tests"] = tests
        columns_out.append(entry)
        seen.add(name)
    # Docs-only columns (no tests) so analytics overlays aren't dropped.
    for name, text in docs_cols.items():
        if name in seen:
            continue
        columns_out.append({"name": name, "description": text})
        seen.add(name)

    entry = {
        "name": model_name,
        "description": model_description or f"Cleaned, deduped {dataset} (silver)",
        "columns": columns_out,
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
        yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, allow_unicode=True),
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
    warn: bool = True,
) -> ScaffoldResult:
    """
    Emit/merge dbt bronze source + stg + silver for a pipeline dataset.

    Create-if-missing by default; `--force` overwrites generated SQL and refreshes
    YAML entries for the dataset. Always regenerates ``dbt/seeds/ops_slo_expected.csv``
    from **all** pipelines (derived; ignores ``force``). When ``warn`` is true, emit
    advisory view-size warnings for large view-materialized relations.
    """
    root = project_root.resolve()
    # Stable analytics names from pipeline ``name``; lake/SQL table stays versioned.
    model_slug = dbt_model_slug(config.name)
    provider, _ = parse_canonical_id(config.name)
    sql_schema, sql_table = sql_names_for_config(config)
    silver: DbtSilverConfig = config.dbt.silver
    stg_cfg: DbtStgConfig = config.dbt.stg
    docs_cfg: DbtDocsConfig = config.dbt.docs
    schema = load_json_schema(resolve_path(root, config.schema_path))
    required = [c for c in (schema.get("required") or []) if isinstance(c, str)]
    schema_descs = schema_column_descriptions(schema)
    root_desc = schema_root_description(schema)
    silver_descs = post_stg_description_map(schema_descs, stg_cfg)
    props = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
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
        provider=provider,
    )
    silver_sql = _render(
        "silver.sql.j2",
        model_slug=model_slug,
        silver=silver,
        provider=provider,
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
        table_description=root_desc,
        column_descriptions=schema_descs,
        schema_properties=props,
        bronze_source=config.destination.type,
    )
    _merge_silver_models_yml(
        models_dir / "_silver__models.yml",
        dataset=model_slug,
        silver=silver,
        required=required,
        force=force,
        dry_run=dry_run,
        actions=actions,
        model_description=root_desc,
        column_descriptions=silver_descs,
        docs=docs_cfg,
    )

    for name_parts, path_chain_t, rel in iter_relation_paths(stg_cfg.relations):
        path_chain = list(path_chain_t)
        parent_key = default_parent_key(schema, silver, rel.parent_key)
        rel_slug = f"{model_slug}__{'__'.join(name_parts)}"
        idx_cols = relation_index_columns(len(path_chain))
        rel_columns = relation_stg_columns(
            schema,
            path_chain=path_chain,
            relation=rel,
            parent_key=parent_key,
            stg=stg_cfg,
            index_columns=idx_cols,
        )
        path_display = "[]".join(path_chain) + "[]"
        rel_stg_sql = _render(
            "stg_relation.sql.j2",
            sql_table=sql_table,
            sql_schema=sql_schema,
            relation_name="__".join(name_parts),
            path_chain=path_chain,
            path_display=path_display,
            index_columns=idx_cols,
            parent_key=parent_key,
            materialized=rel.materialized,
            columns=rel_columns,
            provider=provider,
            meta_columns=_META_COLUMNS,
        )
        rel_unique_key = [parent_key, *idx_cols]
        rel_silver_sql = _render(
            "silver_relation.sql.j2",
            model_slug=rel_slug,
            relation_name="__".join(name_parts),
            materialized=rel.materialized,
            unique_key=rel_unique_key,
            order_by=list(silver.order_by),
            provider=provider,
        )
        _write_or_skip(
            models_dir / f"stg_{rel_slug}.sql",
            rel_stg_sql,
            force=force,
            dry_run=dry_run,
            actions=actions,
        )
        _write_or_skip(
            models_dir / f"silver_{rel_slug}.sql",
            rel_silver_sql,
            force=force,
            dry_run=dry_run,
            actions=actions,
        )
        rel_adapt = compile_relation_adapt(rel)
        rel_not_null = [parent_key]
        for col in rel_adapt.not_null:
            if col not in rel_not_null:
                rel_not_null.append(col)
        rel_silver_cfg = silver.model_copy(
            update={
                "materialized": rel.materialized,
                "unique_key": rel_unique_key,
                "not_null": rel_not_null,
                "unique": list(rel_adapt.unique),
                "accepted_values": dict(rel_adapt.accepted_values),
            }
        )
        _merge_silver_models_yml(
            models_dir / "_silver__models.yml",
            dataset=rel_slug,
            silver=rel_silver_cfg,
            required=[parent_key],
            force=force,
            dry_run=dry_run,
            actions=actions,
            model_description=root_desc,
            column_descriptions=silver_descs,
            docs=docs_cfg,
        )

    _write_slo_seed(root, dry_run=dry_run, actions=actions)

    if warn:
        from det.scaffold.view_warn import emit_view_size_warnings

        emit_view_size_warnings(config, project_root=root)

    return ScaffoldResult(dataset=config.bronze_dataset(), actions=actions)
