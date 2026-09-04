"""SQL generation helpers for DET dbt scaffolding."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from det.logging import get_logger
from det.optional_deps import require_jinja2
from det.runtime.config import (
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
from det.scaffold.relation_load import (
    relation_dedupe_key,
    relation_delete_key,
    resolve_relation_materialization,
)
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)


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


def _is_identity_name(name: str) -> bool:
    """Heuristic identity / key columns for stg SELECT lead group."""
    if name.startswith("__"):
        return False
    return (
        name == "id"
        or name.endswith("_id")
        or name == "key"
        or name.endswith("_key")
    )


def _ordered_meta_columns() -> list[str]:
    """Presentation order: ``__row_hash`` first, then remaining meta A–Z."""
    rest = sorted(c for c in _META_COLUMNS if c != "__row_hash")
    if "__row_hash" in _META_COLUMNS:
        return ["__row_hash", *rest]
    return rest


def _order_stg_select_columns(
    columns: list[dict[str, str]],
    *,
    unique_key: Sequence[str] | None = None,
) -> list[dict[str, str]]:
    """
    Order stg SELECT columns: identity A–Z → payload A–Z → meta
    (``__row_hash`` first, then other ``__*`` A–Z).
    """
    uk = {k for k in (unique_key or ()) if isinstance(k, str) and not k.startswith("__")}
    by_name = {c["name"]: c for c in columns}
    identity: list[str] = []
    payload: list[str] = []
    for name in by_name:
        if name.startswith("__"):
            continue
        if name in uk or _is_identity_name(name):
            identity.append(name)
        else:
            payload.append(name)
    identity.sort()
    payload.sort()
    ordered: list[dict[str, str]] = [by_name[n] for n in identity]
    ordered.extend(by_name[n] for n in payload)
    for meta in _ordered_meta_columns():
        if meta in by_name:
            ordered.append(by_name[meta])
        else:
            ordered.append({"name": meta, "expr": meta})
    return ordered


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


def _env():
    jinja2 = require_jinja2()
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(_TEMPLATES)),
        autoescape=jinja2.select_autoescape(enabled_extensions=()),
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
    *,
    unique_key: Sequence[str] | None = None,
) -> list[dict[str, str]]:
    """
    Build stg select column exprs from schema + optional ``dbt.stg`` adaptations.

    Adaptations order: flatten → coalesce → null_sentinels → map → rename → exclude.
    Final SELECT order: identity A–Z → payload A–Z → meta (``__row_hash`` first).
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
    for name, _prop in props.items():
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

    return _order_stg_select_columns(columns, unique_key=unique_key)


def qualify_grain_col(name_parts: Sequence[str], field: str) -> str:
    """Path-qualify a grain field: ``line_items`` + ``line_id`` → ``line_items__line_id``."""
    if not name_parts:
        raise ValueError("name_parts must be non-empty")
    return "__".join([*name_parts, field])


@dataclass(frozen=True)
class SpineEntry:
    """One path-qualified spine column for a relation stg model."""

    name: str
    level_idx: int
    kind: str  # "grain" | "index"
    field: str


def spine_for_relation(
    name_parts: Sequence[str],
    rel_chain: Sequence[RelationConfig],
) -> list[SpineEntry]:
    """
    Build the ancestor+self spine for a relation at ``name_parts``.

    Empty ``grain`` at a level yields ``{path}__index`` from unnest ordinality.
    """
    if len(name_parts) != len(rel_chain):
        raise ValueError(
            f"name_parts length {len(name_parts)} != rel_chain length {len(rel_chain)}"
        )
    if not name_parts:
        raise ValueError("name_parts must be non-empty")
    out: list[SpineEntry] = []
    for i in range(len(name_parts)):
        parts = tuple(name_parts[: i + 1])
        rel = rel_chain[i]
        if rel.grain:
            for field in rel.grain:
                out.append(
                    SpineEntry(
                        name=qualify_grain_col(parts, field),
                        level_idx=i,
                        kind="grain",
                        field=field,
                    )
                )
        else:
            out.append(
                SpineEntry(
                    name=qualify_grain_col(parts, "index"),
                    level_idx=i,
                    kind="index",
                    field="index",
                )
            )
    return out


def relation_chain_for(
    relations: dict[str, RelationConfig],
    name_parts: Sequence[str],
) -> list[RelationConfig]:
    """Walk ``relations`` tree following YAML keys in ``name_parts``."""
    chain: list[RelationConfig] = []
    cur: dict[str, RelationConfig] = relations
    for part in name_parts:
        if part not in cur:
            raise KeyError(f"relation {part!r} not found under {list(cur)}")
        rel = cur[part]
        chain.append(rel)
        cur = rel.relations
    return chain


def relation_index_columns(depth: int) -> list[str]:
    """Deprecated alias: prefer ``spine_for_relation`` path-qualified names."""
    if depth < 1:
        raise ValueError("relation depth must be >= 1")
    if depth == 1:
        return ["__rel_index"]
    return [f"__rel_index_{i}" for i in range(depth)]


def _spine_cte_expr(
    entry: SpineEntry,
    *,
    schema: dict[str, Any],
    path_chain: Sequence[str],
) -> str:
    """SQL/Jinja expr that projects a spine column inside the exploded CTE."""
    if entry.kind == "index":
        return f"t{entry.level_idx}.__rel_index"
    item_schema = relation_item_schema_at(schema, list(path_chain[: entry.level_idx + 1]))
    props = item_schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}
    prop = props.get(entry.field) if isinstance(props.get(entry.field), dict) else None
    return _path_cast_macro_call(
        f"t{entry.level_idx}._rel",
        f"$.{entry.field}",
        prop if isinstance(prop, dict) else {"type": "string"},
    )


def relation_stg_columns(
    schema: dict[str, Any],
    *,
    path_chain: list[str],
    relation: RelationConfig,
    parent_key: str,
    stg: DbtStgConfig,
    name_parts: Sequence[str] | None = None,
    rel_chain: Sequence[RelationConfig] | None = None,
    spine: Sequence[SpineEntry] | None = None,
) -> list[dict[str, str]]:
    """Column exprs for a relation stg model (parent_key + spine + flatten + meta)."""
    if name_parts is None:
        name_parts = tuple(path_chain)
    if rel_chain is None:
        rel_chain = relation_chain_for(stg.relations, name_parts)
    if spine is None:
        spine = spine_for_relation(name_parts, rel_chain)

    item_schema = relation_item_schema_at(schema, path_chain)
    nested_paths = {child.path or name for name, child in relation.relations.items()}
    leaves = plan_flatten(
        item_schema,
        relation.flatten,
        relation_paths=nested_paths,
        include_root_scalars=True,
        relative_extract=True,
    )
    spine_names = [e.name for e in spine]
    reserved = {parent_key, *spine_names, *_META_COLUMNS}
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

    props = schema.get("properties") or {}
    parent_prop = props.get(parent_key) if isinstance(props.get(parent_key), dict) else None
    pk_expr = _cast_macro_call(parent_key, parent_prop or {"type": "string"})
    lead: list[dict[str, str]] = []
    if parent_key not in root_adapt.exclude and parent_key not in adapt.exclude:
        out_pk, pk_full = _adapt_column_expr(parent_key, pk_expr, root_adapt)
        lead.append({"name": out_pk, "expr": pk_full})

    for entry in spine:
        # Spine already projected in the exploded CTE under ``entry.name``.
        lead.append({"name": entry.name, "expr": entry.name})

    leaf_cols: list[dict[str, str]] = []
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
        leaf_cols.append({"name": out_name, "expr": full})

    # parent_key + spine pinned; leaves identity → payload; meta last.
    ordered_leaves = _order_stg_select_columns(leaf_cols, unique_key=None)
    # Strip meta from ordered_leaves — we'll append after lead+payload via helper.
    non_meta = [c for c in ordered_leaves if not c["name"].startswith("__")]
    return [*lead, *non_meta, *[{"name": m, "expr": m} for m in _ordered_meta_columns()]]


def _render(name: str, **ctx: Any) -> str:
    return _env().get_template(name).render(**ctx)


def expected_silver_sql(
    config: PipelineConfig,
    *,
    project_root: Path,
) -> dict[str, str]:
    """
    Re-render expected silver (+ relation silver) SQL from current config+schema.

    Keys are paths relative to ``project_root`` under ``dbt/models/silver/``.
    Does not compare stg SQL (templates document hand-edits).
    """
    root = project_root.resolve()
    model_slug = dbt_model_slug(config.name)
    provider, _ = parse_canonical_id(config.name)
    sql_schema, sql_table = sql_names_for_config(config)
    silver: DbtSilverConfig = config.dbt.silver
    stg_cfg: DbtStgConfig = config.dbt.stg
    schema = load_json_schema(resolve_path(root, config.schema_path))
    out: dict[str, str] = {
        f"dbt/models/silver/silver_{model_slug}.sql": _render(
            "silver.sql.j2",
            model_slug=model_slug,
            silver=silver,
            provider=provider,
            pipeline_name=config.name,
        )
    }
    for name_parts_t, path_chain_t, rel in iter_relation_paths(stg_cfg.relations):
        name_parts = list(name_parts_t)
        path_chain = list(path_chain_t)
        parent_key = default_parent_key(schema, silver, rel.parent_key)
        rel_slug = f"{model_slug}__{'__'.join(name_parts)}"
        rel_chain = relation_chain_for(stg_cfg.relations, name_parts)
        spine = spine_for_relation(name_parts, rel_chain)
        rel_mat = resolve_relation_materialization(
            rel, incremental_strategy=silver.incremental_strategy
        )
        dedupe_key = relation_dedupe_key(parent_key, spine)
        delete_key = relation_delete_key(parent_key, spine)
        spine_meta = [
            {
                "name": e.name,
                "level_idx": e.level_idx,
                "kind": e.kind,
                "field": e.field,
            }
            for e in spine
        ]
        out[f"dbt/models/silver/silver_{rel_slug}.sql"] = _render(
            "silver_relation.sql.j2",
            model_slug=rel_slug,
            relation_name="__".join(name_parts),
            silver_materialized=rel_mat.silver_materialized,
            incremental_strategy=rel_mat.incremental_strategy or silver.incremental_strategy,
            delete_key=delete_key,
            dedupe_key=dedupe_key,
            order_by=list(silver.order_by),
            watermark=silver.watermark,
            lookback=silver.lookback,
            pipeline_name=config.name,
            provider=provider,
            bigquery=rel.bigquery,
            path_chain=path_chain,
            spine_meta=spine_meta,
            parent_key=parent_key,
            sql_table=sql_table,
            sql_schema=sql_schema,
        )
    return out
