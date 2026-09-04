"""DET dbt scaffold facade.

Re-exports all public (and private) names that callers use, so
``from det.scaffold.dbt import X`` continues to work unchanged.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.logging import get_logger
from det.runtime.config import (
    DbtDocsConfig,
    DbtSilverConfig,
    DbtStgConfig,
    PipelineConfig,
    resolve_path,
)
from det.runtime.ids import dbt_model_slug, parse_canonical_id, sql_names_for_config
from det.runtime.sql_types import (
    duckdb_type_for_prop,  # noqa: F401  (test_flatten imports this via dbt)
)
from det.scaffold.adapt_scope import compile_relation_adapt

# --- SQL generation helpers (re-export) ---
from det.scaffold.dbt_sql import (  # noqa: F401
    _META_COLUMN_DESCRIPTIONS,
    _META_COLUMNS,
    _META_DUCKDB_TYPES,
    _TEMPLATES,
    ScaffoldAction,
    ScaffoldResult,
    SpineEntry,
    _adapt_column_expr,
    _allowed_types,
    _apply_null_sentinels,
    _apply_value_map,
    _cast_macro_call,
    _env,
    _is_identity_name,
    _leaf_or_top_expr,
    _order_stg_select_columns,
    _ordered_meta_columns,
    _parent_flat_adapt,
    _path_cast_macro_call,
    _render,
    _spine_cte_expr,
    _sql_string_literal,
    default_parent_key,
    expected_silver_sql,
    format_read_json_columns,
    post_stg_description_map,
    qualify_grain_col,
    read_json_columns_from_schema,
    relation_chain_for,
    relation_index_columns,
    relation_stg_columns,
    schema_column_descriptions,
    schema_root_description,
    spine_for_relation,
    stg_columns_from_schema,
    widen_read_json_columns,
)

# --- YAML merge helpers (re-export) ---
from det.scaffold.dbt_yaml import (  # noqa: F401
    _add_col_test,
    _append_table_under_source,
    _insert_source_block,
    _merge_silver_models_yml,
    _merge_sources_table,
    _sources_has_source,
    _sources_has_table,
    _table_entry_yaml,
    _yaml_block,
)
from det.scaffold.flatten import iter_relation_paths
from det.scaffold.relation_load import (
    relation_dedupe_key,
    relation_delete_key,
    resolve_relation_materialization,
)
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)

# Shared with scaffold-ops; create-if-missing only (never --force overwrite).
_GENERATE_SCHEMA_NAME_TMPL = (
    Path(__file__).resolve().parent
    / "templates"
    / "ops"
    / "macros"
    / "generate_schema_name.sql"
)


def _ensure_under_root(path: Path, *, root: Path) -> Path:
    """Resolve ``path`` and reject destinations that escape ``root``."""
    resolved = path.expanduser().resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"scaffold path escapes project root {root}: {resolved}")
    return resolved


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


def _bootstrap_generate_schema_name(
    project_root: Path,
    *,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    """Install ``generate_schema_name`` once; never overwrite (ignores ``--force``).

    Embedders often customize this global dbt override. DET only fills the
    greenfield gap so ``schema="silver_*"`` / ``+schema: ops`` are not prefixed
    with ``target.schema``.
    """
    root = project_root.resolve()
    path = _ensure_under_root(
        root / "dbt" / "macros" / "generate_schema_name.sql",
        root=root,
    )
    if path.exists():
        actions.append(ScaffoldAction(path=path, action="skip", detail="exists"))
        return
    content = _GENERATE_SCHEMA_NAME_TMPL.read_text(encoding="utf-8")
    if dry_run:
        actions.append(
            ScaffoldAction(path=path, action="would_write", detail="create")
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    actions.append(ScaffoldAction(path=path, action="write", detail="create"))
    logger.info("scaffolded file", path=str(path), detail="create")


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
    from **all** pipelines (derived; ignores ``force``). Bootstraps
    ``macros/generate_schema_name.sql`` if missing (never overwrites). When
    ``warn`` is true, emit advisory view-size warnings for large view-materialized
    relations.
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
    columns = stg_columns_from_schema(schema, stg_cfg, unique_key=silver.unique_key)
    columns_struct = format_read_json_columns(
        widen_read_json_columns(schema, stg_cfg)
    )

    models_dir = (dbt_models_dir or (root / "dbt" / "models" / "silver")).resolve()
    actions: list[ScaffoldAction] = []
    _bootstrap_generate_schema_name(root, dry_run=dry_run, actions=actions)

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
        pipeline_name=config.name,
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

    for name_parts_t, path_chain_t, rel in iter_relation_paths(stg_cfg.relations):
        name_parts = list(name_parts_t)
        path_chain = list(path_chain_t)
        parent_key = default_parent_key(schema, silver, rel.parent_key)
        rel_slug = f"{model_slug}__{'__'.join(name_parts)}"
        rel_chain = relation_chain_for(stg_cfg.relations, name_parts)
        spine = spine_for_relation(name_parts, rel_chain)
        spine_projections = [
            {"name": e.name, "cte_expr": _spine_cte_expr(e, schema=schema, path_chain=path_chain)}
            for e in spine
        ]
        rel_columns = relation_stg_columns(
            schema,
            path_chain=path_chain,
            relation=rel,
            parent_key=parent_key,
            stg=stg_cfg,
            name_parts=name_parts,
            rel_chain=rel_chain,
            spine=spine,
        )
        path_display = "[]".join(path_chain) + "[]"
        rel_mat = resolve_relation_materialization(
            rel, incremental_strategy=silver.incremental_strategy
        )
        dedupe_key = relation_dedupe_key(parent_key, spine)
        delete_key = relation_delete_key(parent_key, spine)
        rel_stg_sql = _render(
            "stg_relation.sql.j2",
            sql_table=sql_table,
            sql_schema=sql_schema,
            relation_name="__".join(name_parts),
            path_chain=path_chain,
            path_display=path_display,
            spine_projections=spine_projections,
            parent_key=parent_key,
            materialized=rel_mat.stg_materialized,
            columns=rel_columns,
            provider=provider,
            meta_columns=_META_COLUMNS,
        )
        rel_silver_sql = _render(
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
        rel_not_null = list(dedupe_key)
        for col in rel_adapt.not_null:
            if col not in rel_not_null:
                rel_not_null.append(col)
        # YAML uniqueness/not_null tests use full grain; SQL incremental unique_key
        # is delete_key (emitted in silver_relation.sql.j2 for parent_replace).
        rel_silver_update: dict[str, Any] = {
            "materialized": rel_mat.silver_materialized,
            "unique_key": dedupe_key,
            "not_null": rel_not_null,
            "unique": list(rel_adapt.unique),
            "accepted_values": dict(rel_adapt.accepted_values),
        }
        if rel_mat.incremental_strategy is not None:
            rel_silver_update["incremental_strategy"] = rel_mat.incremental_strategy
        rel_silver_cfg = silver.model_copy(update=rel_silver_update)
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
