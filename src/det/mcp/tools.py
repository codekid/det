"""Read-only inspect and dry-run tool implementations for DET MCP.

Dry-run and ops tools live in :mod:`det.mcp.dry_run` and :mod:`det.mcp.ops_tools`;
this module is the stable import façade used by the MCP server and tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from det.mcp import _helpers as h
from det.mcp import airflow_inspect as af
from det.mcp.context import PathSandboxError, resolve_under_root
from det.mcp.dry_run import (
    biglake_register_dry_run,
    dbt_dry_run,
    diff_bronze_silver,
    iceberg_register_dry_run,
    init_pipeline_dry_run,
    migrate_dry_run,
    prune_dry_run,
    scaffold_dbt_dry_run,
    scaffold_ops_dry_run,
    silver_catchup_cleanup_dry_run,
    silver_catchup_dry_run,
)
from det.mcp.ops_tools import (
    check,
    cube_load,
    cube_meta,
    describe_approval,
    describe_model,
    list_approvals,
    list_models,
    list_runs,
    query_analytics,
    summarize_runs,
)

# Back-compat aliases for helpers historically defined on this module.
DEFAULT_LIST_LIMIT = h.DEFAULT_LIST_LIMIT
DEFAULT_SAMPLE_LIMIT = h.DEFAULT_SAMPLE_LIMIT
MAX_SAMPLE_LIMIT = h.MAX_SAMPLE_LIMIT
_prepare_tool = h.prepare_tool
_root = h.root
_approval_plan = h.approval_plan
_pipeline_path = h.pipeline_path
_canonical_id = h.canonical_id
_require_catchup_scope = h.require_catchup_scope
_load_pipeline = h.load_pipeline
_rel = h.rel


def list_pipelines(*, root: Path | None = None) -> dict[str, Any]:
    h.prepare_tool()
    from det.runtime.pipelines import list_pipeline_ids

    base = h.root(root)
    return {"project_root": str(base), "pipelines": list_pipeline_ids(base)}


def list_sources_tool(*, root: Path | None = None) -> dict[str, Any]:
    h.prepare_tool()
    from det.plugins import load_plugins
    from det.runtime.discovery import probe_source_load_errors
    from det.runtime.registry import list_sources

    _ = h.root(root)
    load_plugins()
    base = h.root(root)
    return {
        "sources": list_sources(project_root=base),
        "errors": probe_source_load_errors(project_root=base),
    }


def list_mappers_tool(*, root: Path | None = None) -> dict[str, Any]:
    h.prepare_tool()
    from det.plugins import load_plugins
    from det.runtime.registry import describe_mappers, list_mappers

    base = h.root(root)
    load_plugins()
    mappers = describe_mappers(project_root=base)
    return {
        "mappers": [{"name": name, "summary": summary} for name, summary in mappers],
        "names": list_mappers(project_root=base),
    }


def _connection_display(destination: Any) -> str | None:
    """Never echo a Postgres DSN — report the secret name (DuckDB is a file path)."""
    if destination.type != "postgres":
        return destination.connection
    if destination.connection_env:
        return f"env:{destination.connection_env}"
    return "(postgres DSN in destination.connection — move it to connection_env)"


def describe_pipeline(pipeline: str, *, root: Path | None = None) -> dict[str, Any]:
    h.prepare_tool()
    from det.runtime.ids import sql_names_for_config

    base = h.root(root)
    config, path = h.load_pipeline(pipeline, base)
    silver = config.dbt.silver
    sql_schema, sql_table = sql_names_for_config(config)
    return {
        "name": config.name,
        "path": h.rel(path, base),
        "source": {"type": config.source.type},
        "schema": config.schema_path,
        "destination": {
            "type": config.destination.type,
            "path": config.destination.path,
            "dataset": config.destination.dataset,
            "medallion_prefix": config.destination.dataset or "bronze",
            "connection": _connection_display(config.destination),
            "connection_env": config.destination.connection_env,
            "partition": (
                config.destination.partition
                if config.destination.type == "iceberg"
                else None
            ),
            "sql_schema": sql_schema,
            "sql_table": sql_table,
        },
        "dataset": config.bronze_dataset(),
        "fs_dataset": config.fs_dataset_relpath(),
        "wire_version": config.wire_version,
        "ingestion": {"library": config.ingestion.library},
        "dbt": {
            "silver": {
                "materialized": silver.materialized,
                "unique_key": list(silver.unique_key),
                "order_by": list(silver.order_by),
                "incremental_strategy": silver.incremental_strategy,
                "watermark": silver.watermark,
                "lookback": silver.lookback,
                "not_null": list(silver.not_null),
                "unique": list(silver.unique),
                "accepted_values": {k: list(v) for k, v in silver.accepted_values.items()},
                **(
                    {
                        "bigquery": {
                            "partition_by": (
                                silver.bigquery.partition_by.to_dbt_dict()
                                if silver.bigquery.partition_by
                                else None
                            ),
                            "cluster_by": list(silver.bigquery.cluster_by),
                            "require_partition_filter": (
                                silver.bigquery.require_partition_filter
                            ),
                        }
                    }
                    if silver.bigquery is not None
                    else {}
                ),
            },
            "stg": {
                "coalesce": {k: list(v) for k, v in config.dbt.stg.coalesce.items()},
                "null_sentinels": {k: list(v) for k, v in config.dbt.stg.null_sentinels.items()},
                "rename": dict(config.dbt.stg.rename),
                "exclude": list(config.dbt.stg.exclude),
                "map": {k: dict(v) for k, v in config.dbt.stg.map.items()},
            },
        },
    }


def list_raw_partitions(
    pipeline: str,
    *,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    h.prepare_tool()
    from det.destinations.models import raw_dataset_dir

    base = h.root(root)
    config, _ = h.load_pipeline(pipeline, base)
    dataset_dir = raw_dataset_dir(config, base)
    capped = max(1, min(int(limit), DEFAULT_LIST_LIMIT))
    runs = h.insp.walk_hive_runs(
        dataset_dir, root=base, limit=capped, require_committed=True, normalize_iso=False
    )
    return {
        "pipeline": config.name,
        "dataset_dir": h.rel(dataset_dir, base),
        "limit": capped,
        "truncated": len(runs) >= capped,
        "runs": runs,
    }


def list_bronze_partitions(
    pipeline: str,
    *,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    h.prepare_tool()
    from det.destinations.models import bronze_dataset_dir
    from det.runtime.ids import sql_names_for_config

    base = h.root(root)
    config, _ = h.load_pipeline(pipeline, base)
    dest = config.destination
    if dest.type == "iceberg":
        from det.destinations.models import lake_root
        from det.ingestion.iceberg_writer import list_iceberg_extract_runs, load_iceberg_table

        sql_schema, sql_table = sql_names_for_config(config)
        dataset_dir = bronze_dataset_dir(config, base)
        capped = max(1, min(int(limit), DEFAULT_LIST_LIMIT))
        try:
            ice = load_iceberg_table(
                lake=lake_root(dest, base),
                namespace=sql_schema,
                table=sql_table,
                table_location=dataset_dir,
            )
        except ImportError as exc:
            return {
                "pipeline": config.name,
                "destination_type": "iceberg",
                "schema": sql_schema,
                "table": sql_table,
                "location": h.rel(dataset_dir, base),
                "runs": [],
                "note": str(exc),
            }
        runs_raw = list_iceberg_extract_runs(ice, limit=capped) if ice is not None else []
        runs = [
            {
                "interval_start": start,
                "interval_end": end,
                "extract_run_datetime": run,
            }
            for start, end, run in runs_raw
        ]
        return {
            "pipeline": config.name,
            "destination_type": "iceberg",
            "schema": sql_schema,
            "table": sql_table,
            "location": h.rel(dataset_dir, base),
            "limit": capped,
            "truncated": len(runs) >= capped,
            "runs": runs,
        }
    if dest.type != "filesystem":
        hint: dict[str, Any] = {
            "pipeline": config.name,
            "destination_type": dest.type,
            "note": (
                "Bronze is not a hive directory for this destination; "
                "listing is unavailable. Use prune_dry_run or query the database."
            ),
            "dataset": config.bronze_dataset(),
            "runs": [],
        }
        sql_schema, sql_table = sql_names_for_config(config)
        if dest.type == "duckdb":
            hint["connection"] = dest.connection
            hint["schema"] = sql_schema
            hint["table"] = sql_table
        elif dest.type == "postgres":
            hint["connection"] = _connection_display(dest)
            hint["schema"] = sql_schema
            hint["table"] = sql_table
        return hint

    dataset_dir = bronze_dataset_dir(config, base)
    capped = max(1, min(int(limit), DEFAULT_LIST_LIMIT))
    runs = h.insp.walk_hive_runs(
        dataset_dir,
        root=base,
        limit=capped,
        normalize_iso=False,
        require_committed=True,
    )
    return {
        "pipeline": config.name,
        "destination_type": "filesystem",
        "dataset_dir": h.rel(dataset_dir, base),
        "limit": capped,
        "truncated": len(runs) >= capped,
        "runs": runs,
    }


def read_manifest(run_path: str, *, root: Path | None = None) -> dict[str, Any]:
    """Read meta/manifest.json for a raw extract-run directory under the lake."""
    h.prepare_tool()
    base = h.root(root)
    run_dir = resolve_under_root(run_path, root=base)
    if not run_dir.is_dir():
        raise FileNotFoundError(f"run path is not a directory: {run_dir}")

    manifest = run_dir / "meta" / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError(f"manifest not found: {h.rel(manifest, base)}")

    # Must live under a lake raw/ tree (…/raw/<dataset>/…/meta/manifest.json).
    parts = manifest.resolve().parts
    if "raw" not in parts or "meta" not in parts:
        raise PathSandboxError(
            f"manifest must be under a lake raw/…/meta/ path: {h.rel(manifest, base)}"
        )

    raw = json.loads(manifest.read_text(encoding="utf-8"))
    return {
        "path": h.rel(manifest, base),
        "manifest": raw,
    }


def lake_path_for_pipeline(pipeline: str, *, root: Path | None = None) -> str:
    """Display path for the lake (ops root in layout 2; unified root in layout 1)."""
    h.prepare_tool()
    from det.destinations.models import lake_roots_for

    base = h.root(root)
    config, _ = h.load_pipeline(pipeline, base)
    roots = lake_roots_for(base, destination=config.destination)
    return h.rel(roots.ops, base)


def diff_partitions(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Compare raw vs bronze extract-run coverage (hive and/or SQL meta)."""
    h.prepare_tool()
    return h.insp.diff_partitions(
        pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        limit=limit,
        root=root,
    )


def sample_raw(
    pipeline: str,
    *,
    stage: str = "named",
    limit: int = DEFAULT_SAMPLE_LIMIT,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Sample raw wire/rows at a load stage (wire|rows|named|coerced)."""
    h.prepare_tool()
    return h.insp.sample_raw(
        pipeline,
        stage=stage,  # type: ignore[arg-type]
        limit=limit,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        root=root,
    )


def validate_sample(
    pipeline: str,
    *,
    limit: int = DEFAULT_SAMPLE_LIMIT,
    max_errors: int = 20,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Coerce + JSON Schema check on a capped raw sample (errors as data)."""
    h.prepare_tool()
    return h.insp.validate_sample(
        pipeline,
        limit=limit,
        max_errors=max_errors,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        root=root,
    )


def sample_bronze(
    pipeline: str,
    *,
    limit: int = DEFAULT_SAMPLE_LIMIT,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Sample landed bronze rows (filesystem JSONL or SQL LIMIT). Inspection only."""
    h.prepare_tool()
    return h.insp.sample_bronze(
        pipeline,
        limit=limit,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        root=root,
    )


def diagnose_pipeline(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Composite coverage + validation diagnose with suggested CLI commands."""
    h.prepare_tool()
    return h.insp.diagnose_pipeline(
        pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        sample_limit=sample_limit,
        root=root,
    )


def schema_from_sample_dry_run(
    pipeline: str | None = None,
    *,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    records: list[dict[str, Any]] | None = None,
    limit: int = MAX_SAMPLE_LIMIT,
    schema_out: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Infer bronze JSON Schema from sample rows (dry-run; never writes)."""
    h.prepare_tool()
    return h.gen.schema_from_sample_dry_run(
        pipeline,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        records=records,
        limit=limit,
        schema_out=schema_out,
        root=root,
    )


def mapper_from_diff_dry_run(
    from_schema: str,
    to_schema: str,
    mapper_name: str,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Diff two schemas and draft a mapper stub (dry-run; never writes)."""
    h.prepare_tool()
    return h.gen.mapper_from_diff_dry_run(
        from_schema,
        to_schema,
        mapper_name,
        root=root,
    )


def airflow_health(*, root: Path | None = None) -> dict[str, Any]:
    """Airflow /health via DET_AIRFLOW_* (Compose defaults). Never mutates."""
    return af.airflow_health(root=root)


def list_airflow_dags(*, root: Path | None = None) -> dict[str, Any]:
    """List DET DAGs from Airflow REST API."""
    return af.list_airflow_dags(root=root)


def list_airflow_dag_runs(
    dag_id: str,
    *,
    limit: int = 10,
    root: Path | None = None,
) -> dict[str, Any]:
    """List recent DagRuns for one DAG (read-only)."""
    return af.list_airflow_dag_runs(dag_id, limit=limit, root=root)


def describe_airflow_det_env(*, root: Path | None = None) -> dict[str, Any]:
    """Decode local airflow/.env DET_* knobs (passwords redacted)."""
    return af.describe_airflow_det_env(root=root)


def preview_backfill_conf(
    interval_start: str,
    interval_end: str,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview backfill conf + trigger command strings (never triggers)."""
    return af.preview_backfill_conf(interval_start, interval_end, root=root)


__all__ = [
    "DEFAULT_LIST_LIMIT",
    "DEFAULT_SAMPLE_LIMIT",
    "MAX_SAMPLE_LIMIT",
    "airflow_health",
    "biglake_register_dry_run",
    "check",
    "cube_load",
    "cube_meta",
    "dbt_dry_run",
    "describe_airflow_det_env",
    "describe_approval",
    "describe_model",
    "describe_pipeline",
    "diagnose_pipeline",
    "diff_bronze_silver",
    "diff_partitions",
    "iceberg_register_dry_run",
    "init_pipeline_dry_run",
    "lake_path_for_pipeline",
    "list_airflow_dag_runs",
    "list_airflow_dags",
    "list_approvals",
    "list_bronze_partitions",
    "list_mappers_tool",
    "list_models",
    "list_pipelines",
    "list_raw_partitions",
    "list_runs",
    "list_sources_tool",
    "mapper_from_diff_dry_run",
    "migrate_dry_run",
    "preview_backfill_conf",
    "prune_dry_run",
    "query_analytics",
    "read_manifest",
    "sample_bronze",
    "sample_raw",
    "scaffold_dbt_dry_run",
    "scaffold_ops_dry_run",
    "schema_from_sample_dry_run",
    "silver_catchup_cleanup_dry_run",
    "silver_catchup_dry_run",
    "summarize_runs",
    "validate_sample",
]
