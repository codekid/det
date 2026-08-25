"""Read-only inspect and dry-run tool implementations for DET MCP."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from det.mcp import airflow_inspect as af
from det.mcp import generate as gen
from det.mcp import inspect as insp
from det.mcp.context import PathSandboxError, project_root, resolve_under_root
from det.mcp.reload import refresh_det_runtime
from det.runtime.lake import LakeRef
from det.runtime.lake import relpath as lake_relpath
from det.runtime.manifest import is_committed_raw_dir

DEFAULT_LIST_LIMIT = insp.DEFAULT_LIST_LIMIT
DEFAULT_SAMPLE_LIMIT = insp.DEFAULT_SAMPLE_LIMIT
MAX_SAMPLE_LIMIT = insp.MAX_SAMPLE_LIMIT


def _prepare_tool() -> None:
    """Evict stale det.* modules so long-lived MCP sees disk edits."""
    import importlib

    import det.mcp.generate as generate_mod
    import det.mcp.inspect as inspect_mod

    refresh_det_runtime()
    # Re-bind inspect/generate so their imports of registry/plugins/runtime are fresh.
    global insp, gen
    insp = importlib.reload(inspect_mod)
    gen = importlib.reload(generate_mod)


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _approval_plan(command: str, argv: list[str]) -> dict[str, Any]:
    from det.runtime.approval import make_plan

    return make_plan(command, argv).to_dict()


def _pipeline_path(pipeline: str, root: Path) -> Path:
    """Resolve a pipeline name (``noaa.storm_events``), path, or nested stem."""
    from det.runtime.pipelines import resolve_pipeline_ref

    return resolve_pipeline_ref(pipeline, project_root=root).path


def _load_pipeline(pipeline: str, root: Path):
    from det.runtime.config import load_pipeline_config
    from det.runtime.pipelines import resolve_pipeline_ref

    resolved = resolve_pipeline_ref(pipeline, project_root=root)
    return load_pipeline_config(resolved.path), resolved.path


def _rel(path: Path | LakeRef, root: Path) -> str:
    return lake_relpath(path, root)


def _parse_hive_key(dirname: str, prefix: str) -> str | None:
    if not dirname.startswith(prefix):
        return None
    return dirname[len(prefix) :]


def _walk_hive_runs(
    dataset_dir: Path | LakeRef,
    *,
    root: Path,
    limit: int,
    require_committed: bool = False,
) -> list[dict[str, Any]]:
    """Walk hive interval/extract-run dirs; cap at *limit* runs."""
    out: list[dict[str, Any]] = []
    if not dataset_dir.is_dir():
        return out
    for start_dir in sorted(dataset_dir.iterdir()):
        if not start_dir.is_dir():
            continue
        start_val = _parse_hive_key(start_dir.name, "__interval_start_datetime=")
        if start_val is None:
            continue
        for end_dir in sorted(start_dir.iterdir()):
            if not end_dir.is_dir():
                continue
            end_val = _parse_hive_key(end_dir.name, "__interval_end_datetime=")
            if end_val is None:
                continue
            for run_dir in sorted(end_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                run_val = _parse_hive_key(run_dir.name, "__extract_run_datetime=")
                if run_val is None:
                    continue
                if require_committed and not is_committed_raw_dir(run_dir):
                    continue
                out.append(
                    {
                        "interval_start": start_val,
                        "interval_end": end_val,
                        "extract_run_datetime": run_val,
                        "path": _rel(run_dir, root),
                    }
                )
                if len(out) >= limit:
                    return out
    return out


def list_pipelines(*, root: Path | None = None) -> dict[str, Any]:
    _prepare_tool()
    from det.runtime.pipelines import list_pipeline_ids

    base = _root(root)
    return {"project_root": str(base), "pipelines": list_pipeline_ids(base)}


def list_sources_tool(*, root: Path | None = None) -> dict[str, Any]:
    _prepare_tool()
    from det.plugins import load_plugins
    from det.runtime.discovery import probe_source_load_errors
    from det.runtime.registry import list_sources

    _ = _root(root)
    load_plugins()
    base = _root(root)
    return {
        "sources": list_sources(project_root=base),
        "errors": probe_source_load_errors(project_root=base),
    }


def list_mappers_tool(*, root: Path | None = None) -> dict[str, Any]:
    _prepare_tool()
    from det.plugins import load_plugins
    from det.runtime.registry import describe_mappers, list_mappers

    _ = _root(root)
    load_plugins()
    return {
        "mappers": [{"name": name, "summary": summary} for name, summary in describe_mappers()],
        "names": list_mappers(),
    }


def _connection_display(destination: Any) -> str | None:
    """Never echo a Postgres DSN — report the secret name (DuckDB is a file path)."""
    if destination.type != "postgres":
        return destination.connection
    if destination.connection_env:
        return f"env:{destination.connection_env}"
    return "(postgres DSN in destination.connection — move it to connection_env)"


def describe_pipeline(pipeline: str, *, root: Path | None = None) -> dict[str, Any]:
    _prepare_tool()
    from det.runtime.ids import sql_names_for_config

    base = _root(root)
    config, path = _load_pipeline(pipeline, base)
    silver = config.dbt.silver
    sql_schema, sql_table = sql_names_for_config(config)
    return {
        "name": config.name,
        "path": _rel(path, base),
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
    _prepare_tool()
    from det.destinations.models import raw_dataset_dir

    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    dataset_dir = raw_dataset_dir(config, base)
    capped = max(1, min(int(limit), DEFAULT_LIST_LIMIT))
    runs = _walk_hive_runs(dataset_dir, root=base, limit=capped, require_committed=True)
    return {
        "pipeline": config.name,
        "dataset_dir": _rel(dataset_dir, base),
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
    _prepare_tool()
    from det.destinations.models import bronze_dataset_dir
    from det.runtime.ids import sql_names_for_config

    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
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
                "location": _rel(dataset_dir, base),
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
            "location": _rel(dataset_dir, base),
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
    runs = _walk_hive_runs(dataset_dir, root=base, limit=capped)
    return {
        "pipeline": config.name,
        "destination_type": "filesystem",
        "dataset_dir": _rel(dataset_dir, base),
        "limit": capped,
        "truncated": len(runs) >= capped,
        "runs": runs,
    }


def read_manifest(run_path: str, *, root: Path | None = None) -> dict[str, Any]:
    """Read meta/manifest.json for a raw extract-run directory under the lake."""
    _prepare_tool()
    base = _root(root)
    run_dir = resolve_under_root(run_path, root=base)
    if not run_dir.is_dir():
        raise FileNotFoundError(f"run path is not a directory: {run_dir}")

    manifest = run_dir / "meta" / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError(f"manifest not found: {_rel(manifest, base)}")

    # Must live under a lake raw/ tree (…/raw/<dataset>/…/meta/manifest.json).
    parts = manifest.resolve().parts
    if "raw" not in parts or "meta" not in parts:
        raise PathSandboxError(
            f"manifest must be under a lake raw/…/meta/ path: {_rel(manifest, base)}"
        )

    raw = json.loads(manifest.read_text(encoding="utf-8"))
    return {
        "path": _rel(manifest, base),
        "manifest": raw,
    }


def prune_dry_run(
    pipeline: str,
    *,
    interval_start: str,
    interval_end: str | None = None,
    keep: int = 1,
    root: Path | None = None,
) -> dict[str, Any]:
    _prepare_tool()
    from det.runtime.prune import BronzePruner

    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    plan = BronzePruner(base).plan(
        config,
        interval_start=interval_start,
        interval_end=interval_end,
        keep=keep,
    )
    from det.runtime.approval import prune_write_argv

    return {
        "pipeline": config.name,
        "keep": keep,
        "approval_plan": _approval_plan(
            "prune",
            prune_write_argv(
                config.name,
                interval_start,
                interval_end=interval_end,
                keep=keep,
            ),
        ),
        "remove_count": plan.remove_count,
        "to_remove": [
            {
                "interval_start": r.interval_start,
                "interval_end": r.interval_end,
                "extract_run_datetime": r.extract_run_datetime,
                "path": _rel(r.path, base) if r.path is not None else None,
            }
            for r in plan.to_remove
        ],
        "to_keep": [
            {
                "interval_start": r.interval_start,
                "interval_end": r.interval_end,
                "extract_run_datetime": r.extract_run_datetime,
                "path": _rel(r.path, base) if r.path is not None else None,
            }
            for r in plan.to_keep
        ],
    }


def dbt_dry_run(
    pipeline: str | None = None,
    *,
    command: str = "build",
    select: list[str] | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    _prepare_tool()
    from det.runtime.dbt_runner import analytics_exclude, run_dbt

    base = _root(root)
    pipeline_arg: Path | str | None = None
    if pipeline is not None:
        pipeline_arg = _pipeline_path(pipeline, base)
    result = run_dbt(
        project_root=base,
        command=command,  # type: ignore[arg-type]
        select=select,
        exclude=analytics_exclude(select),
        pipeline=pipeline_arg,
        dry_run=True,
    )
    from det.runtime.approval import dbt_write_argv

    return {
        "dry_run": True,
        "command": result.command,
        "select": list(result.select),
        "project_dir": _rel(result.project_dir, base),
        "lake_path": result.lake_path,
        "bronze_source": result.bronze_source,
        "approval_plan": _approval_plan(
            "dbt",
            dbt_write_argv(pipeline, command=command, select=select),
        ),
    }


def scaffold_dbt_dry_run(
    pipeline: str,
    *,
    force: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    _prepare_tool()
    from det.scaffold.dbt import scaffold_dbt

    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    from det.runtime.approval import scaffold_dbt_write_argv

    result = scaffold_dbt(config, project_root=base, force=force, dry_run=True)
    return {
        "dry_run": True,
        "dataset": result.dataset,
        "approval_plan": _approval_plan(
            "scaffold-dbt",
            scaffold_dbt_write_argv(config.name, force=force),
        ),
        "actions": [
            {
                "action": a.action,
                "path": _rel(a.path, base),
                "detail": a.detail,
            }
            for a in result.actions
        ],
    }


def init_pipeline_dry_run(
    name: str,
    source_type: str,
    *,
    destination_type: str = "iceberg",
    connection: str | None = None,
    lake_path: str | None = None,
    skip_dbt: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    _prepare_tool()
    from det.scaffold.init_pipeline import init_pipeline

    base = _root(root)
    result = init_pipeline(
        name=name,
        source_type=source_type,
        project_root=base,
        dry_run=True,
        skip_dbt=skip_dbt,
        destination_type=destination_type,
        lake_path=lake_path,
        connection=connection,
    )
    from det.runtime.approval import init_pipeline_write_argv

    return {
        "dry_run": True,
        "name": result.name,
        "pipeline_path": _rel(result.pipeline_path, base),
        "schema_path": _rel(result.schema_path, base),
        "approval_plan": _approval_plan(
            "init-pipeline",
            init_pipeline_write_argv(
                name,
                source_type,
                destination_type=destination_type,
                connection=connection,
                lake_path=lake_path,
                skip_dbt=skip_dbt,
            ),
        ),
        "actions": [
            {
                "action": a.action,
                "path": _rel(a.path, base),
                "detail": a.detail,
            }
            for a in result.actions
        ],
    }


def lake_path_for_pipeline(pipeline: str, *, root: Path | None = None) -> str:
    _prepare_tool()
    from det.destinations.models import lake_root

    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    return _rel(lake_root(config.destination, base), base)


def diff_partitions(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Compare raw vs bronze extract-run coverage (hive and/or SQL meta)."""
    _prepare_tool()
    return insp.diff_partitions(
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
    _prepare_tool()
    return insp.sample_raw(
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
    _prepare_tool()
    return insp.validate_sample(
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
    _prepare_tool()
    return insp.sample_bronze(
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
    _prepare_tool()
    return insp.diagnose_pipeline(
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
    _prepare_tool()
    return gen.schema_from_sample_dry_run(
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
    _prepare_tool()
    return gen.mapper_from_diff_dry_run(
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


def migrate_dry_run(
    pipeline: str,
    to_bronze: str,
    schema: str,
    mapper: str,
    interval_start: str | None = None,
    *,
    interval_end: str | None = None,
    from_raw: str | None = None,
    validate_limit: int = MAX_SAMPLE_LIMIT,
    wire_version: int | None = None,
    recreate_iceberg: bool = False,
    all_raw: bool = False,
    all_raw_runs: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview det migrate: parse/map/validate raw partitions; never writes bronze."""
    _prepare_tool()
    from det.mcp.inspect import clamp_sample_limit
    from det.runtime.approval import migrate_write_argv
    from det.runtime.migrate import BronzeMigrator, MigratePlan
    from det.runtime.pipelines import resolve_pipeline_ref

    if all_raw:
        if interval_start is not None or interval_end is not None:
            raise ValueError("--all-raw cannot be combined with interval_start/end")
        if not recreate_iceberg:
            raise ValueError("--all-raw requires recreate_iceberg")
    elif interval_start is None:
        raise ValueError("interval_start is required unless all_raw")

    base = _root(root)
    capped = clamp_sample_limit(validate_limit)
    resolved = resolve_pipeline_ref(pipeline, project_root=base)
    schema_path = Path(schema)
    if not schema_path.is_absolute():
        schema_path = base / schema_path
    plan = BronzeMigrator(base).migrate(
        pipeline=resolved.path,
        to_bronze=to_bronze,
        schema_path=schema_path,
        mapper_name=mapper,
        interval_start=interval_start,
        interval_end=interval_end,
        from_raw=from_raw,
        dry_run=True,
        validate_limit=capped,
        wire_version=wire_version,
        recreate_iceberg=recreate_iceberg,
        all_raw=all_raw,
        all_raw_runs=all_raw_runs,
    )
    assert isinstance(plan, MigratePlan)
    out = plan.to_dict()
    out["validate_limit"] = capped
    out["pipeline"] = resolved.canonical_id
    out["approval_plan"] = _approval_plan(
        "migrate",
        migrate_write_argv(
            resolved.canonical_id,
            to_bronze,
            schema,
            mapper,
            interval_start,
            interval_end=interval_end,
            from_raw=from_raw,
            wire_version=wire_version,
            recreate_iceberg=recreate_iceberg,
            all_raw=all_raw,
            all_raw_runs=all_raw_runs,
        ),
    )
    bits: list[str] = []
    if recreate_iceberg:
        bits.append(" --recreate-iceberg")
    if all_raw:
        bits.append(" --all-raw")
    if all_raw_runs:
        bits.append(" --all-raw-runs")
    flag_bit = "".join(bits)
    if all_raw:
        scope = ""
    else:
        scope = f" -s {interval_start}"
    out["note"] = (
        "Dry-run only — no bronze written. Apply with "
        f"`det migrate -p {resolved.canonical_id} --to-bronze {to_bronze} "
        f"--schema {schema} --mapper {mapper}{scope}{flag_bit}` after user confirms."
    )
    return out


_RECEIPT_SECRET_KEYS = frozenset(
    {
        "connection",
        "password",
        "dsn",
        "secret",
        "token",
        "api_key",
        "apikey",
    }
)
_RECEIPT_NOTE = (
    "Receipts are observability for extract/load attempts. "
    "meta/manifest.json is the authority for landed partitions."
)


def _runs_lake(pipeline: str | None, root: Path):
    from det.destinations.models import lake_root
    from det.logging import sanitize_lake_uri
    from det.runtime.config import load_pipeline_config
    from det.runtime.lake import open_lake, pick_lake_spec
    from det.runtime.pipelines import resolve_pipeline_ref

    if pipeline:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        resolve_under_root(resolved.path, root=root)
        config = load_pipeline_config(resolved.path)
        lake = lake_root(config.destination, root)
        return lake, config.name, sanitize_lake_uri(str(lake))
    spec = pick_lake_spec(destination_path=None)
    lake = open_lake(spec, root)
    return lake, None, sanitize_lake_uri(str(lake))


def _public_receipt(row: dict[str, Any], *, root: Path) -> dict[str, Any]:
    from det.logging import sanitize_lake_uri

    out = {
        key: value
        for key, value in row.items()
        if key.lower() not in _RECEIPT_SECRET_KEYS
        and not key.lower().endswith(("_password", "_secret", "_token", "_dsn", "_connection"))
    }
    path = out.get("path")
    if isinstance(path, str):
        if "://" in path:
            out["path"] = sanitize_lake_uri(path)
        else:
            try:
                out["path"] = _rel(Path(path), root)
            except Exception:
                out["path"] = path
    return out


def list_runs(
    pipeline: str | None = None,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    command: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    List extract/load run receipts (observability).

    Manifest remains the authority for landed partitions. Never returns a
    destination connection.
    """
    _prepare_tool()
    from det.mcp.inspect import clamp_list_limit
    from det.runtime.receipts import list_receipts

    base = _root(root)
    lake, pipe_id, lake_display = _runs_lake(pipeline, base)
    capped = clamp_list_limit(limit)
    rows = list_receipts(
        lake,
        pipeline=pipe_id,
        since=since,
        until=until,
        status=status,
        command=command,
        limit=capped,
    )
    public = [_public_receipt(row, root=base) for row in rows]
    return {
        "pipeline": pipe_id,
        "lake": lake_display,
        "limit": capped,
        "truncated": len(rows) >= capped,
        "note": _RECEIPT_NOTE,
        "runs": public,
    }


def list_models(*, root: Path | None = None) -> dict[str, Any]:
    """List dbt models (stg/silver/gold/ops) from dbt/models YAML + SQL."""
    _prepare_tool()
    from det.mcp.catalog import list_dbt_models

    return list_dbt_models(root=_root(root))


def describe_model(name: str, *, root: Path | None = None) -> dict[str, Any]:
    """Describe one dbt model: schema, grain, columns from YAML."""
    _prepare_tool()
    from det.mcp.catalog import describe_dbt_model

    return describe_dbt_model(name, root=_root(root))


def query_analytics(
    sql: str,
    *,
    warehouse: str = "analytics",
    limit: int = DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Capped read-only SELECT on analytics or ops DuckDB (not certified metrics)."""
    _prepare_tool()
    from det.mcp.query_sql import query_analytics as run_query

    if warehouse not in {"analytics", "ops"}:
        return {
            "ok": False,
            "error": "invalid_warehouse",
            "detail": "warehouse must be analytics or ops",
            "rows": [],
        }
    return run_query(sql, warehouse=warehouse, limit=limit, root=_root(root))


def cube_meta(*, root: Path | None = None) -> dict[str, Any]:
    """Cube Core meta (cubes/measures/dimensions). Start Cube with make cube-up."""
    _prepare_tool()
    from det.mcp.cube_client import cube_meta as fetch_meta

    return fetch_meta(root=_root(root))


def cube_load(
    measures: list[str],
    *,
    dimensions: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    limit: int = DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Run a Cube REST load query (certified gold/ops metrics)."""
    _prepare_tool()
    from det.mcp.cube_client import cube_load as run_load

    return run_load(
        measures=measures,
        dimensions=dimensions,
        filters=filters,
        limit=limit,
        root=_root(root),
    )


def check(
    pipeline: str | None = None,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Pipeline structure check (schema file, source plugin, optional dbt models).

    Same payload as ``det check --json``. Never writes; not a substitute for
    extract/load.
    """
    _prepare_tool()
    from det.runtime.check import check_project, findings_payload

    base = _root(root)
    findings = check_project(base, pipeline=pipeline)
    return findings_payload(findings)


def list_approvals(*, root: Path | None = None) -> dict[str, Any]:
    """Unused, unexpired approval records (MCP never creates these files)."""
    _prepare_tool()
    from det.runtime.approval import list_unused_approvals

    base = _root(root)
    return {"project_root": str(base), "approvals": list_unused_approvals(base)}


def describe_approval(approval_id: str, *, root: Path | None = None) -> dict[str, Any]:
    """Load one approval record; expired is derived at read time."""
    _prepare_tool()
    from det.runtime.approval import ApprovalError, effective_status, load_approval

    base = _root(root)
    try:
        record = dict(load_approval(base, approval_id))
    except ApprovalError as exc:
        if exc.code == "approval_not_found":
            raise FileNotFoundError(str(exc)) from exc
        raise
    record["status"] = effective_status(record)
    return record


def summarize_runs(
    pipeline: str | None = None,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    command: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Summarize extract/load run receipts (counts, error codes, p50/p95).

    Numbers only — no SLO thresholds. Manifest remains the data authority.
    """
    _prepare_tool()
    from det.runtime.receipts import summarize_receipts

    base = _root(root)
    lake, pipe_id, lake_display = _runs_lake(pipeline, base)
    payload = summarize_receipts(
        lake,
        pipeline=pipe_id,
        since=since,
        until=until,
        status=status,
        command=command,
    )
    payload["pipeline"] = pipe_id
    payload["lake"] = lake_display
    payload["note"] = _RECEIPT_NOTE
    return payload


def biglake_register_dry_run(
    *,
    pipeline: str | None = None,
    lake_path: str | None = None,
    project: str | None = None,
    location: str | None = None,
    connection: str | None = None,
    skip_ops: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview BigLake registration plan (never creates BQ resources)."""
    _prepare_tool()
    from det.runtime.biglake_register import (
        biglake_register_write_argv,
        build_biglake_register_plan,
    )

    base = _root(root)
    pipe_path = None
    if pipeline:
        _, pipe_path = _load_pipeline(pipeline, base)
    argv = biglake_register_write_argv(
        lake_path=lake_path,
        pipeline=pipeline,
        project=project,
        location=location,
        connection=connection,
        skip_ops=skip_ops,
    )
    plan = build_biglake_register_plan(
        project_root=base,
        lake_path=lake_path,
        pipeline=pipe_path,
        project=project,
        location=location,
        connection=connection,
        include_ops=not skip_ops and pipeline is None,
    )
    return {
        **plan.to_dict(),
        "iam_hint": build_iam_hint(plan),
        "approval_plan": _approval_plan("biglake-register", argv),
        "note": (
            "Dry-run only — no BigLake tables created. Operator: det approve --plan "
            "<approval_plan> --approved-by <id>. Agent: det biglake-register --apply "
            "--approval <id> in a later turn."
        ),
    }
