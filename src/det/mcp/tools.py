"""Read-only inspect and dry-run tool implementations for DET MCP."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from det.destinations.models import bronze_dataset_dir, lake_root, raw_dataset_dir
from det.mcp import airflow_inspect as af
from det.mcp import generate as gen
from det.mcp import inspect as insp
from det.mcp.context import PathSandboxError, project_root, resolve_under_root
from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config
from det.runtime.dbt_runner import run_dbt
from det.runtime.ids import sql_names_for_config
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref
from det.runtime.prune import BronzePruner
from det.runtime.registry import describe_mappers, list_mappers, list_sources
from det.scaffold.dbt import scaffold_dbt
from det.scaffold.init_pipeline import init_pipeline

DEFAULT_LIST_LIMIT = insp.DEFAULT_LIST_LIMIT
DEFAULT_SAMPLE_LIMIT = insp.DEFAULT_SAMPLE_LIMIT
MAX_SAMPLE_LIMIT = insp.MAX_SAMPLE_LIMIT


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _pipeline_path(pipeline: str, root: Path) -> Path:
    """Resolve a pipeline name (``noaa.storm_events``), path, or nested stem."""
    return resolve_pipeline_ref(pipeline, project_root=root).path


def _load_pipeline(pipeline: str, root: Path):
    resolved = resolve_pipeline_ref(pipeline, project_root=root)
    return load_pipeline_config(resolved.path), resolved.path


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path.resolve())


def _parse_hive_key(dirname: str, prefix: str) -> str | None:
    if not dirname.startswith(prefix):
        return None
    return dirname[len(prefix) :]


def _walk_hive_runs(dataset_dir: Path, *, root: Path, limit: int) -> list[dict[str, Any]]:
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
    base = _root(root)
    return {"project_root": str(base), "pipelines": list_pipeline_ids(base)}


def list_sources_tool(*, root: Path | None = None) -> dict[str, Any]:
    _ = _root(root)
    load_plugins()
    return {"sources": list_sources()}


def list_mappers_tool(*, root: Path | None = None) -> dict[str, Any]:
    _ = _root(root)
    load_plugins()
    return {
        "mappers": [
            {"name": name, "summary": summary} for name, summary in describe_mappers()
        ],
        "names": list_mappers(),
    }


def describe_pipeline(pipeline: str, *, root: Path | None = None) -> dict[str, Any]:
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
            "connection": config.destination.connection,
            "sql_schema": sql_schema,
            "sql_table": sql_table,
        },
        "dataset": config.bronze_dataset(),
        "fs_dataset": config.fs_dataset_relpath(),
        "ingestion": {"library": config.ingestion.library},
        "dbt": {
            "silver": {
                "materialized": silver.materialized,
                "unique_key": list(silver.unique_key),
                "order_by": list(silver.order_by),
                "incremental_strategy": silver.incremental_strategy,
                "watermark": silver.watermark,
                "lookback": silver.lookback,
            }
        },
    }


def list_raw_partitions(
    pipeline: str,
    *,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    dataset_dir = raw_dataset_dir(config, base)
    capped = max(1, min(int(limit), DEFAULT_LIST_LIMIT))
    runs = _walk_hive_runs(dataset_dir, root=base, limit=capped)
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
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    dest = config.destination
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
            hint["connection"] = "(postgres DSN)"
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
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    plan = BronzePruner(base).plan(
        config,
        interval_start=interval_start,
        interval_end=interval_end,
        keep=keep,
    )
    return {
        "pipeline": config.name,
        "keep": keep,
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
    base = _root(root)
    pipeline_arg: Path | str | None = None
    if pipeline is not None:
        pipeline_arg = _pipeline_path(pipeline, base)
    result = run_dbt(
        project_root=base,
        command=command,  # type: ignore[arg-type]
        select=select,
        pipeline=pipeline_arg,
        dry_run=True,
    )
    return {
        "dry_run": True,
        "command": result.command,
        "select": list(result.select),
        "project_dir": _rel(result.project_dir, base),
        "lake_path": result.lake_path,
        "bronze_source": result.bronze_source,
    }


def scaffold_dbt_dry_run(
    pipeline: str,
    *,
    force: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    result = scaffold_dbt(config, project_root=base, force=force, dry_run=True)
    return {
        "dry_run": True,
        "dataset": result.dataset,
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
    destination_type: str = "filesystem",
    connection: str | None = None,
    lake_path: str = "./data/lake",
    skip_dbt: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
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
    return {
        "dry_run": True,
        "name": result.name,
        "pipeline_path": _rel(result.pipeline_path, base),
        "schema_path": _rel(result.schema_path, base),
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
    return af.preview_backfill_conf(
        interval_start, interval_end, root=root
    )


def migrate_dry_run(
    pipeline: str,
    to_bronze: str,
    schema: str,
    mapper: str,
    interval_start: str,
    *,
    interval_end: str | None = None,
    from_raw: str | None = None,
    validate_limit: int = MAX_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview det migrate: parse/map/validate raw partitions; never writes bronze."""
    from det.mcp.inspect import clamp_sample_limit
    from det.runtime.migrate import BronzeMigrator, MigratePlan
    from det.runtime.pipelines import resolve_pipeline_ref

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
    )
    assert isinstance(plan, MigratePlan)
    out = plan.to_dict()
    out["validate_limit"] = capped
    out["pipeline"] = resolved.canonical_id
    out["note"] = (
        "Dry-run only — no bronze written. Apply with "
        f"`det migrate -p {resolved.canonical_id} --to-bronze {to_bronze} "
        f"--schema {schema} --mapper {mapper} -s {interval_start}` "
        "after user confirms."
    )
    return out
