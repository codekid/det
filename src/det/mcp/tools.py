"""Read-only inspect and dry-run tool implementations for DET MCP."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from det.destinations.models import bronze_dataset_dir, lake_root, raw_dataset_dir
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

DEFAULT_LIST_LIMIT = 200


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
