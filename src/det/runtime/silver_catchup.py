"""Bronze ↔ silver catch-up: latest-per-interval diff, ops manifest, dbt vars.

Correctness grain: for each interval, the latest bronze ``__extract_run_datetime``
must appear in silver. Older siblings are informational only. Catch-up heals via
a replaceable ops manifest and one ``det dbt --catchup`` build (not full-refresh).
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from det.logging import get_logger
from det.mcp.inspect._common import DEFAULT_LIST_LIMIT, clamp_list_limit
from det.mcp.inspect._partitions import list_bronze_runs
from det.mcp.query_sql import analytics_duckdb_path
from det.optional_deps import require_duckdb
from det.runtime.config import PipelineConfig, load_pipeline_config
from det.runtime.ids import dbt_model_slug, parse_canonical_id
from det.runtime.lake import LakeRef, relpath, resolve_lake_roots
from det.runtime.meta import identity_iso
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref
from det.runtime.settings import DetSettings, get_active_settings

logger = get_logger(__name__)

CATCHUP_DIR = ("ops", "silver_catchup")
CATCHUP_MANIFEST_NAME = "manifest.json"
MANIFEST_VERSION = 1


def silver_relation(config: PipelineConfig) -> tuple[str, str]:
    """Return ``(schema, table)`` for the scaffolded parent silver model."""
    provider, _ = parse_canonical_id(config.name)
    slug = dbt_model_slug(config.name)
    return f"silver_{provider}", f"silver_{slug}"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def catchup_manifest_ref(ops_lake: LakeRef) -> LakeRef:
    ref = ops_lake
    for part in CATCHUP_DIR:
        ref = ref / part
    return ref / CATCHUP_MANIFEST_NAME


def resolve_ops_lake(
    *,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    active = settings if settings is not None else get_active_settings()
    if active is None:
        active = DetSettings.from_env(project_root=project_root)
    if lake_path is not None and str(lake_path).strip():
        active = active.with_overrides(lake_override=str(lake_path).strip())
    roots = resolve_lake_roots(active, project_root=project_root)
    return roots.ops


def _norm_ts(value: object) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        try:
            return identity_iso(value)  # type: ignore[arg-type]
        except Exception:
            return identity_iso(str(value))
    return identity_iso(str(value))


def list_silver_extract_runs(
    config: PipelineConfig,
    *,
    project_root: Path,
    analytics_db: Path | None = None,
) -> tuple[set[str], str | None]:
    """Distinct ``__extract_run_datetime`` values present in silver. Empty if missing."""
    schema, table = silver_relation(config)
    db_path = analytics_db if analytics_db is not None else analytics_duckdb_path(project_root)
    if not db_path.is_file():
        return set(), f"DuckDB file not found: {db_path}"
    duckdb = require_duckdb()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        exists = con.execute(
            """
            select count(*) from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [schema, table],
        ).fetchone()
        if not exists or exists[0] == 0:
            return set(), f"table not found: {schema}.{table}"
        # schema/table from silver_relation (pipeline ids), not caller SQL.
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        rows = con.execute(
            f'select distinct "__extract_run_datetime" from {qualified}'  # noqa: S608
        ).fetchall()
    except Exception as exc:
        return set(), str(exc)
    finally:
        con.close()
    out: set[str] = set()
    for (raw,) in rows:
        if raw is None:
            continue
        out.add(_norm_ts(raw))
    return out, None


def _latest_per_interval(
    bronze_runs: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map (interval_start, interval_end) → bronze run with max extract_run_datetime."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for run in bronze_runs:
        start = str(run.get("interval_start") or "")
        end = str(run.get("interval_end") or "")
        ts = _norm_ts(run.get("extract_run_datetime"))
        if not start or not end or not ts:
            continue
        key = (start, end)
        prev = best.get(key)
        if prev is None or _norm_ts(prev.get("extract_run_datetime")) < ts:
            best[key] = {
                "interval_start": start,
                "interval_end": end,
                "extract_run_datetime": ts,
            }
    return best


def diff_bronze_silver(
    pipeline: str | PipelineConfig,
    *,
    project_root: Path,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    detected_at: str | None = None,
) -> dict[str, Any]:
    """Compare latest bronze extract-run per interval to silver coverage."""
    root = project_root.resolve()
    if isinstance(pipeline, PipelineConfig):
        config = pipeline
    else:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        config = load_pipeline_config(resolved.path)

    capped = clamp_list_limit(limit)
    bronze_runs, bronze_note = list_bronze_runs(
        config,
        root=root,
        limit=capped,
        interval_start=interval_start,
        interval_end=interval_end,
    )
    silver_runs, silver_note = list_silver_extract_runs(
        config, project_root=root, analytics_db=analytics_db
    )

    latest = _latest_per_interval(bronze_runs)
    stamp = detected_at or datetime.now(UTC).isoformat()

    catchup: list[dict[str, Any]] = []
    ok_intervals: list[dict[str, Any]] = []
    stale_siblings: list[dict[str, Any]] = []

    latest_keys = {
        (
            r["interval_start"],
            r["interval_end"],
            r["extract_run_datetime"],
        )
        for r in latest.values()
    }

    for (start, end), run in sorted(latest.items(), key=lambda kv: kv[0]):
        ts = run["extract_run_datetime"]
        row = {
            "pipeline": config.name,
            "interval_start": start,
            "interval_end": end,
            "extract_run_datetime": ts,
        }
        if ts in silver_runs:
            ok_intervals.append(row)
        else:
            catchup.append({**row, "detected_at": stamp})

    for run in bronze_runs:
        start = str(run.get("interval_start") or "")
        end = str(run.get("interval_end") or "")
        ts = _norm_ts(run.get("extract_run_datetime"))
        key = (start, end, ts)
        if key in latest_keys:
            continue
        if ts and ts not in silver_runs:
            stale_siblings.append(
                {
                    "pipeline": config.name,
                    "interval_start": start,
                    "interval_end": end,
                    "extract_run_datetime": ts,
                }
            )

    schema, table = silver_relation(config)
    out: dict[str, Any] = {
        "pipeline": config.name,
        "materialized": config.dbt.silver.materialized,
        "silver_schema": schema,
        "silver_table": table,
        "limit": capped,
        "catchup_runs": catchup[:capped],
        "ok_intervals": ok_intervals[:capped],
        "stale_siblings_ignored": stale_siblings[:capped],
        "catchup_count": len(catchup),
        "ok_count": len(ok_intervals),
        "stale_siblings_count": len(stale_siblings),
        "truncated": (
            len(catchup) > capped
            or len(ok_intervals) > capped
            or len(stale_siblings) > capped
            or len(bronze_runs) >= capped
        ),
    }
    notes = [n for n in (bronze_note, silver_note) if n]
    if notes:
        out["note"] = "; ".join(notes)
    return out


def diff_bronze_silver_fleet(
    *,
    project_root: Path,
    pipelines: Sequence[str] | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    detected_at: str | None = None,
) -> dict[str, Any]:
    """Run :func:`diff_bronze_silver` for many pipelines; aggregate catch-up rows."""
    root = project_root.resolve()
    ids = list(pipelines) if pipelines is not None else list_pipeline_ids(root)
    stamp = detected_at or datetime.now(UTC).isoformat()
    per_pipeline: list[dict[str, Any]] = []
    catchup_all: list[dict[str, Any]] = []
    for pipe_id in ids:
        one = diff_bronze_silver(
            pipe_id,
            project_root=root,
            interval_start=interval_start,
            interval_end=interval_end,
            limit=limit,
            analytics_db=analytics_db,
            detected_at=stamp,
        )
        per_pipeline.append(one)
        catchup_all.extend(one.get("catchup_runs") or [])
    return {
        "pipelines": ids,
        "pipeline_count": len(ids),
        "catchup_runs": catchup_all,
        "catchup_count": len(catchup_all),
        "results": per_pipeline,
        "detected_at": stamp,
    }


def manifest_payload_from_catchup(
    catchup_runs: Sequence[dict[str, Any]],
    *,
    detected_at: str | None = None,
) -> dict[str, Any]:
    stamp = detected_at or datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for raw in catchup_runs:
        rows.append(
            {
                "pipeline": str(raw["pipeline"]),
                "extract_run_datetime": _norm_ts(raw["extract_run_datetime"]),
                "interval_start": str(raw.get("interval_start") or ""),
                "interval_end": str(raw.get("interval_end") or ""),
                "detected_at": str(raw.get("detected_at") or stamp),
            }
        )
    return {
        "manifest_version": MANIFEST_VERSION,
        "updated_at": stamp,
        "runs": rows,
    }


def write_catchup_manifest(
    payload: dict[str, Any],
    *,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    path = catchup_manifest_ref(ops)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    n_runs = len(payload.get("runs") or [])
    logger.info("silver catchup manifest written", path=str(path), runs=n_runs)
    return path


def read_catchup_manifest(
    *,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> dict[str, Any] | None:
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    path = catchup_manifest_ref(ops)
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"catch-up manifest must be a JSON object: {path}")
    return raw


def catchup_vars_from_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """dbt ``--vars`` map: ``det_catchup_by_pipeline`` → pipeline → run timestamps."""
    by_pipeline: dict[str, list[str]] = defaultdict(list)
    for row in payload.get("runs") or []:
        pipe = str(row.get("pipeline") or "").strip()
        ts = _norm_ts(row.get("extract_run_datetime"))
        if pipe and ts and ts not in by_pipeline[pipe]:
            by_pipeline[pipe].append(ts)
    return {
        "det_catchup": True,
        "det_catchup_by_pipeline": dict(by_pipeline),
    }


def catchup_select_from_manifest(
    payload: dict[str, Any],
    *,
    project_root: Path,
) -> list[str]:
    """dbt ``--select`` for silver models listed in the manifest."""
    root = project_root.resolve()
    seen: set[str] = set()
    selects: list[str] = []
    for row in payload.get("runs") or []:
        pipe = str(row.get("pipeline") or "").strip()
        if not pipe or pipe in seen:
            continue
        seen.add(pipe)
        try:
            resolved = resolve_pipeline_ref(pipe, project_root=root)
            config = load_pipeline_config(resolved.path)
        except Exception:
            slug = dbt_model_slug(pipe)
            selects.append(f"silver_{slug}")
            continue
        slug = dbt_model_slug(config.name)
        selects.append(f"silver_{slug}")
    return selects


def plan_catchup_manifest(
    *,
    project_root: Path,
    pipeline: str | None = None,
    all_pipelines: bool = False,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
) -> dict[str, Any]:
    """Build a replaceable catch-up manifest payload (does not write)."""
    root = project_root.resolve()
    rel = "/".join((*CATCHUP_DIR, CATCHUP_MANIFEST_NAME))
    if all_pipelines:
        fleet = diff_bronze_silver_fleet(
            project_root=root,
            interval_start=interval_start,
            interval_end=interval_end,
            limit=limit,
            analytics_db=analytics_db,
        )
        payload = manifest_payload_from_catchup(fleet.get("catchup_runs") or [])
        return {
            "dry_run": True,
            "diff": fleet,
            "manifest": payload,
            "manifest_relpath": rel,
        }
    if pipeline is None:
        raise ValueError("pipeline is required unless all_pipelines=True")
    one = diff_bronze_silver(
        pipeline,
        project_root=root,
        interval_start=interval_start,
        interval_end=interval_end,
        limit=limit,
        analytics_db=analytics_db,
    )
    payload = manifest_payload_from_catchup(one.get("catchup_runs") or [])
    return {
        "dry_run": True,
        "diff": one,
        "manifest": payload,
        "manifest_relpath": rel,
    }


def manifest_relpath_for_root(project_root: Path, path: LakeRef) -> str:
    return relpath(path, project_root)
