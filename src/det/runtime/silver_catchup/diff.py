"""Bronze ↔ silver catch-up: latest-per-interval diff, ops manifest, dbt vars.

Correctness grain: for each interval, the latest bronze ``__extract_run_datetime``
must appear in silver for that same ``(interval_start, interval_end)``. Coverage
keys are ``(interval_start, interval_end, extract_run_datetime)`` — run timestamps
alone are not unique across intervals. Older siblings are informational only.
Catch-up heals via an **immutable** ops manifest pair
(``ops/silver_catchup/<manifest_id>.json`` + ``.runs.jsonl``) and one
``det dbt --catchup`` build (DuckDB ``read_json`` or BigQuery external table on
GCS; not full-refresh).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from det.runtime.bronze_runs import list_bronze_runs
from det.runtime.config import PipelineConfig, load_pipeline_config
from det.runtime.limits import DEFAULT_LIST_LIMIT, clamp_list_limit
from det.runtime.meta import identity_iso
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref
from det.runtime.silver_catchup.bq_heal import (
    analytics_target_is_bigquery,
    _list_silver_extract_runs_bigquery,
)
from det.runtime.silver_catchup.duckdb_silver import _list_silver_extract_runs_duckdb
from det.runtime.silver_catchup.ids import (
    _APPLY_BRONZE_CAP,
    _coverage_key,
    _norm_ts,
    parse_extract_lookback,
    silver_relation,
    validate_catchup_candidate_scope,
)

def _intervals_from_runs(
    bronze_runs: Sequence[dict[str, Any]],
) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for run in bronze_runs:
        start = _norm_ts(run.get("interval_start"))
        end = _norm_ts(run.get("interval_end"))
        if not start or not end:
            continue
        key = (start, end)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out

def list_silver_extract_runs(
    config: PipelineConfig,
    *,
    project_root: Path,
    analytics_db: Path | None = None,
    intervals: Sequence[tuple[str, str]] | None = None,
) -> tuple[set[tuple[str, str, str]], str | None]:
    """Distinct silver coverage keys ``(interval_start, interval_end, extract_run)``.

    When ``DET_DBT_TARGET=bigquery``, reads BigQuery silver
    (``DET_GCP_PROJECT`` / custom schema from ``silver_relation``). Otherwise
    reads DuckDB analytics (``analytics_duckdb_path`` / ``DET_ANALYTICS_DUCKDB``).

    When ``intervals`` is set, probes only those ``(start, end)`` pairs (Mode A
    or interval-scoped Mode B). ``intervals=[]`` returns empty without scanning.
    ``intervals=None`` keeps a full-table ``DISTINCT`` (unscoped Mode B).

    Extract-run timestamps alone are not unique across intervals (parallel runs can
    share a second-precision clock), so membership must include interval bounds.
    """
    if analytics_target_is_bigquery():
        return _list_silver_extract_runs_bigquery(config, intervals=intervals)
    return _list_silver_extract_runs_duckdb(
        config,
        project_root=project_root,
        analytics_db=analytics_db,
        intervals=intervals,
    )


def _latest_per_interval(
    bronze_runs: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map (interval_start, interval_end) → bronze run with max extract_run_datetime."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for run in bronze_runs:
        start = _norm_ts(run.get("interval_start"))
        end = _norm_ts(run.get("interval_end"))
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


def _expand_bronze_for_intervals(
    config: PipelineConfig,
    *,
    root: Path,
    intervals: Sequence[tuple[str, str]],
    limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    """Re-list all bronze extract runs for each touched interval (siblings)."""
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    note: str | None = None
    for start, end in intervals:
        runs, one_note = list_bronze_runs(
            config,
            root=root,
            limit=limit,
            interval_start=start,
            interval_end=end,
        )
        if one_note:
            note = one_note
        for run in runs:
            s = _norm_ts(run.get("interval_start"))
            e = _norm_ts(run.get("interval_end"))
            if s != start or e != end:
                continue
            ts = _norm_ts(run.get("extract_run_datetime"))
            if not ts:
                continue
            key = (start, end, ts)
            by_key[key] = {
                **run,
                "interval_start": start,
                "interval_end": end,
                "extract_run_datetime": ts,
            }
            if len(by_key) >= limit:
                return list(by_key.values()), note
    return list(by_key.values()), note


def _load_bronze_candidates(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None,
    interval_end: str | None,
    extract_lookback: str | None,
) -> tuple[list[dict[str, Any]], str | None, str, str | None]:
    """Return (bronze_runs, note, candidate_mode, lookback_raw)."""
    lookback_raw = (
        str(extract_lookback).strip() if extract_lookback is not None else ""
    ) or None
    validate_catchup_candidate_scope(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=lookback_raw,
    )
    if lookback_raw:
        delta = parse_extract_lookback(lookback_raw)
        since = identity_iso(datetime.now(UTC) - delta)
        recent, note = list_bronze_runs(
            config,
            root=root,
            limit=limit,
            extract_run_since=since,
        )
        intervals = _intervals_from_runs(recent)
        if not intervals:
            return [], note, "extract_lookback", lookback_raw
        expanded, expand_note = _expand_bronze_for_intervals(
            config, root=root, intervals=intervals, limit=limit
        )
        return expanded, note or expand_note, "extract_lookback", lookback_raw

    runs, note = list_bronze_runs(
        config,
        root=root,
        limit=limit,
        interval_start=interval_start,
        interval_end=interval_end,
    )
    if interval_start is not None:
        return runs, note, "interval", None
    return runs, note, "full", None


def diff_bronze_silver(
    pipeline: str | PipelineConfig,
    *,
    project_root: Path,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_lookback: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    detected_at: str | None = None,
    complete: bool = False,
) -> dict[str, Any]:
    """Compare latest bronze extract-run per interval to silver coverage.

    When ``complete=True`` (manifest plan/apply), list bronze without the MCP
    display clamp and return every catch-up row. Raises if the safety cap is hit.

    ``extract_lookback`` (e.g. ``48h``) enables Mode A: candidates from recent
    bronze extract runs, silver probed only for those intervals. Mode A always
    discovers up to ``_APPLY_BRONZE_CAP``; ``limit`` only truncates displayed
    rows. Omit lookback for Mode B (full census or ``-s``/``-e`` interval window).
    """
    root = project_root.resolve()
    if isinstance(pipeline, PipelineConfig):
        config = pipeline
    else:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        config = load_pipeline_config(resolved.path)

    lookback_set = bool(extract_lookback and str(extract_lookback).strip())
    display_limit = clamp_list_limit(limit)
    if complete or lookback_set:
        discovery_cap = _APPLY_BRONZE_CAP
    else:
        discovery_cap = display_limit

    bronze_runs, bronze_note, candidate_mode, lookback_raw = _load_bronze_candidates(
        config,
        root=root,
        limit=discovery_cap,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
    )
    if complete and len(bronze_runs) >= _APPLY_BRONZE_CAP:
        raise ValueError(
            "catch-up apply found too many bronze runs "
            f"(>={_APPLY_BRONZE_CAP}); narrow -s/-e / --extract-lookback "
            "or raise the apply cap"
        )

    probe_intervals: list[tuple[str, str]] | None
    if candidate_mode == "full":
        probe_intervals = None
    else:
        probe_intervals = _intervals_from_runs(bronze_runs)

    silver_keys, silver_note = list_silver_extract_runs(
        config,
        project_root=root,
        analytics_db=analytics_db,
        intervals=probe_intervals,
    )
    if complete and silver_note:
        raise ValueError(
            "catch-up apply requires readable silver coverage; "
            f"{silver_note}"
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
        cov = _coverage_key(start, end, ts)
        if cov is not None and cov in silver_keys:
            ok_intervals.append(row)
        else:
            catchup.append({**row, "detected_at": stamp})

    for run in bronze_runs:
        start = _norm_ts(run.get("interval_start"))
        end = _norm_ts(run.get("interval_end"))
        ts = _norm_ts(run.get("extract_run_datetime"))
        if not start or not end or not ts:
            continue
        key = (start, end, ts)
        if key in latest_keys:
            continue
        cov = _coverage_key(start, end, ts)
        if cov is not None and cov not in silver_keys:
            stale_siblings.append(
                {
                    "pipeline": config.name,
                    "interval_start": start,
                    "interval_end": end,
                    "extract_run_datetime": ts,
                }
            )

    schema, table = silver_relation(config)
    # Discovery incomplete when the safety/discovery cap was hit (not display slice).
    truncated = len(bronze_runs) >= discovery_cap
    if complete:
        catchup_out = catchup
        ok_out = ok_intervals
        stale_out = stale_siblings
        display_truncated = False
    else:
        catchup_out = catchup[:display_limit]
        ok_out = ok_intervals[:display_limit]
        stale_out = stale_siblings[:display_limit]
        display_truncated = (
            len(catchup) > display_limit
            or len(ok_intervals) > display_limit
            or len(stale_siblings) > display_limit
        )
    out: dict[str, Any] = {
        "pipeline": config.name,
        "materialized": config.dbt.silver.materialized,
        "silver_schema": schema,
        "silver_table": table,
        "limit": display_limit if not complete else len(bronze_runs),
        "discovery_cap": discovery_cap,
        "complete": complete,
        "candidate_mode": candidate_mode,
        "catchup_runs": catchup_out,
        "ok_intervals": ok_out,
        "stale_siblings_ignored": stale_out,
        "catchup_count": len(catchup),
        "ok_count": len(ok_intervals),
        "stale_siblings_count": len(stale_siblings),
        "truncated": truncated,
        "display_truncated": display_truncated,
    }
    if lookback_raw:
        out["extract_lookback"] = lookback_raw
    if interval_start is not None:
        out["interval_start"] = interval_start
        if interval_end is not None:
            out["interval_end"] = interval_end
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
    extract_lookback: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    detected_at: str | None = None,
    complete: bool = False,
) -> dict[str, Any]:
    """Run :func:`diff_bronze_silver` for many pipelines; aggregate catch-up rows."""
    root = project_root.resolve()
    ids = list(pipelines) if pipelines is not None else list_pipeline_ids(root)
    stamp = detected_at or datetime.now(UTC).isoformat()
    per_pipeline: list[dict[str, Any]] = []
    catchup_all: list[dict[str, Any]] = []
    catchup_count = 0
    truncated = False
    display_truncated = False
    for pipe_id in ids:
        # Resolve via package so monkeypatch on det.runtime.silver_catchup.diff_bronze_silver works.
        from det.runtime import silver_catchup as _sc

        one = _sc.diff_bronze_silver(
            pipe_id,
            project_root=root,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
            limit=limit,
            analytics_db=analytics_db,
            detected_at=stamp,
            complete=complete,
        )
        per_pipeline.append(one)
        # Flatten displayed rows; count uses each pipeline's retained total.
        catchup_all.extend(one.get("catchup_runs") or [])
        catchup_count += int(one.get("catchup_count") or 0)
        truncated = truncated or bool(one.get("truncated"))
        display_truncated = display_truncated or bool(one.get("display_truncated"))
    out: dict[str, Any] = {
        "pipelines": ids,
        "pipeline_count": len(ids),
        "catchup_runs": catchup_all,
        "catchup_count": catchup_count,
        "results": per_pipeline,
        "detected_at": stamp,
        "complete": complete,
        "truncated": truncated,
        "display_truncated": display_truncated,
        "candidate_mode": (
            "extract_lookback"
            if extract_lookback and str(extract_lookback).strip()
            else ("interval" if interval_start is not None else "full")
        ),
    }
    if extract_lookback and str(extract_lookback).strip():
        out["extract_lookback"] = str(extract_lookback).strip()
    return out

