"""Bronze/raw partition listing, diff, and run resolution helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.destinations.models import bronze_dataset_dir, raw_dataset_dir
from det.runtime.bronze_runs import list_bronze_runs
from det.runtime.config import PipelineConfig
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import LakeRef
from det.runtime.limits import DEFAULT_LIST_LIMIT, clamp_list_limit
from det.runtime.manifest import is_committed_raw_dir
from det.runtime.meta import (
    from_partition_value,
    to_interval_datetime,
)

from ._common import (
    _assert_under_raw,
    _load_pipeline,
    _parse_hive_key,
    _rel,
    _resolve_lake_run_path,
    _root,
    _run_dict,
    _run_key,
    walk_hive_runs,
)


def to_interval_or_partition(value: str) -> str:
    """Normalize a caller interval/run value to ISO UTC."""
    compact = value.strip()
    # Hive compact form: 20260801T000000Z
    if len(compact) == 16 and compact.endswith("Z") and ":" not in compact:
        try:
            return from_partition_value(compact)
        except Exception:  # noqa: S110  # fall through to to_interval_datetime
            pass
    return to_interval_datetime(compact)


def _raw_run_dir(run: dict[str, Any], root: Path) -> Path | LakeRef:
    path = run.get("path")
    if not path:
        raise FileNotFoundError("resolved run has no path")
    return _resolve_lake_run_path(str(path), root=root)


def diff_partitions(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_list_limit(limit)

    raw_dir = raw_dataset_dir(config, base)
    # Oversample slightly so set math is meaningful when both sides are large.
    walk_cap = capped
    raw_runs = walk_hive_runs(
        raw_dir,
        root=base,
        limit=walk_cap,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
        require_committed=True,
    )
    bronze_runs, bronze_note = list_bronze_runs(
        config,
        root=base,
        limit=walk_cap,
        interval_start=interval_start,
        interval_end=interval_end,
    )

    raw_by_key = {_run_key(r): r for r in raw_runs}
    bronze_by_key = {_run_key(r): r for r in bronze_runs}
    raw_keys = set(raw_by_key)
    bronze_keys = set(bronze_by_key)

    only_raw_keys = sorted(raw_keys - bronze_keys)
    only_bronze_keys = sorted(bronze_keys - raw_keys)
    both_keys = raw_keys & bronze_keys

    only_raw = [raw_by_key[k] for k in only_raw_keys[:capped]]
    only_bronze = [bronze_by_key[k] for k in only_bronze_keys[:capped]]

    out: dict[str, Any] = {
        "pipeline": config.name,
        "destination_type": config.destination.type,
        "limit": capped,
        "only_raw": only_raw,
        "only_bronze": only_bronze,
        "both_count": len(both_keys),
        "only_raw_count": len(only_raw_keys),
        "only_bronze_count": len(only_bronze_keys),
        "truncated": (
            len(only_raw_keys) > capped
            or len(only_bronze_keys) > capped
            or len(raw_runs) >= walk_cap
            or len(bronze_runs) >= walk_cap
        ),
        "raw_dataset_dir": _rel(raw_dir, base),
    }
    if config.destination.type == "filesystem":
        out["bronze_dataset_dir"] = _rel(bronze_dataset_dir(config, base), base)
    else:
        sql_schema, sql_table = sql_names_for_config(config)
        out["bronze_schema"] = sql_schema
        out["bronze_table"] = sql_table
    if bronze_note:
        out["note"] = bronze_note
    return out


def resolve_raw_run(
    config: PipelineConfig,
    *,
    root: Path,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
) -> dict[str, Any]:
    """Resolve a raw extract-run directory; prefer run_path, else latest match."""
    if run_path is not None:
        run_dir = _resolve_lake_run_path(run_path, root=root)
        if not run_dir.is_dir():
            raise FileNotFoundError(f"run path is not a directory: {run_dir}")
        _assert_under_raw(run_dir, root=root)
        if not is_committed_raw_dir(run_dir):
            raise FileNotFoundError(
                f"run path is not a committed extract (no meta/manifest.json): "
                f"{_rel(run_dir, root)}"
            )
        # Derive keys from hive path when possible.
        start = end = run = None
        try:
            run_compact = _parse_hive_key(run_dir.name, "__extract_run_datetime=")
            end_dir = run_dir.parent
            start_dir = end_dir.parent
            end_compact = _parse_hive_key(end_dir.name, "__interval_end_datetime=")
            start_compact = _parse_hive_key(start_dir.name, "__interval_start_datetime=")
            if start_compact and end_compact and run_compact:
                start = from_partition_value(start_compact)
                end = from_partition_value(end_compact)
                run = from_partition_value(run_compact)
        except Exception:  # noqa: S110  # skip unparseable hive keys
            pass
        return _run_dict(
            interval_start=start or "",
            interval_end=end or "",
            extract_run_datetime=run or "",
            path=_rel(run_dir, root),
        )

    runs = walk_hive_runs(
        raw_dataset_dir(config, root),
        root=root,
        limit=DEFAULT_LIST_LIMIT,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
        require_committed=True,
    )
    if extract_run_datetime is not None:
        want = to_interval_or_partition(extract_run_datetime)
        runs = [r for r in runs if r["extract_run_datetime"] == want]
    if not runs:
        raise FileNotFoundError(
            f"no raw runs found for pipeline {config.name}"
            + (f" in window starting {interval_start}" if interval_start else "")
        )
    # Latest by extract_run_datetime (ISO sorts lexicographically for UTC).
    return max(runs, key=lambda r: r["extract_run_datetime"])
