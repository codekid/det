from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

import pendulum
from pendulum import DateTime


def identity_iso(value: object) -> str:
    """Normalize a SQL or Python datetime/string to DET ISO-8601 UTC identity."""
    if isinstance(value, DateTime):
        return value.in_timezone("UTC").isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return pendulum.instance(value, tz="UTC").isoformat()
        return pendulum.instance(value).in_timezone("UTC").isoformat()
    text = str(value)
    if len(text) >= 15 and text[8:9] == "T" and "-" not in text[:8]:
        return from_partition_value(text)
    return to_interval_datetime(text)


def to_interval_datetime(value: datetime | DateTime | str) -> str:
    """
    Normalize an interval bound to ISO 8601 in UTC, e.g. 2026-08-06T00:00:00+00:00.

    A bare date is read as midnight UTC.
    """
    if isinstance(value, str):
        parsed = pendulum.parse(value)
    elif isinstance(value, DateTime):
        parsed = value
    else:
        parsed = pendulum.instance(value)
    if not isinstance(parsed, DateTime):
        raise ValueError(f"Not an interval datetime: {value!r}")
    return parsed.in_timezone("UTC").isoformat()


def resolve_interval(start: str, end: str | None = None) -> tuple[str, str]:
    """
    Normalize both interval bounds to ISO 8601 UTC. The end is exclusive and
    defaults to start + 1 day, so it is never null on a landed row.
    """
    start_iso = to_interval_datetime(start)
    if end is None:
        end_dt = pendulum.parse(start_iso)
        if not isinstance(end_dt, DateTime):
            raise TypeError(f"expected DateTime from pendulum.parse, got {type(end_dt)}")
        end_iso = end_dt.add(days=1).isoformat()
    else:
        end_iso = to_interval_datetime(end)
    if end_iso <= start_iso:
        raise ValueError(f"interval end {end_iso} must be after start {start_iso}")
    return start_iso, end_iso


_LEGACY_EXTRACT_RUN_FORMAT = "YYYY/MM/DD HH:mm:ss.S"


def to_partition_value(value: str) -> str:
    """
    Compact a datetime meta value into a path-safe hive partition value.

    A directory name cannot carry the '/' and ':' that ISO datetimes use, so the
    partition holds the basic ISO 8601 form of the same UTC instant:
    2026-08-01T00:00:00+00:00 -> 20260801T000000Z. Legacy
    `yyyy/mm/dd HH:mm:ss.S` extract-run values are still accepted and treated as
    naive local wall-clock for path encoding only.
    """
    if "/" in value:
        parsed = pendulum.from_format(value, _LEGACY_EXTRACT_RUN_FORMAT)
        return parsed.format("YYYYMMDDTHHmmss")
    parsed = pendulum.parse(value)
    if not isinstance(parsed, DateTime):
        raise ValueError(f"Not a partition datetime: {value!r}")
    return parsed.in_timezone("UTC").format("YYYYMMDDTHHmmss") + "Z"


def from_partition_value(value: str) -> str:
    """
    Expand a compact hive partition value back to ISO 8601 UTC.

    Second precision only (matches to_partition_value). Legacy extract-run
    folders without a trailing Z are treated as UTC for path decoding.
    """
    compact = value.strip()
    if compact.endswith("Z"):
        parsed = pendulum.from_format(compact[:-1], "YYYYMMDDTHHmmss", tz="UTC")
    else:
        parsed = pendulum.from_format(compact, "YYYYMMDDTHHmmss", tz="UTC")
    return parsed.isoformat()


def format_extract_run_datetime(when: datetime | DateTime | None = None) -> str:
    """
    Run-start timestamp as ISO 8601 UTC (same shape as __interval_*_datetime).

    Captured once per `det run` / extract and reused for every row and the hive
    path. Migrate stamps bronze ``__extract_run_datetime`` from the raw
    manifest instead; this helper still sets ``bronze_loaded_at`` and log context.
    """
    if when is None:
        return pendulum.now("UTC").isoformat()
    if isinstance(when, DateTime):
        return when.in_timezone("UTC").isoformat()
    return pendulum.instance(when).in_timezone("UTC").isoformat()


def data_interval_date(interval_start: datetime | DateTime | str) -> str:
    if isinstance(interval_start, str):
        return interval_start[:10]
    if isinstance(interval_start, DateTime):
        return interval_start.in_timezone("UTC").to_date_string()
    return interval_start.date().isoformat()


def row_hash(canonical: dict[str, Any]) -> str:
    """Stable hash of canonical (non-meta) fields."""
    payload = json.dumps(canonical, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def attach_meta(
    canonical: dict[str, Any],
    *,
    filename: str | None,
    extract_run_datetime: str,
    interval_start_datetime: str,
    interval_end_datetime: str,
    bronze_loaded_at: str,
) -> dict[str, Any]:
    """Attach runtime __* meta columns after schema validation (no __raw)."""
    out = dict(canonical)
    start_iso, end_iso = resolve_interval(interval_start_datetime, interval_end_datetime)
    out["__row_hash"] = row_hash(canonical)
    out["__filename"] = filename
    out["__extract_run_datetime"] = extract_run_datetime
    out["__bronze_loaded_at"] = bronze_loaded_at
    out["__interval_start_datetime"] = start_iso
    out["__interval_end_datetime"] = end_iso
    out["__data_interval_date"] = data_interval_date(start_iso)
    return out
