"""Derived heartbeat triage fields (advisory only)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

DEFAULT_HEARTBEAT_INTERVAL_SEC = 30
HEARTBEAT_STALE_FACTOR = 3

HeartbeatStatus = Literal["fresh", "stale", "unknown"]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _parse_iso(value: str) -> datetime:
    """Parse ISO stamps; offset-less values are treated as UTC (never naive)."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def enrich_heartbeat_fields(
    record: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Add ``heartbeat_age_sec`` / ``heartbeat_status`` (does not mutate authority)."""
    out = dict(record)
    stamp = out.get("heartbeat_at")
    if not stamp:
        out["heartbeat_age_sec"] = None
        out["heartbeat_status"] = "unknown"
        return out
    try:
        beat = _parse_iso(str(stamp))
        clock = now or _utcnow()
        if clock.tzinfo is None:
            clock = clock.replace(tzinfo=UTC)
        else:
            clock = clock.astimezone(UTC)
        age = max(0, int((clock - beat).total_seconds()))
    except (TypeError, ValueError, OverflowError):
        out["heartbeat_age_sec"] = None
        out["heartbeat_status"] = "unknown"
        return out
    out["heartbeat_age_sec"] = age
    try:
        interval = int(out.get("heartbeat_interval_sec") or DEFAULT_HEARTBEAT_INTERVAL_SEC)
    except (TypeError, ValueError):
        interval = DEFAULT_HEARTBEAT_INTERVAL_SEC
    if interval < 1:
        interval = DEFAULT_HEARTBEAT_INTERVAL_SEC
    if age > interval * HEARTBEAT_STALE_FACTOR:
        out["heartbeat_status"] = "stale"
    else:
        out["heartbeat_status"] = "fresh"
    return out
