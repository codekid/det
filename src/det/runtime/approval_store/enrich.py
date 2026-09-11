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
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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
    except (TypeError, ValueError):
        out["heartbeat_age_sec"] = None
        out["heartbeat_status"] = "unknown"
        return out
    clock = now or _utcnow()
    age = max(0, int((clock - beat).total_seconds()))
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
