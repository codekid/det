"""Shared lease types, TTL helpers, and payload helpers."""

from __future__ import annotations

import os
import secrets
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from det.errors import DetConflictError
from det.runtime.lake import LakeRef
from det.runtime.meta import to_partition_value

DEFAULT_LOCK_TTL_SEC = 7200
DEFAULT_LOCK_BACKEND = "lake"
DEFAULT_LOCK_MODE = "exact"
DEFAULT_LOCK_PG_DSN_ENV = "DET_LOCK_PG_DSN"
DEFAULT_LOCK_PG_SCHEMA = "det_lease"
DEFAULT_LOCK_PG_TABLE = "leases"

_HELD: ContextVar[str | None] = ContextVar("det_lease_held", default=None)


class LeaseHeldError(DetConflictError):
    """Another writer holds a live lease for this pipeline+interval."""

    def __init__(self, message: str, *, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload or {}


@dataclass
class Lease:
    """Runtime lease handle. ``version`` is mutated on lake CAS refresh."""

    token: str
    pipeline: str
    interval_start: str
    interval_end: str
    ttl_sec: int
    lock_id: str
    path: LakeRef | None = None
    version: str | None = None


_ACTIVE: ContextVar[Lease | None] = ContextVar("det_lease_active", default=None)


def locks_enabled(env: Mapping[str, str] | None = None) -> bool:
    environ = os.environ if env is None else env
    raw = (environ.get("DET_LOCK") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def resolve_lock_ttl_sec(
    explicit: int | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> int:
    if explicit is not None:
        if explicit < 1:
            raise ValueError("lock TTL must be >= 1 second")
        return explicit
    environ = os.environ if env is None else env
    raw = environ.get("DET_LOCK_TTL_SEC")
    if raw is not None and str(raw).strip():
        value = int(str(raw).strip())
        if value < 1:
            raise ValueError("DET_LOCK_TTL_SEC must be >= 1")
        return value
    return DEFAULT_LOCK_TTL_SEC


def default_lock_owner(env: Mapping[str, str] | None = None) -> str:
    environ = os.environ if env is None else env
    named = (environ.get("DET_LOCK_OWNER") or "").strip()
    if named:
        return named
    return f"pid:{os.getpid()}"


def lock_id(pipeline: str, interval_start: str, interval_end: str) -> str:
    return (
        f"{pipeline}/"
        f"{to_partition_value(interval_start)}_{to_partition_value(interval_end)}"
    )


def lock_path(
    lake: LakeRef,
    pipeline: str,
    interval_start: str,
    interval_end: str,
) -> LakeRef:
    start_k = to_partition_value(interval_start)
    end_k = to_partition_value(interval_end)
    return lake / "locks" / pipeline / f"{start_k}_{end_k}.json"


def advisory_lock_keys(pipeline: str, interval_start: str, interval_end: str) -> tuple[int, int]:
    """Two int4 keys for pg_advisory_lock (same identity as the lake path)."""
    import hashlib

    def i32(text: str) -> int:
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF

    return i32(pipeline), i32(f"{interval_start}/{interval_end}")


def new_token() -> str:
    return secrets.token_hex(16)


def expires_at_iso(ttl_sec: int) -> str:
    return (datetime.now(UTC) + timedelta(seconds=ttl_sec)).isoformat()


def is_expired(payload: Mapping[str, Any], *, now: datetime | None = None) -> bool:
    raw = str(payload.get("expires_at") or "")
    if not raw:
        return True
    try:
        expires = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return True
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    clock = now or datetime.now(UTC)
    return expires <= clock


def lease_payload(
    *,
    pipeline: str,
    interval_start: str,
    interval_end: str,
    command: str,
    token: str,
    owner: str,
    ttl_sec: int,
) -> dict[str, Any]:
    return {
        "pipeline": pipeline,
        "interval_start": interval_start,
        "interval_end": interval_end,
        "command": command,
        "token": token,
        "owner": owner,
        "ttl_sec": ttl_sec,
        "expires_at": expires_at_iso(ttl_sec),
    }


def held_message(location: str, payload: Mapping[str, Any]) -> str:
    owner = payload.get("owner") or "unknown"
    expires = payload.get("expires_at") or "unknown"
    pipeline = payload.get("pipeline") or ""
    start = payload.get("interval_start") or ""
    return (
        f"lease held by {owner} until {expires} at {location}; "
        f"after the worker is dead: det lock-release -p {pipeline} -s {start} --force"
    )
