"""Lake lease on (pipeline, resolved interval). Equality only — not overlap."""

from __future__ import annotations

import json
import os
import secrets
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from det.logging import get_logger
from det.runtime.lake import LakeRef
from det.runtime.meta import to_partition_value

logger = get_logger(__name__)

DEFAULT_LOCK_TTL_SEC = 7200
_HELD: ContextVar[str | None] = ContextVar("det_lease_held", default=None)


class LeaseHeldError(RuntimeError):
    """Another writer holds a live lake lease for this pipeline+interval."""

    def __init__(self, message: str, *, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload or {}


@dataclass(frozen=True)
class Lease:
    path: LakeRef
    token: str
    pipeline: str
    interval_start: str
    interval_end: str
    ttl_sec: int
    lock_id: str


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


def read_lock(path: LakeRef) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _expires_at_iso(ttl_sec: int) -> str:
    return (datetime.now(UTC) + timedelta(seconds=ttl_sec)).isoformat()


def _is_expired(payload: dict[str, Any], *, now: datetime | None = None) -> bool:
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


def _payload(
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
        "expires_at": _expires_at_iso(ttl_sec),
    }


def _held_message(path: LakeRef, payload: dict[str, Any]) -> str:
    owner = payload.get("owner") or "unknown"
    expires = payload.get("expires_at") or "unknown"
    pipeline = payload.get("pipeline") or ""
    start = payload.get("interval_start") or ""
    return (
        f"lake lease held by {owner} until {expires} at {path}; "
        f"after the worker is dead: det lock-release -p {pipeline} -s {start} --force"
    )


def acquire_lease(
    lake: LakeRef,
    *,
    pipeline: str,
    interval_start: str,
    interval_end: str,
    command: str,
    ttl_sec: int | None = None,
    owner: str | None = None,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
) -> Lease | None:
    """Create the lock object. Returns None when locks are disabled.

    Nested same id is a no-op Lease.
    """
    if enabled is None:
        enabled = locks_enabled(env)
    if not enabled:
        return None
    ttl = resolve_lock_ttl_sec(ttl_sec, env=env)
    who = owner or default_lock_owner(env)
    path = lock_path(lake, pipeline, interval_start, interval_end)
    ident = lock_id(pipeline, interval_start, interval_end)
    nested = _HELD.get()
    if nested == ident:
        existing = read_lock(path) or {}
        return Lease(
            path=path,
            token=str(existing.get("token") or ""),
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            ttl_sec=ttl,
            lock_id=ident,
        )

    token = secrets.token_hex(16)
    body = _payload(
        pipeline=pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        command=command,
        token=token,
        owner=who,
        ttl_sec=ttl,
    )
    raw = json.dumps(body, indent=2, sort_keys=True).encode("utf-8")
    try:
        path.create_exclusive(raw)
    except FileExistsError:
        held = read_lock(path)
        if held is None:
            raise LeaseHeldError(
                f"lake lease exists but is unreadable at {path}",
                payload={},
            ) from None
        if not _is_expired(held):
            raise LeaseHeldError(_held_message(path, held), payload=held) from None
        if str(held.get("token") or "") != str((read_lock(path) or {}).get("token") or ""):
            raise LeaseHeldError(_held_message(path, held), payload=held) from None
        # Steal expired lease: rewrite in place (owner crashed).
        path.write_bytes(raw)
        logger.info(
            "stole expired lake lease",
            path=str(path),
            previous_owner=held.get("owner"),
        )
    logger.info(
        "acquired lake lease",
        path=str(path),
        ttl_sec=ttl,
        owner=who,
        command=command,
    )
    return Lease(
        path=path,
        token=token,
        pipeline=pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        ttl_sec=ttl,
        lock_id=ident,
    )


def refresh_lease(lease: Lease | None) -> None:
    if lease is None or not lease.token:
        return
    held = read_lock(lease.path)
    if held is None or str(held.get("token") or "") != lease.token:
        return
    held["expires_at"] = _expires_at_iso(lease.ttl_sec)
    held["ttl_sec"] = lease.ttl_sec
    lease.path.write_bytes(json.dumps(held, indent=2, sort_keys=True).encode("utf-8"))


def release_lease(lease: Lease | None) -> None:
    if lease is None or not lease.token:
        return
    held = read_lock(lease.path)
    if held is None:
        return
    if str(held.get("token") or "") != lease.token:
        return
    lease.path.unlink(missing_ok=True)
    logger.info("released lake lease", path=str(lease.path))


def force_release_lock(path: LakeRef) -> dict[str, Any] | None:
    """Delete a lock object regardless of TTL. Operator-only; worker must be dead."""
    payload = read_lock(path)
    if payload is None:
        return None
    path.unlink(missing_ok=True)
    logger.info(
        "force-released lake lease",
        path=str(path),
        owner=payload.get("owner"),
        expires_at=payload.get("expires_at"),
    )
    return payload


# Public library aliases (det.__all__); same callables as read/force-release.
inspect_lease = read_lock
release_lock = force_release_lock


@contextmanager
def pipeline_lease(
    lake: LakeRef,
    *,
    pipeline: str,
    interval_start: str,
    interval_end: str,
    command: str,
    ttl_sec: int | None = None,
    owner: str | None = None,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
) -> Iterator[Lease | None]:
    lease = acquire_lease(
        lake,
        pipeline=pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        command=command,
        ttl_sec=ttl_sec,
        owner=owner,
        env=env,
        enabled=enabled,
    )
    ident = lock_id(pipeline, interval_start, interval_end)
    nested = _HELD.get() == ident
    token = None
    if lease is not None and not nested:
        token = _HELD.set(ident)
    try:
        if lease is not None:
            refresh_lease(lease)
        yield lease
    finally:
        if lease is not None and not nested:
            release_lease(lease)
            if token is not None:
                _HELD.reset(token)
