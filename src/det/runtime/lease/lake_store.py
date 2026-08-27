"""Lake-native lease store with object-store CAS on s3/gs."""

from __future__ import annotations

import json
from typing import Any

from det.logging import get_logger
from det.runtime.lake import LakeRef, ObjectVersionConflict
from det.runtime.lease._common import (
    Lease,
    LeaseHeldError,
    expires_at_iso,
    held_message,
    is_expired,
    lease_payload,
    lock_id,
    lock_path,
    new_token,
)

logger = get_logger(__name__)


def read_lock(path: LakeRef) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


class LakeLeaseStore:
    def __init__(self, lake: LakeRef) -> None:
        self.lake = lake

    def acquire(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> Lease:
        path = lock_path(self.lake, pipeline, interval_start, interval_end)
        ident = lock_id(pipeline, interval_start, interval_end)
        token = new_token()
        body = lease_payload(
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            command=command,
            token=token,
            owner=owner,
            ttl_sec=ttl_sec,
        )
        raw = json.dumps(body, indent=2, sort_keys=True).encode("utf-8")
        try:
            version = path.create_exclusive(raw)
        except FileExistsError:
            version = self._steal_expired(path, raw, body)
        logger.info(
            "acquired lake lease",
            path=str(path),
            ttl_sec=ttl_sec,
            owner=owner,
            command=command,
        )
        return Lease(
            path=path,
            token=token,
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            ttl_sec=ttl_sec,
            lock_id=ident,
            version=version,
        )

    def _steal_expired(
        self, path: LakeRef, raw: bytes, body: dict[str, Any]
    ) -> str:
        held = read_lock(path)
        if held is None:
            raise LeaseHeldError(
                f"lake lease exists but is unreadable at {path}",
                payload={},
            )
        if not is_expired(held):
            raise LeaseHeldError(held_message(str(path), held), payload=dict(held))
        expected = path.object_version()
        if expected is None:
            raise LeaseHeldError(held_message(str(path), held), payload=dict(held))
        # Soft token re-check before CAS (defense in depth).
        again = read_lock(path) or {}
        if str(held.get("token") or "") != str(again.get("token") or ""):
            raise LeaseHeldError(held_message(str(path), held), payload=dict(held))
        try:
            version = path.replace_if_match(expected, raw)
        except ObjectVersionConflict as exc:
            raise LeaseHeldError(
                held_message(str(path), held), payload=dict(held)
            ) from exc
        logger.info(
            "stole expired lake lease",
            path=str(path),
            previous_owner=held.get("owner"),
        )
        del body  # payload already in raw
        return version

    def refresh(self, lease: Lease) -> None:
        if not lease.token or lease.path is None:
            return
        held = read_lock(lease.path)
        if held is None or str(held.get("token") or "") != lease.token:
            return
        if lease.version is None:
            # Nested / legacy handle without version: best-effort token write.
            held["expires_at"] = expires_at_iso(lease.ttl_sec)
            held["ttl_sec"] = lease.ttl_sec
            lease.path.write_bytes(
                json.dumps(held, indent=2, sort_keys=True).encode("utf-8")
            )
            lease.version = lease.path.object_version()
            return
        held["expires_at"] = expires_at_iso(lease.ttl_sec)
        held["ttl_sec"] = lease.ttl_sec
        raw = json.dumps(held, indent=2, sort_keys=True).encode("utf-8")
        try:
            lease.version = lease.path.replace_if_match(lease.version, raw)
        except ObjectVersionConflict:
            return

    def release(self, lease: Lease) -> None:
        if not lease.token or lease.path is None:
            return
        held = read_lock(lease.path)
        if held is None:
            return
        if str(held.get("token") or "") != lease.token:
            return
        if lease.version is None:
            lease.path.unlink(missing_ok=True)
            logger.info("released lake lease", path=str(lease.path))
            return
        if lease.path.delete_if_match(lease.version):
            logger.info("released lake lease", path=str(lease.path))
            return
        # Version may have advanced (refresh); re-check token then CAS on current.
        held = read_lock(lease.path)
        if held is None or str(held.get("token") or "") != lease.token:
            return
        current = lease.path.object_version()
        if current is not None and lease.path.delete_if_match(current):
            logger.info("released lake lease", path=str(lease.path))

    def inspect(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
    ) -> dict[str, Any] | None:
        path = lock_path(self.lake, pipeline, interval_start, interval_end)
        return read_lock(path)

    def force_release(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
    ) -> dict[str, Any] | None:
        path = lock_path(self.lake, pipeline, interval_start, interval_end)
        return force_release_lock(path)


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
