"""Compose primary store with legacy read fallback."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from det.runtime.approval_store.enrich import DEFAULT_HEARTBEAT_INTERVAL_SEC
from det.runtime.approval_store.legacy import LegacyApprovalReader
from det.runtime.approval_store.store import ApprovalStore


def _legacy_readonly_error(approval_id: str, *, action: str) -> Exception:
    from det.runtime.approval import ApprovalError

    return ApprovalError(
        "approval_legacy_readonly",
        f"approval {approval_id} exists only under legacy .det/approvals; "
        f"mint a new approval (lake/postgres store) instead of {action} the old file",
    )


class CompositeApprovalStore:
    def __init__(self, *, primary: ApprovalStore, legacy: LegacyApprovalReader) -> None:
        self._primary = primary
        self._legacy = legacy

    def create(self, record: dict[str, Any]) -> dict[str, Any]:
        return self._primary.create(record)

    def load(self, approval_id: str) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        try:
            return self._primary.load(approval_id)
        except ApprovalError as exc:
            if exc.code != "approval_not_found":
                raise
        legacy = self._legacy.load(approval_id)
        if legacy is None:
            raise ApprovalError("approval_not_found", f"no approval file for {approval_id}")
        return legacy

    def ensure_writable(self, approval_id: str) -> None:
        """Reject legacy-only ids before check/claim/consume mutate paths.

        ``load`` still serves legacy for inspect (list/show); writers must mint
        into the primary lake/postgres store.
        """
        from det.runtime.approval import ApprovalError

        try:
            self._primary.load(approval_id)
        except ApprovalError as exc:
            if exc.code != "approval_not_found":
                raise
            if self._legacy.load(approval_id) is not None:
                raise _legacy_readonly_error(approval_id, action="using") from exc
            raise

    def list(
        self,
        *,
        statuses: Sequence[str] | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        primary = self._primary.list(statuses=statuses, now=now)
        by_id = {str(r.get("id")): r for r in primary}
        for rec in self._legacy.list(statuses=statuses, now=now):
            rid = str(rec.get("id"))
            if rid not in by_id:
                by_id[rid] = rec
        return [by_id[k] for k in sorted(by_id)]

    def claim(
        self,
        approval_id: str,
        *,
        claimed_by: str,
        now: datetime | None = None,
        heartbeat_interval_sec: int = DEFAULT_HEARTBEAT_INTERVAL_SEC,
    ) -> dict[str, Any]:
        # Claims only against the primary store (lake/postgres). Legacy records
        # must be re-approved into the new store.
        from det.runtime.approval import ApprovalError

        try:
            return self._primary.claim(
                approval_id,
                claimed_by=claimed_by,
                now=now,
                heartbeat_interval_sec=heartbeat_interval_sec,
            )
        except ApprovalError as exc:
            self._raise_if_legacy_only(approval_id, action="claiming", cause=exc)
            raise

    def consume(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        try:
            return self._primary.consume(approval_id, now=now)
        except ApprovalError as exc:
            if exc.code != "approval_not_found":
                raise
            if self._legacy.load(approval_id) is not None:
                raise _legacy_readonly_error(approval_id, action="consuming") from exc
            raise

    def release(
        self,
        approval_id: str,
        *,
        released_by: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        try:
            return self._primary.release(approval_id, released_by=released_by, now=now)
        except ApprovalError as exc:
            self._raise_if_legacy_only(approval_id, action="releasing", cause=exc)
            raise

    def touch_heartbeat(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        try:
            return self._primary.touch_heartbeat(approval_id, now=now)
        except ApprovalError as exc:
            self._raise_if_legacy_only(approval_id, action="heartbeating", cause=exc)
            raise

    def _raise_if_legacy_only(
        self,
        approval_id: str,
        *,
        action: str,
        cause: Exception,
    ) -> None:
        """If id exists only under legacy .det, raise approval_legacy_readonly."""
        from det.runtime.approval import ApprovalError

        if self._legacy.load(approval_id) is None:
            return
        try:
            self._primary.load(approval_id)
        except ApprovalError as load_exc:
            if load_exc.code == "approval_not_found":
                raise _legacy_readonly_error(approval_id, action=action) from cause
