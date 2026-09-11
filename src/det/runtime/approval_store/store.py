"""ApprovalStore protocol."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol

from det.runtime.approval_store.enrich import DEFAULT_HEARTBEAT_INTERVAL_SEC


class ApprovalStore(Protocol):
    def create(self, record: dict[str, Any]) -> dict[str, Any]: ...

    def load(self, approval_id: str) -> dict[str, Any]: ...

    def list(
        self,
        *,
        statuses: Sequence[str] | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]: ...

    def claim(
        self,
        approval_id: str,
        *,
        claimed_by: str,
        now: datetime | None = None,
        heartbeat_interval_sec: int = DEFAULT_HEARTBEAT_INTERVAL_SEC,
    ) -> dict[str, Any]: ...

    def consume(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]: ...

    def release(
        self,
        approval_id: str,
        *,
        released_by: str,
        now: datetime | None = None,
    ) -> dict[str, Any]: ...

    def touch_heartbeat(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]: ...
