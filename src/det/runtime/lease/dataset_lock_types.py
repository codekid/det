"""Bronze dataset lock types, constants, and store protocol."""

from __future__ import annotations

import os
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from det.runtime.ids import fs_dataset_parts, validate_canonical_id
from det.runtime.lake import LakeRef

DEFAULT_DATASET_LOCK_PG_TABLE = "dataset_locks"
DEFAULT_DATASET_LOCK_SHARED_PG_TABLE = "dataset_lock_shared"
DEFAULT_DATASET_LOCK_WAIT_SEC = 3600
DEFAULT_CAS_RETRIES = 50

DatasetLockMode = Literal["shared", "exclusive"]


@dataclass
class DatasetLockHandle:
    """Runtime handle for a shared or exclusive bronze-dataset lock."""

    token: str
    dataset_id: str
    mode: DatasetLockMode
    ttl_sec: int
    command: str
    owner: str
    path: LakeRef | None = None
    version: str | None = None
    store: Any | None = field(default=None, repr=False, compare=False)


_DATASET_HELD: ContextVar[str | None] = ContextVar("det_dataset_lock_held", default=None)
_DATASET_ACTIVE: ContextVar[DatasetLockHandle | None] = ContextVar(
    "det_dataset_lock_active", default=None
)


def dataset_lock_path(lake: LakeRef, dataset_id: str) -> LakeRef:
    cid = validate_canonical_id(dataset_id)
    out = lake / "locks" / "datasets"
    for part in fs_dataset_parts(cid):
        out = out / part
    return out / "_lock.json"


def resolve_dataset_lock_wait_sec(
    env: Mapping[str, str] | None = None,
) -> int | None:
    environ = os.environ if env is None else env
    raw = (environ.get("DET_DATASET_LOCK_WAIT_SEC") or "").strip()
    if not raw:
        return DEFAULT_DATASET_LOCK_WAIT_SEC
    value = int(raw)
    if value < 0:
        raise ValueError("DET_DATASET_LOCK_WAIT_SEC must be >= 0")
    return None if value == 0 else value


class DatasetLockStore(Protocol):
    """Store protocol for bronze dataset RW locks."""

    def acquire_shared(
        self,
        *,
        dataset_id: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> DatasetLockHandle: ...

    def acquire_exclusive(
        self,
        *,
        dataset_id: str,
        command: str,
        ttl_sec: int,
        owner: str,
        wait: bool = True,
        wait_sec: int | None = None,
        poll_interval: float = 0.2,
    ) -> DatasetLockHandle: ...

    def refresh(self, handle: DatasetLockHandle) -> None: ...

    def ensure_held(self, handle: DatasetLockHandle) -> None: ...

    def release(self, handle: DatasetLockHandle) -> None: ...

    def inspect(self, *, dataset_id: str) -> dict[str, Any] | None: ...

    def force_release(self, *, dataset_id: str) -> dict[str, Any] | None: ...
