"""Bronze-dataset reader/writer locks (shared publish / exclusive recreate)."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from det.logging import get_logger
from det.runtime.ids import fs_dataset_parts, validate_canonical_id
from det.runtime.lake import LakeRef, ObjectVersionConflict
from det.runtime.lease._common import (
    LeaseFencedError,
    LeaseHeldError,
    default_lock_owner,
    expires_at_iso,
    is_expired,
    locks_enabled,
    new_token,
    resolve_lock_ttl_sec,
)
from det.runtime.lease.resolve import resolve_lease_options
from det.runtime.lease.store import ResolvedLeaseOptions

logger = get_logger(__name__)

DEFAULT_DATASET_LOCK_PG_TABLE = "dataset_locks"
DEFAULT_DATASET_LOCK_SHARED_PG_TABLE = "dataset_lock_shared"
DEFAULT_DATASET_LOCK_WAIT_SEC = 3600
DEFAULT_CAS_RETRIES = 50

DatasetLockMode = Literal["shared", "exclusive"]

_DATASET_HELD: ContextVar[str | None] = ContextVar("det_dataset_lock_held", default=None)
_DATASET_ACTIVE: ContextVar[DatasetLockHandle | None] = ContextVar(
    "det_dataset_lock_active", default=None
)


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


def _empty_body(dataset_id: str) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "exclusive": None,
        "exclusive_intent": None,
        "shared": [],
    }


def _holder(*, token: str, owner: str, command: str, ttl_sec: int) -> dict[str, Any]:
    return {
        "token": token,
        "owner": owner,
        "command": command,
        "ttl_sec": ttl_sec,
        "expires_at": expires_at_iso(ttl_sec),
    }


def _prune_body(body: dict[str, Any]) -> None:
    ex = body.get("exclusive")
    if isinstance(ex, dict) and is_expired(ex):
        body["exclusive"] = None
    intent = body.get("exclusive_intent")
    if isinstance(intent, dict) and is_expired(intent):
        body["exclusive_intent"] = None
    shared = body.get("shared")
    if not isinstance(shared, list):
        body["shared"] = []
        return
    body["shared"] = [
        row
        for row in shared
        if isinstance(row, dict) and not is_expired(row)
    ]


def _live_shared(body: Mapping[str, Any]) -> list[dict[str, Any]]:
    shared = body.get("shared")
    if not isinstance(shared, list):
        return []
    return [row for row in shared if isinstance(row, dict) and not is_expired(row)]


def _live_exclusive(body: Mapping[str, Any]) -> dict[str, Any] | None:
    ex = body.get("exclusive")
    if not isinstance(ex, dict) or is_expired(ex):
        return None
    return ex


def _live_exclusive_intent(body: Mapping[str, Any]) -> dict[str, Any] | None:
    intent = body.get("exclusive_intent")
    if not isinstance(intent, dict) or is_expired(intent):
        return None
    return intent


def _serialize(body: dict[str, Any]) -> bytes:
    return json.dumps(body, indent=2, sort_keys=True).encode("utf-8")


def _dataset_held_message(location: str, body: Mapping[str, Any]) -> str:
    ex = _live_exclusive(body)
    if ex is not None:
        owner = ex.get("owner") or "unknown"
        expires = ex.get("expires_at") or "unknown"
        return (
            f"dataset exclusive lock held by {owner} until {expires} at {location}; "
            f"after the worker is dead: det lock-release --dataset-id … --force"
        )
    intent = _live_exclusive_intent(body)
    if intent is not None:
        owner = intent.get("owner") or "unknown"
        expires = intent.get("expires_at") or "unknown"
        return (
            f"dataset exclusive lock pending by {owner} until {expires} at {location}"
        )
    count = len(_live_shared(body))
    return f"dataset lock busy ({count} shared holder(s)) at {location}"


def read_dataset_lock(path: LakeRef) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _load_body_for_acquire(
    path: LakeRef, dataset_id: str, *, exists: bool
) -> dict[str, Any]:
    if not exists:
        return _empty_body(dataset_id)
    body = read_dataset_lock(path)
    if body is None:
        raise LeaseHeldError(
            f"dataset lock exists but is unreadable at {path}",
            payload={},
        )
    return body


class LakeDatasetLockStore:
    def __init__(self, lake: LakeRef) -> None:
        self.lake = lake

    def _path(self, dataset_id: str) -> LakeRef:
        return dataset_lock_path(self.lake, dataset_id)

    def acquire_shared(
        self,
        *,
        dataset_id: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> DatasetLockHandle:
        path = self._path(dataset_id)
        token = new_token()
        holder = _holder(token=token, owner=owner, command=command, ttl_sec=ttl_sec)
        for _ in range(DEFAULT_CAS_RETRIES):
            exists = path.exists()
            body = _load_body_for_acquire(path, dataset_id, exists=exists)
            _prune_body(body)
            if _live_exclusive(body) is not None or _live_exclusive_intent(body) is not None:
                raise LeaseHeldError(
                    _dataset_held_message(str(path), body),
                    payload=dict(body),
                )
            shared = _live_shared(body)
            if any(str(row.get("token") or "") == token for row in shared):
                pass
            else:
                shared = list(shared)
                shared.append(holder)
                body["shared"] = shared
            raw = _serialize(body)
            version = path.object_version() if exists else None
            try:
                if not exists:
                    new_version = path.create_exclusive(raw)
                else:
                    if version is None:
                        raise LeaseHeldError(
                            _dataset_held_message(str(path), body),
                            payload=dict(body),
                        )
                    new_version = path.replace_if_match(version, raw)
            except (FileExistsError, ObjectVersionConflict):
                continue
            logger.info(
                "acquired dataset shared lock",
                dataset_id=dataset_id,
                path=str(path),
                owner=owner,
                command=command,
            )
            return DatasetLockHandle(
                token=token,
                dataset_id=dataset_id,
                mode="shared",
                ttl_sec=ttl_sec,
                command=command,
                owner=owner,
                path=path,
                version=new_version,
                store=self,
            )
        raise LeaseHeldError(
            f"dataset shared lock CAS retries exhausted at {path}",
            payload={},
        )

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
    ) -> DatasetLockHandle:
        path = self._path(dataset_id)
        token = new_token()
        deadline = None if wait_sec is None else time.monotonic() + wait_sec
        while True:
            for _ in range(DEFAULT_CAS_RETRIES):
                exists = path.exists()
                body = _load_body_for_acquire(path, dataset_id, exists=exists)
                _prune_body(body)
                if _live_exclusive(body) is not None:
                    break
                if _live_shared(body):
                    if wait:
                        body["exclusive_intent"] = _holder(
                            token=token,
                            owner=owner,
                            command=command,
                            ttl_sec=ttl_sec,
                        )
                        raw = _serialize(body)
                        version = path.object_version() if exists else None
                        try:
                            if not exists:
                                path.create_exclusive(raw)
                            elif version is not None:
                                path.replace_if_match(version, raw)
                            else:
                                continue
                        except (FileExistsError, ObjectVersionConflict):
                            continue
                    break
                body["exclusive"] = _holder(
                    token=token, owner=owner, command=command, ttl_sec=ttl_sec
                )
                body["shared"] = []
                body["exclusive_intent"] = None
                raw = _serialize(body)
                version = path.object_version() if exists else None
                try:
                    if not exists:
                        new_version = path.create_exclusive(raw)
                    else:
                        if version is None:
                            raise LeaseHeldError(
                                _dataset_held_message(str(path), body),
                                payload=dict(body),
                            )
                        new_version = path.replace_if_match(version, raw)
                except (FileExistsError, ObjectVersionConflict):
                    continue
                logger.info(
                    "acquired dataset exclusive lock",
                    dataset_id=dataset_id,
                    path=str(path),
                    owner=owner,
                    command=command,
                )
                return DatasetLockHandle(
                    token=token,
                    dataset_id=dataset_id,
                    mode="exclusive",
                    ttl_sec=ttl_sec,
                    command=command,
                    owner=owner,
                    path=path,
                    version=new_version,
                    store=self,
                )
            if not wait:
                path = self._path(dataset_id)
                body = read_dataset_lock(path) or _empty_body(dataset_id)
                raise LeaseHeldError(
                    _dataset_held_message(str(path), body),
                    payload=dict(body),
                )
            if deadline is not None and time.monotonic() >= deadline:
                path = self._path(dataset_id)
                body = read_dataset_lock(path) or _empty_body(dataset_id)
                raise LeaseHeldError(
                    _dataset_held_message(str(path), body),
                    payload=dict(body),
                )
            time.sleep(poll_interval)

    def refresh(self, handle: DatasetLockHandle) -> None:
        if not handle.token or handle.path is None:
            return
        path = handle.path
        for _ in range(DEFAULT_CAS_RETRIES):
            body = read_dataset_lock(path)
            if body is None:
                return
            _prune_body(body)
            if handle.mode == "exclusive":
                ex = _live_exclusive(body)
                if ex is None or str(ex.get("token") or "") != handle.token:
                    return
                ex["expires_at"] = expires_at_iso(handle.ttl_sec)
                ex["ttl_sec"] = handle.ttl_sec
                body["exclusive"] = ex
            else:
                shared = _live_shared(body)
                updated = False
                for row in shared:
                    if str(row.get("token") or "") == handle.token:
                        row["expires_at"] = expires_at_iso(handle.ttl_sec)
                        row["ttl_sec"] = handle.ttl_sec
                        updated = True
                if not updated:
                    return
                body["shared"] = shared
            raw = _serialize(body)
            if handle.version is None:
                return
            try:
                handle.version = path.replace_if_match(handle.version, raw)
            except ObjectVersionConflict:
                handle.version = path.object_version()
                continue
            return

    def ensure_held(self, handle: DatasetLockHandle) -> None:
        if not handle.token or handle.path is None:
            raise LeaseFencedError(
                "dataset lock fence requires a token and lock path",
                payload={},
            )
        path = handle.path
        for _ in range(DEFAULT_CAS_RETRIES):
            body = read_dataset_lock(path)
            if body is None:
                raise LeaseFencedError(
                    f"dataset lock missing at {path} (lost or released)",
                    payload={},
                )
            _prune_body(body)
            if handle.mode == "exclusive":
                ex = _live_exclusive(body)
                if ex is None or str(ex.get("token") or "") != handle.token:
                    raise LeaseFencedError(
                        _dataset_held_message(str(path), body),
                        payload=dict(body),
                    )
                ex["expires_at"] = expires_at_iso(handle.ttl_sec)
                ex["ttl_sec"] = handle.ttl_sec
                body["exclusive"] = ex
            else:
                shared = _live_shared(body)
                match = next(
                    (
                        row
                        for row in shared
                        if str(row.get("token") or "") == handle.token
                    ),
                    None,
                )
                if match is None:
                    raise LeaseFencedError(
                        _dataset_held_message(str(path), body),
                        payload=dict(body),
                    )
                match["expires_at"] = expires_at_iso(handle.ttl_sec)
                match["ttl_sec"] = handle.ttl_sec
                body["shared"] = shared
            if handle.version is None:
                raise LeaseFencedError(
                    f"dataset lock fence requires a CAS version at {path}",
                    payload=dict(body),
                )
            raw = _serialize(body)
            try:
                handle.version = path.replace_if_match(handle.version, raw)
            except ObjectVersionConflict:
                handle.version = path.object_version()
                continue
            return
        body = read_dataset_lock(path) or {}
        raise LeaseFencedError(
            _dataset_held_message(str(path), body),
            payload=dict(body),
        )

    def release(self, handle: DatasetLockHandle) -> None:
        if not handle.token or handle.path is None:
            return
        path = handle.path
        for _ in range(DEFAULT_CAS_RETRIES):
            body = read_dataset_lock(path)
            if body is None:
                return
            _prune_body(body)
            changed = False
            if handle.mode == "exclusive":
                ex = _live_exclusive(body)
                if ex is not None and str(ex.get("token") or "") == handle.token:
                    body["exclusive"] = None
                    changed = True
            else:
                shared = _live_shared(body)
                new_shared = [
                    row
                    for row in shared
                    if str(row.get("token") or "") != handle.token
                ]
                if len(new_shared) != len(shared):
                    body["shared"] = new_shared
                    changed = True
            if not changed:
                return
            version = handle.version or path.object_version()
            if (
                not _live_exclusive(body)
                and not _live_shared(body)
                and not _live_exclusive_intent(body)
            ):
                if version is None:
                    return
                if path.delete_if_match(version):
                    logger.info("released dataset lock", path=str(path))
                    handle.version = None
                    return
                handle.version = path.object_version()
                continue
            if version is None:
                return
            raw = _serialize(body)
            try:
                handle.version = path.replace_if_match(version, raw)
            except ObjectVersionConflict:
                handle.version = path.object_version()
                continue
            logger.info(
                "released dataset lock holder",
                path=str(path),
                mode=handle.mode,
            )
            return

    def inspect(self, *, dataset_id: str) -> dict[str, Any] | None:
        path = self._path(dataset_id)
        body = read_dataset_lock(path)
        if body is None:
            return None
        _prune_body(body)
        if (
            not _live_exclusive(body)
            and not _live_shared(body)
            and not _live_exclusive_intent(body)
        ):
            return None
        return body

    def force_release(self, *, dataset_id: str) -> dict[str, Any] | None:
        path = self._path(dataset_id)
        payload = read_dataset_lock(path)
        if payload is None:
            return None
        path.unlink(missing_ok=True)
        logger.info(
            "force-released dataset lock",
            path=str(path),
            dataset_id=dataset_id,
        )
        return payload


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


def open_dataset_lock_store(
    lake: LakeRef,
    options: ResolvedLeaseOptions,
    *,
    resolve_secret: Any | None = None,
) -> DatasetLockStore:
    if options.backend == "lake":
        return LakeDatasetLockStore(lake)
    if options.backend == "postgres":
        from det.runtime.lease.dataset_lock_postgres import PostgresDatasetLockStore

        if resolve_secret is None:
            raise ValueError(
                "postgres dataset lock backend requires settings.resolve_secret"
            )
        return PostgresDatasetLockStore(
            resolve_secret=resolve_secret,
            dsn_env=options.pg_dsn_env,
            schema=options.pg_schema,
            locks_table=DEFAULT_DATASET_LOCK_PG_TABLE,
            shared_table=DEFAULT_DATASET_LOCK_SHARED_PG_TABLE,
        )
    raise ValueError(f"unknown lease backend {options.backend!r}")


def _resolve_opts(
    *,
    ttl_sec: int | None,
    owner: str | None,
    enabled: bool | None,
    options: ResolvedLeaseOptions | None,
    env: Mapping[str, str] | None,
) -> ResolvedLeaseOptions:
    from dataclasses import replace

    opts = options or resolve_lease_options(
        env=env, ttl_sec=ttl_sec, owner=owner, enabled=enabled
    )
    if enabled is not None:
        opts = replace(opts, enabled=enabled)
    if ttl_sec is not None:
        opts = replace(opts, ttl_sec=ttl_sec)
    if owner is not None:
        opts = replace(opts, owner=owner)
    if enabled is None and env is not None:
        opts = replace(opts, enabled=locks_enabled(env))
    return opts


def _acquire_shared_handle(
    lake: LakeRef,
    dataset_id: str,
    *,
    command: str,
    ttl_sec: int | None,
    owner: str | None,
    env: Mapping[str, str] | None,
    enabled: bool | None,
    options: ResolvedLeaseOptions | None,
    resolve_secret: Any | None,
    store: DatasetLockStore | None,
) -> DatasetLockHandle | None:
    opts = _resolve_opts(
        ttl_sec=ttl_sec, owner=owner, enabled=enabled, options=options, env=env
    )
    if not opts.enabled:
        return None
    ttl = resolve_lock_ttl_sec(
        ttl_sec if ttl_sec is not None else opts.ttl_sec, env=env
    )
    who = owner or opts.owner or default_lock_owner(env)
    cid = validate_canonical_id(dataset_id)
    nested = _DATASET_HELD.get()
    active_store = store or open_dataset_lock_store(
        lake, opts, resolve_secret=resolve_secret
    )
    if nested == cid:
        active = _DATASET_ACTIVE.get()
        if active is not None:
            return active
    handle = active_store.acquire_shared(
        dataset_id=cid,
        command=command,
        ttl_sec=ttl,
        owner=who,
    )
    handle.store = active_store
    return handle


def refresh_dataset_lock(
    handle: DatasetLockHandle | None,
    *,
    store: DatasetLockStore | None = None,
) -> None:
    if handle is None or not handle.token:
        return
    resolved = store if store is not None else handle.store
    if resolved is not None:
        resolved.refresh(handle)
        return
    if handle.path is not None:
        LakeDatasetLockStore(handle.path.parent.parent.parent).refresh(handle)


def assert_dataset_lock_held(
    handle: DatasetLockHandle | None,
    *,
    store: DatasetLockStore | None = None,
) -> None:
    if handle is None or not handle.token:
        return
    resolved = store if store is not None else handle.store
    if resolved is not None:
        resolved.ensure_held(handle)
        return
    if handle.path is not None:
        LakeDatasetLockStore(handle.path.parent.parent.parent).ensure_held(handle)
        return
    raise LeaseFencedError(
        "dataset lock fence requires a bound store or lake lock path",
        payload={},
    )


def release_dataset_lock(
    handle: DatasetLockHandle | None,
    *,
    store: DatasetLockStore | None = None,
) -> None:
    if handle is None or not handle.token:
        return
    resolved = store if store is not None else handle.store
    if resolved is not None:
        resolved.release(handle)
        return
    if handle.path is not None:
        LakeDatasetLockStore(handle.path.parent.parent.parent).release(handle)


@contextmanager
def dataset_shared_lock(
    lake: LakeRef,
    dataset_id: str,
    *,
    command: str,
    ttl_sec: int | None = None,
    owner: str | None = None,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
    options: ResolvedLeaseOptions | None = None,
    resolve_secret: Any | None = None,
) -> Iterator[DatasetLockHandle | None]:
    opts = _resolve_opts(
        ttl_sec=ttl_sec, owner=owner, enabled=enabled, options=options, env=env
    )
    store: DatasetLockStore | None = None
    if opts.enabled:
        store = open_dataset_lock_store(lake, opts, resolve_secret=resolve_secret)
    handle = _acquire_shared_handle(
        lake,
        dataset_id,
        command=command,
        ttl_sec=ttl_sec,
        owner=owner,
        env=env,
        enabled=opts.enabled,
        options=opts,
        resolve_secret=resolve_secret,
        store=store,
    )
    cid = validate_canonical_id(dataset_id)
    nested = _DATASET_HELD.get() == cid
    held_token = None
    active_token = None
    if handle is not None and not nested:
        held_token = _DATASET_HELD.set(cid)
        active_token = _DATASET_ACTIVE.set(handle)
    try:
        if handle is not None and not nested:
            refresh_dataset_lock(handle, store=store)
        yield handle
    finally:
        if handle is not None and not nested:
            release_dataset_lock(handle, store=store)
            if active_token is not None:
                _DATASET_ACTIVE.reset(active_token)
            if held_token is not None:
                _DATASET_HELD.reset(held_token)


@contextmanager
def dataset_exclusive_lock(
    lake: LakeRef,
    dataset_id: str,
    *,
    command: str,
    ttl_sec: int | None = None,
    owner: str | None = None,
    env: Mapping[str, str] | None = None,
    enabled: bool | None = None,
    options: ResolvedLeaseOptions | None = None,
    resolve_secret: Any | None = None,
    wait: bool = True,
    wait_sec: int | None = None,
    poll_interval: float = 0.2,
) -> Iterator[DatasetLockHandle | None]:
    opts = _resolve_opts(
        ttl_sec=ttl_sec, owner=owner, enabled=enabled, options=options, env=env
    )
    if not opts.enabled:
        yield None
        return
    ttl = resolve_lock_ttl_sec(
        ttl_sec if ttl_sec is not None else opts.ttl_sec, env=env
    )
    who = owner or opts.owner or default_lock_owner(env)
    cid = validate_canonical_id(dataset_id)
    if _DATASET_HELD.get() == cid:
        raise LeaseHeldError(
            f"dataset lock already held in-process for {cid}",
            payload={"dataset_id": cid},
        )
    store = open_dataset_lock_store(lake, opts, resolve_secret=resolve_secret)
    if wait_sec is None:
        wait_sec = resolve_dataset_lock_wait_sec(env)
    handle = store.acquire_exclusive(
        dataset_id=cid,
        command=command,
        ttl_sec=ttl,
        owner=who,
        wait=wait,
        wait_sec=wait_sec,
        poll_interval=poll_interval,
    )
    handle.store = store
    held_token = _DATASET_HELD.set(cid)
    active_token = _DATASET_ACTIVE.set(handle)
    try:
        refresh_dataset_lock(handle, store=store)
        yield handle
    finally:
        release_dataset_lock(handle, store=store)
        _DATASET_ACTIVE.reset(active_token)
        _DATASET_HELD.reset(held_token)


def force_release_dataset_lock(
    lake: LakeRef,
    dataset_id: str,
    *,
    options: ResolvedLeaseOptions,
    resolve_secret: Any | None = None,
) -> dict[str, Any] | None:
    store = open_dataset_lock_store(lake, options, resolve_secret=resolve_secret)
    return store.force_release(dataset_id=validate_canonical_id(dataset_id))
