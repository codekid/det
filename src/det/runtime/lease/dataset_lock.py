"""Bronze-dataset reader/writer locks (shared publish / exclusive recreate)."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from det.runtime.ids import validate_canonical_id
from det.runtime.lake import LakeRef
from det.runtime.lease._common import (
    LeaseFencedError,
    LeaseHeldError,
    default_lock_owner,
    locks_enabled,
    resolve_lock_ttl_sec,
)
from det.runtime.lease.dataset_lock_body import (
    _dataset_held_message,
    read_dataset_lock,
)
from det.runtime.lease.dataset_lock_lake import LakeDatasetLockStore
from det.runtime.lease.dataset_lock_types import (
    _DATASET_ACTIVE,
    _DATASET_HELD,
    DEFAULT_CAS_RETRIES,
    DEFAULT_DATASET_LOCK_PG_TABLE,
    DEFAULT_DATASET_LOCK_SHARED_PG_TABLE,
    DEFAULT_DATASET_LOCK_WAIT_SEC,
    DatasetLockHandle,
    DatasetLockMode,
    DatasetLockStore,
    dataset_lock_path,
    resolve_dataset_lock_wait_sec,
)
from det.runtime.lease.resolve import resolve_lease_options
from det.runtime.lease.store import ResolvedLeaseOptions

__all__ = [
    "DEFAULT_CAS_RETRIES",
    "DEFAULT_DATASET_LOCK_PG_TABLE",
    "DEFAULT_DATASET_LOCK_SHARED_PG_TABLE",
    "DEFAULT_DATASET_LOCK_WAIT_SEC",
    "DatasetLockHandle",
    "DatasetLockMode",
    "DatasetLockStore",
    "LakeDatasetLockStore",
    "_dataset_held_message",
    "assert_dataset_lock_held",
    "dataset_exclusive_lock",
    "dataset_lock_path",
    "dataset_shared_lock",
    "force_release_dataset_lock",
    "open_dataset_lock_store",
    "read_dataset_lock",
    "refresh_dataset_lock",
    "release_dataset_lock",
    "resolve_dataset_lock_wait_sec",
]


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
