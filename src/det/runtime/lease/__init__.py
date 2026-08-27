"""Pipeline interval leases: lake CAS (default) or opt-in Postgres."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from det.runtime.lake import LakeRef
from det.runtime.lease._common import (
    _ACTIVE,
    _HELD,
    DEFAULT_LOCK_BACKEND,
    DEFAULT_LOCK_MODE,
    DEFAULT_LOCK_PG_DSN_ENV,
    DEFAULT_LOCK_PG_SCHEMA,
    DEFAULT_LOCK_PG_TABLE,
    DEFAULT_LOCK_TTL_SEC,
    Lease,
    LeaseHeldError,
    advisory_lock_keys,
    default_lock_owner,
    lock_id,
    lock_path,
    locks_enabled,
    resolve_lock_ttl_sec,
)
from det.runtime.lease.lake_store import LakeLeaseStore, force_release_lock, read_lock
from det.runtime.lease.resolve import resolve_lease_options
from det.runtime.lease.store import (
    LeaseStore,
    ResolvedLeaseOptions,
    open_lease_store,
)

# Public library aliases (det.__all__); same callables as read/force-release.
inspect_lease = read_lock
release_lock = force_release_lock


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
    options: ResolvedLeaseOptions | None = None,
    store: LeaseStore | None = None,
    resolve_secret: Any | None = None,
) -> Lease | None:
    """Create the lock object. Returns None when locks are disabled.

    Nested same id is a no-op Lease.
    """
    opts = options or resolve_lease_options(
        env=env, ttl_sec=ttl_sec, owner=owner, enabled=enabled
    )
    if enabled is not None:
        # Explicit flag wins over resolved options.
        from dataclasses import replace

        opts = replace(opts, enabled=enabled)
    if not opts.enabled:
        return None

    ttl = resolve_lock_ttl_sec(
        ttl_sec if ttl_sec is not None else opts.ttl_sec, env=env
    )
    who = owner or opts.owner or default_lock_owner(env)
    ident = lock_id(pipeline, interval_start, interval_end)
    nested = _HELD.get()
    active_store = store or open_lease_store(lake, opts, resolve_secret=resolve_secret)

    if nested == ident:
        active = _ACTIVE.get()
        if active is not None:
            return active
        existing = active_store.inspect(
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
        ) or {}
        path = (
            lock_path(lake, pipeline, interval_start, interval_end)
            if opts.backend == "lake"
            else None
        )
        return Lease(
            path=path,
            token=str(existing.get("token") or ""),
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            ttl_sec=ttl,
            lock_id=ident,
            version=path.object_version() if path is not None else None,
        )

    return active_store.acquire(
        pipeline=pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        command=command,
        ttl_sec=ttl,
        owner=who,
    )


def refresh_lease(lease: Lease | None, *, store: LeaseStore | None = None) -> None:
    if lease is None or not lease.token:
        return
    if store is not None:
        store.refresh(lease)
        return
    if lease.path is not None:
        # LakeLeaseStore.refresh only uses lease.path; lake root arg is unused.
        LakeLeaseStore(lease.path).refresh(lease)


def release_lease(lease: Lease | None, *, store: LeaseStore | None = None) -> None:
    if lease is None or not lease.token:
        return
    if store is not None:
        store.release(lease)
        return
    if lease.path is not None:
        LakeLeaseStore(lease.path).release(lease)


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
    options: ResolvedLeaseOptions | None = None,
    resolve_secret: Any | None = None,
) -> Iterator[Lease | None]:
    opts = options or resolve_lease_options(
        env=env, ttl_sec=ttl_sec, owner=owner, enabled=enabled
    )
    if enabled is not None:
        from dataclasses import replace

        opts = replace(opts, enabled=enabled)

    store: LeaseStore | None = None
    if opts.enabled:
        store = open_lease_store(lake, opts, resolve_secret=resolve_secret)

    lease = acquire_lease(
        lake,
        pipeline=pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        command=command,
        ttl_sec=ttl_sec,
        owner=owner,
        env=env,
        enabled=opts.enabled,
        options=opts,
        store=store,
        resolve_secret=resolve_secret,
    )
    ident = lock_id(pipeline, interval_start, interval_end)
    nested = _HELD.get() == ident
    held_token = None
    active_token = None
    if lease is not None and not nested:
        held_token = _HELD.set(ident)
        active_token = _ACTIVE.set(lease)
    try:
        if lease is not None and not nested:
            if store is not None:
                store.refresh(lease)
            else:
                refresh_lease(lease)
        yield lease
    finally:
        if lease is not None and not nested:
            if store is not None:
                store.release(lease)
            else:
                release_lease(lease)
            if active_token is not None:
                _ACTIVE.reset(active_token)
            if held_token is not None:
                _HELD.reset(held_token)


__all__ = [
    "DEFAULT_LOCK_BACKEND",
    "DEFAULT_LOCK_MODE",
    "DEFAULT_LOCK_PG_DSN_ENV",
    "DEFAULT_LOCK_PG_SCHEMA",
    "DEFAULT_LOCK_PG_TABLE",
    "DEFAULT_LOCK_TTL_SEC",
    "Lease",
    "LeaseHeldError",
    "LeaseStore",
    "ResolvedLeaseOptions",
    "acquire_lease",
    "advisory_lock_keys",
    "default_lock_owner",
    "force_release_lock",
    "inspect_lease",
    "lock_id",
    "lock_path",
    "locks_enabled",
    "open_lease_store",
    "pipeline_lease",
    "read_lock",
    "refresh_lease",
    "release_lease",
    "release_lock",
    "resolve_lease_options",
    "resolve_lock_ttl_sec",
]
