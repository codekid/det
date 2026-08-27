"""LeaseStore protocol and factory."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from det.runtime.lake import LakeRef
from det.runtime.lease._common import (
    DEFAULT_LOCK_BACKEND,
    DEFAULT_LOCK_MODE,
    DEFAULT_LOCK_PG_DSN_ENV,
    DEFAULT_LOCK_PG_SCHEMA,
    DEFAULT_LOCK_PG_TABLE,
    Lease,
)

LockBackend = Literal["lake", "postgres"]
LockMode = Literal["exact", "overlap"]


@dataclass(frozen=True)
class ResolvedLeaseOptions:
    backend: LockBackend = DEFAULT_LOCK_BACKEND  # type: ignore[assignment]
    mode: LockMode = DEFAULT_LOCK_MODE  # type: ignore[assignment]
    pg_dsn_env: str = DEFAULT_LOCK_PG_DSN_ENV
    pg_schema: str = DEFAULT_LOCK_PG_SCHEMA
    pg_table: str = DEFAULT_LOCK_PG_TABLE
    enabled: bool = True
    ttl_sec: int | None = None
    owner: str | None = None


class LeaseStore(Protocol):
    def acquire(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> Lease: ...

    def refresh(self, lease: Lease) -> None: ...

    def ensure_held(self, lease: Lease) -> None:
        """Hard fence: raise if this handle no longer owns the live lease."""
        ...

    def release(self, lease: Lease) -> None: ...

    def inspect(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
    ) -> dict[str, Any] | None: ...

    def force_release(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
    ) -> dict[str, Any] | None: ...


def open_lease_store(
    lake: LakeRef,
    options: ResolvedLeaseOptions,
    *,
    resolve_secret: Any | None = None,
) -> LeaseStore:
    """Build the store for *options*. Postgres requires ``resolve_secret``."""
    if options.backend == "lake":
        from det.runtime.lease.lake_store import LakeLeaseStore

        if options.mode != "exact":
            raise ValueError(
                f"lease mode {options.mode!r} requires backend=postgres "
                "(lake leases are equality-only)"
            )
        return LakeLeaseStore(lake)

    if options.backend == "postgres":
        from det.runtime.lease.postgres_store import PostgresLeaseStore

        if resolve_secret is None:
            raise ValueError(
                "postgres lease backend requires settings.resolve_secret "
                "(or an explicit resolve_secret callable)"
            )
        return PostgresLeaseStore(
            resolve_secret=resolve_secret,
            dsn_env=options.pg_dsn_env,
            schema=options.pg_schema,
            table=options.pg_table,
            mode=options.mode,
        )

    raise ValueError(f"unknown lease backend {options.backend!r}")
