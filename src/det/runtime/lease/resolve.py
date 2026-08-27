"""Resolve lease options from env / DetSettings / pipeline YAML."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING

from det.runtime.lease._common import (
    DEFAULT_LOCK_BACKEND,
    DEFAULT_LOCK_MODE,
    DEFAULT_LOCK_PG_DSN_ENV,
    DEFAULT_LOCK_PG_SCHEMA,
    DEFAULT_LOCK_PG_TABLE,
    default_lock_owner,
    locks_enabled,
    resolve_lock_ttl_sec,
)
from det.runtime.lease.store import LockBackend, LockMode, ResolvedLeaseOptions

if TYPE_CHECKING:
    from det.runtime.config import LeaseConfig, PipelineConfig
    from det.runtime.settings import DetSettings


def _env_get(environ: Mapping[str, str], key: str) -> str | None:
    raw = environ.get(key)
    if raw is None or not str(raw).strip():
        return None
    return str(raw).strip()


def parse_lock_backend(raw: str | None) -> LockBackend | None:
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip().lower()
    if text not in {"lake", "postgres"}:
        raise ValueError(f"DET_LOCK_BACKEND must be 'lake' or 'postgres', got {raw!r}")
    return text  # type: ignore[return-value]


def parse_lock_mode(raw: str | None) -> LockMode | None:
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip().lower()
    if text not in {"exact", "overlap"}:
        raise ValueError(f"DET_LOCK_MODE must be 'exact' or 'overlap', got {raw!r}")
    return text  # type: ignore[return-value]


def resolve_lease_options(
    *,
    settings: DetSettings | None = None,
    pipeline: PipelineConfig | None = None,
    env: Mapping[str, str] | None = None,
    ttl_sec: int | None = None,
    owner: str | None = None,
    enabled: bool | None = None,
) -> ResolvedLeaseOptions:
    """effective_X = env if set else pipeline.lease.X if set else settings/default."""
    environ: Mapping[str, str] = os.environ if env is None else env
    lease_cfg: LeaseConfig | None = None
    if pipeline is not None:
        lease_cfg = pipeline.lease

    backend = parse_lock_backend(_env_get(environ, "DET_LOCK_BACKEND"))
    if backend is None and lease_cfg is not None and lease_cfg.backend:
        backend = lease_cfg.backend  # type: ignore[assignment]
    if backend is None and settings is not None and settings.lock_backend:
        # Settings may carry from_env defaults; only used when env + pipeline omit.
        backend = settings.lock_backend  # type: ignore[assignment]
    if backend is None:
        backend = DEFAULT_LOCK_BACKEND  # type: ignore[assignment]

    mode = parse_lock_mode(_env_get(environ, "DET_LOCK_MODE"))
    if mode is None and lease_cfg is not None and lease_cfg.mode:
        mode = lease_cfg.mode  # type: ignore[assignment]
    if mode is None and settings is not None and settings.lock_mode:
        mode = settings.lock_mode  # type: ignore[assignment]
    if mode is None:
        mode = DEFAULT_LOCK_MODE  # type: ignore[assignment]

    pg_dsn_env = _env_get(environ, "DET_LOCK_PG_DSN_ENV")
    if pg_dsn_env is None and lease_cfg is not None and lease_cfg.pg_dsn_env:
        pg_dsn_env = lease_cfg.pg_dsn_env
    if pg_dsn_env is None and settings is not None:
        pg_dsn_env = settings.lock_pg_dsn_env
    if pg_dsn_env is None:
        pg_dsn_env = DEFAULT_LOCK_PG_DSN_ENV

    pg_schema = _env_get(environ, "DET_LOCK_PG_SCHEMA")
    if pg_schema is None and lease_cfg is not None and lease_cfg.pg_schema:
        pg_schema = lease_cfg.pg_schema
    if pg_schema is None and settings is not None:
        pg_schema = settings.lock_pg_schema
    if pg_schema is None:
        pg_schema = DEFAULT_LOCK_PG_SCHEMA

    pg_table = _env_get(environ, "DET_LOCK_PG_TABLE")
    if pg_table is None and lease_cfg is not None and lease_cfg.pg_table:
        pg_table = lease_cfg.pg_table
    if pg_table is None and settings is not None:
        pg_table = settings.lock_pg_table
    if pg_table is None:
        pg_table = DEFAULT_LOCK_PG_TABLE

    if mode == "overlap" and backend != "postgres":
        raise ValueError(
            "lease mode 'overlap' requires backend=postgres "
            f"(got backend={backend!r})"
        )

    if enabled is None:
        if settings is not None:
            enabled = settings.locks_enabled
        else:
            enabled = locks_enabled(environ)

    if ttl_sec is None:
        if settings is not None:
            ttl_sec = settings.lock_ttl_sec
        else:
            ttl_sec = resolve_lock_ttl_sec(None, env=environ)

    if owner is None:
        if settings is not None:
            owner = settings.lock_owner
        else:
            owner = default_lock_owner(environ)

    return ResolvedLeaseOptions(
        backend=backend,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        pg_dsn_env=pg_dsn_env,
        pg_schema=pg_schema,
        pg_table=pg_table,
        enabled=bool(enabled),
        ttl_sec=int(ttl_sec) if ttl_sec is not None else None,
        owner=owner,
    )
