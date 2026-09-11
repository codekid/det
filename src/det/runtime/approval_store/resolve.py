"""Resolve approval backend options and open the store."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.runtime.approval_store.constants import (
    DEFAULT_APPROVAL_BACKEND,
    DEFAULT_APPROVAL_PG_DSN_ENV,
    DEFAULT_APPROVAL_PG_SCHEMA,
    DEFAULT_APPROVAL_PG_TABLE,
    ENV_BACKEND,
    ENV_PG_DSN_ENV,
    ENV_PG_SCHEMA,
    ENV_PG_TABLE,
    ApprovalBackend,
    parse_approval_backend,
)
from det.runtime.approval_store.store import ApprovalStore

__all__ = [
    "ApprovalBackend",
    "DEFAULT_APPROVAL_BACKEND",
    "DEFAULT_APPROVAL_PG_DSN_ENV",
    "DEFAULT_APPROVAL_PG_SCHEMA",
    "DEFAULT_APPROVAL_PG_TABLE",
    "ResolvedApprovalOptions",
    "open_approval_store",
    "parse_approval_backend",
    "resolve_approval_options",
]


@dataclass(frozen=True)
class ResolvedApprovalOptions:
    backend: ApprovalBackend = DEFAULT_APPROVAL_BACKEND
    pg_dsn_env: str = DEFAULT_APPROVAL_PG_DSN_ENV
    pg_schema: str = DEFAULT_APPROVAL_PG_SCHEMA
    pg_table: str = DEFAULT_APPROVAL_PG_TABLE


def resolve_approval_options(
    *,
    env: Mapping[str, str] | None = None,
    settings: Any | None = None,
) -> ResolvedApprovalOptions:
    """Resolve backend options.

    Explicit non-empty values on ``DetSettings`` (including ``with_overrides``)
    win over ``DET_APPROVAL_*`` env. Env is used only when settings is absent
    or the corresponding settings field is empty.
    """
    environ = os.environ if env is None else env

    backend = None
    if settings is not None:
        backend = parse_approval_backend(getattr(settings, "approval_backend", None))
    if backend is None:
        backend = parse_approval_backend(environ.get(ENV_BACKEND))
    if backend is None:
        backend = DEFAULT_APPROVAL_BACKEND

    def _pick(attr: str, env_key: str, default: str) -> str:
        if settings is not None:
            from_settings = str(getattr(settings, attr, "") or "").strip()
            if from_settings:
                return from_settings
        from_env = (environ.get(env_key) or "").strip()
        if from_env:
            return from_env
        return default

    return ResolvedApprovalOptions(
        backend=backend,
        pg_dsn_env=_pick("approval_pg_dsn_env", ENV_PG_DSN_ENV, DEFAULT_APPROVAL_PG_DSN_ENV),
        pg_schema=_pick("approval_pg_schema", ENV_PG_SCHEMA, DEFAULT_APPROVAL_PG_SCHEMA),
        pg_table=_pick("approval_pg_table", ENV_PG_TABLE, DEFAULT_APPROVAL_PG_TABLE),
    )


def open_approval_store(
    project_root: Path,
    *,
    options: ResolvedApprovalOptions | None = None,
    settings: Any | None = None,
    resolve_secret: Any | None = None,
    lake_path: str | None = None,
) -> ApprovalStore:
    """Build the active approval store (lake or postgres) with legacy read fallback."""
    from det.runtime.approval_store.composite import CompositeApprovalStore
    from det.runtime.settings import DetSettings, get_active_settings

    root = project_root.resolve()
    active = settings if settings is not None else get_active_settings()
    if active is None:
        active = DetSettings.from_env(project_root=root)
    opts = options or resolve_approval_options(settings=active)
    secret = resolve_secret or active.resolve_secret

    if opts.backend == "postgres":
        from det.runtime.approval_store.postgres_store import PostgresApprovalStore

        primary: ApprovalStore = PostgresApprovalStore(
            resolve_secret=secret,
            dsn_env=opts.pg_dsn_env,
            schema=opts.pg_schema,
            table=opts.pg_table,
        )
    else:
        from det.runtime.approval_store.lake_store import LakeApprovalStore
        from det.runtime.silver_catchup.paths import resolve_ops_lake

        ops = resolve_ops_lake(project_root=root, settings=active, lake_path=lake_path)
        primary = LakeApprovalStore(ops)

    from det.runtime.approval_store.legacy import LegacyApprovalReader

    return CompositeApprovalStore(
        primary=primary,
        legacy=LegacyApprovalReader(root),
    )
