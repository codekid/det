"""Pluggable approval storage (lake default, Postgres opt-in)."""

from __future__ import annotations

from det.runtime.approval_store.constants import (
    DEFAULT_APPROVAL_BACKEND,
    DEFAULT_APPROVAL_PG_DSN_ENV,
    DEFAULT_APPROVAL_PG_SCHEMA,
    DEFAULT_APPROVAL_PG_TABLE,
    ApprovalBackend,
    parse_approval_backend,
)
from det.runtime.approval_store.enrich import (
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    HEARTBEAT_STALE_FACTOR,
    enrich_heartbeat_fields,
)
from det.runtime.approval_store.resolve import (
    ResolvedApprovalOptions,
    open_approval_store,
    resolve_approval_options,
)
from det.runtime.approval_store.store import ApprovalStore

__all__ = [
    "ApprovalBackend",
    "ApprovalStore",
    "DEFAULT_APPROVAL_BACKEND",
    "DEFAULT_APPROVAL_PG_DSN_ENV",
    "DEFAULT_APPROVAL_PG_SCHEMA",
    "DEFAULT_APPROVAL_PG_TABLE",
    "DEFAULT_HEARTBEAT_INTERVAL_SEC",
    "HEARTBEAT_STALE_FACTOR",
    "ResolvedApprovalOptions",
    "enrich_heartbeat_fields",
    "open_approval_store",
    "parse_approval_backend",
    "resolve_approval_options",
]
