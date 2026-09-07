"""TypedDict contracts for silver catch-up ops payloads.

Committed shapes under ``ops/silver_catchup/`` (immutable scm manifest + runs
NDJSON) and BQ cleanup plan/list rows. Display-only diff envelopes stay untyped.
"""

from __future__ import annotations

from typing import Literal, NotRequired, TypedDict


class CatchupSidecarRunRow(TypedDict):
    """Coverage keys only — what ``.runs.jsonl`` stores (no ``detected_at``)."""

    pipeline: str
    interval_start: str
    interval_end: str
    extract_run_datetime: str


class CatchupRunRow(TypedDict):
    """Manifest ``runs[]`` entry — coverage keys plus required detection stamp."""

    pipeline: str
    interval_start: str
    interval_end: str
    extract_run_datetime: str
    detected_at: str


class CatchupManifestPayload(TypedDict):
    """Immutable catch-up commit object ``ops/silver_catchup/<scm_id>.json``."""

    manifest_version: int
    manifest_id: str
    content_digest: str
    updated_at: str
    runs: list[CatchupRunRow]


class CatchupCleanupTarget(TypedDict):
    """One ``_det_catchup_runs_*`` table in a cleanup list/plan."""

    table_id: str
    relation: str
    created: str | None
    manifest_id: str | None
    existed: NotRequired[bool]


class CatchupCleanupPlan(TypedDict):
    """Dry-run / apply payload for BQ catch-up external-table cleanup."""

    mode: Literal["manifest_id", "created_before"]
    manifest_id: str | None
    older_than: str | None
    created_before: str | None
    project: str
    dataset: str
    targets: list[CatchupCleanupTarget]
    target_count: int


class CatchupCleanupDropResult(TypedDict):
    """Result of dropping one catch-up external table."""

    manifest_id: str
    relation: str
    existed: bool
    dropped: bool


class CatchupCleanupApplyResult(TypedDict):
    """Apply payload: cleanup plan fields plus drop results."""

    mode: Literal["manifest_id", "created_before"]
    manifest_id: str | None
    older_than: str | None
    created_before: str | None
    project: str
    dataset: str
    targets: list[CatchupCleanupTarget]
    target_count: int
    results: list[CatchupCleanupDropResult]
    dropped_count: int
