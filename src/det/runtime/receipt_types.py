"""TypedDict contracts for lake run receipts under ``{lake}/runs/``."""

from __future__ import annotations

from datetime import date
from typing import NotRequired, TypedDict


class ReceiptPayload(TypedDict):
    """Normalized ops receipt row (see ``OPS_RECEIPT_COLUMNS`` / ``normalize_receipt``)."""

    receipt_version: int
    attempt_id: str
    pipeline: str
    command: str
    status: str
    started_at: str
    attempt_date: NotRequired[str | date]
    lake_layout: NotRequired[int]
    interval_start: NotRequired[str | None]
    interval_end: NotRequired[str | None]
    extract_run_datetime: NotRequired[str | None]
    wire_version: NotRequired[int | None]
    finished_at: NotRequired[str | None]
    duration_ms: NotRequired[int | None]
    owner: NotRequired[str]
    destination: NotRequired[str | None]
    artifacts: NotRequired[int | None]
    raw_bytes: NotRequired[int | None]
    rows: NotRequired[int | None]
    schema_sha256: NotRequired[str | None]
    error_code: NotRequired[str | None]
    error_class: NotRequired[str | None]
    error_message: NotRequired[str | None]
