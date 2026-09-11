"""Approval backend constants (no settings import — safe for DetSettings)."""

from __future__ import annotations

from typing import Literal

ApprovalBackend = Literal["lake", "postgres"]

DEFAULT_APPROVAL_BACKEND: ApprovalBackend = "lake"
DEFAULT_APPROVAL_PG_DSN_ENV = "DET_APPROVAL_PG_DSN"
DEFAULT_APPROVAL_PG_SCHEMA = "det_approval"
DEFAULT_APPROVAL_PG_TABLE = "approvals"

ENV_BACKEND = "DET_APPROVAL_BACKEND"
ENV_PG_DSN_ENV = "DET_APPROVAL_PG_DSN_ENV"
ENV_PG_SCHEMA = "DET_APPROVAL_PG_SCHEMA"
ENV_PG_TABLE = "DET_APPROVAL_PG_TABLE"


def parse_approval_backend(raw: str | None) -> ApprovalBackend | None:
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip().lower()
    if text not in {"lake", "postgres"}:
        raise ValueError(f"{ENV_BACKEND} must be 'lake' or 'postgres', got {raw!r}")
    return text  # type: ignore[return-value]
