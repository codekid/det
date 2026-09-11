"""Live Postgres approval store. Skipped unless DET_APPROVAL_PG_DSN or DET_LOCK_PG_DSN is set."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

from det.runtime.approval import (
    claim_approval,
    consume_approval,
    create_approval,
    prune_write_argv,
    release_approval,
)
from det.runtime.settings import DetSettings, use_settings

_DSN = (
    os.environ.get("DET_APPROVAL_PG_DSN") or os.environ.get("DET_LOCK_PG_DSN") or ""
).strip()

pytestmark = [
    pytest.mark.skipif(not _DSN, reason="DET_APPROVAL_PG_DSN / DET_LOCK_PG_DSN not set"),
]

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


def test_postgres_create_claim_consume_release(tmp_path, monkeypatch):
    monkeypatch.setenv("DET_APPROVAL_BACKEND", "postgres")
    monkeypatch.setenv("DET_APPROVAL_PG_DSN", _DSN)
    monkeypatch.setenv("DET_APPROVAL_PG_SCHEMA", "det_approval_test")
    monkeypatch.setenv("DET_APPROVAL_PG_TABLE", "approvals_ut")
    settings = DetSettings.from_env(project_root=tmp_path)
    argv = prune_write_argv("example_api.events", "2026-08-01")
    with use_settings(settings):
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="pg-tester", now=NOW
        )
        claimed = claim_approval(
            tmp_path, "prune", argv, rec["id"], require=True, now=NOW
        )
        assert claimed is not None and claimed["status"] == "claimed"
        released = release_approval(tmp_path, rec["id"], released_by="ops", now=NOW)
        assert released["status"] == "unused"
        claimed2 = claim_approval(
            tmp_path, "prune", argv, rec["id"], require=True, now=NOW
        )
        assert claimed2 is not None
        consumed = consume_approval(tmp_path, rec["id"], now=NOW)
        assert consumed["status"] == "consumed"
