"""Hermetic ops SLO: fixture receipts → materialize → dbt build tag:ops."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.dbt_runner import run_dbt
from det.runtime.lake import open_lake
from det.runtime.receipts_materialize import materialize_receipts

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")
pytest.importorskip("dbt.cli.main")


def _write_json_receipt(
    lake,
    *,
    attempt_id: str,
    started_at: datetime,
    pipeline: str = "noaa.storm_events",
    command: str = "extract",
    status: str = "ok",
    **extra,
) -> None:
    body = {
        "receipt_version": 1,
        "attempt_id": attempt_id,
        "pipeline": pipeline,
        "command": command,
        "interval_start": "2026-08-01T00:00:00+00:00",
        "interval_end": "2026-08-02T00:00:00+00:00",
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": started_at.isoformat(),
        "duration_ms": 12,
        "owner": "test",
        **extra,
    }
    dt = started_at.astimezone(UTC).date().isoformat()
    path = (
        lake
        / "runs"
        / f"dt={dt}"
        / pipeline
        / f"{command}__2026-08-01T00-00-00Z_2026-08-02T00-00-00Z__{attempt_id}.json"
    )
    path.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")


def _seed_ok_receipts(lake, *, now: datetime) -> None:
    """Recent ok extract+load for seeded noaa.storm_events SLOs."""
    _write_json_receipt(
        lake, attempt_id="okextract1", started_at=now, command="extract"
    )
    _write_json_receipt(
        lake, attempt_id="okload0001", started_at=now, command="load"
    )


@pytest.mark.integration
def test_ops_tag_build_passes_with_recent_ok_receipts(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    lake_root = tmp_path / "lake"
    ops_db = tmp_path / "det_ops.duckdb"
    lake = open_lake(str(lake_root), tmp_path)
    now = datetime.now(tz=UTC)
    _seed_ok_receipts(lake, now=now)
    stats = materialize_receipts(
        lake,
        since=(now - timedelta(days=1)).date().isoformat(),
        until=(now + timedelta(days=1)).date().isoformat(),
        now=now,
    )
    assert stats.rows_written == 2

    monkeypatch.setenv("DET_OPS_DUCKDB", str(ops_db.resolve()))
    result = run_dbt(
        project_root=project_root,
        select=["tag:ops"],
        lake_path=str(lake_root.resolve()),
    )
    assert result.returncode == 0, result.output[-4000:]


@pytest.mark.integration
def test_ops_tag_build_fails_on_schema_invalid_fail_closed(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    lake_root = tmp_path / "lake"
    ops_db = tmp_path / "det_ops.duckdb"
    lake = open_lake(str(lake_root), tmp_path)
    now = datetime.now(tz=UTC)
    _seed_ok_receipts(lake, now=now)
    _write_json_receipt(
        lake,
        attempt_id="badschema1",
        started_at=now - timedelta(minutes=5),
        command="extract",
        status="error",
        error_code="schema_invalid",
        error_class="SchemaError",
        error_message="fixture fail-closed",
    )
    materialize_receipts(
        lake,
        since=(now - timedelta(days=1)).date().isoformat(),
        until=(now + timedelta(days=1)).date().isoformat(),
        now=now,
    )

    monkeypatch.setenv("DET_OPS_DUCKDB", str(ops_db.resolve()))
    result = run_dbt(
        project_root=project_root,
        select=["tag:ops"],
        lake_path=str(lake_root.resolve()),
    )
    assert result.returncode != 0, result.output[-4000:]
    assert "assert_ops_slo_fail_closed" in result.output or "FAIL" in result.output
