from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from det.runtime.lake import open_lake
from det.runtime.receipts import normalize_receipt
from det.runtime.receipts_materialize import materialize_receipts, scan_ops_run_receipts

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")


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


def test_normalize_receipt_maps_v1_fields():
    row = normalize_receipt(
        {
            "receipt_version": 1,
            "attempt_id": "abc123",
            "pipeline": "noaa.storm_events",
            "command": "load",
            "interval_start": "2026-08-01T00:00:00+00:00",
            "interval_end": "2026-08-02T00:00:00+00:00",
            "status": "error",
            "started_at": "2026-08-16T12:00:00+00:00",
            "finished_at": "2026-08-16T12:00:01+00:00",
            "duration_ms": 1000,
            "owner": "cli",
            "error_code": "http_error",
            "error_class": "HttpError",
            "error_message": "boom",
            "lake_layout": 1,
            "unknown_extra": "ignored",
        }
    )
    assert row is not None
    assert row["attempt_id"] == "abc123"
    assert row["attempt_date"] == date(2026, 8, 16)
    assert row["error_code"] == "http_error"
    assert "unknown_extra" not in row
    assert "lake_layout" not in row


def test_normalize_receipt_rejects_incomplete():
    assert normalize_receipt({"attempt_id": "x"}) is None


def test_materialize_replace_by_day_idempotent(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    day = datetime(2026, 8, 16, 10, 0, tzinfo=UTC)
    _write_json_receipt(lake, attempt_id="aaaa1111", started_at=day)
    _write_json_receipt(lake, attempt_id="bbbb2222", started_at=day, command="load")

    stats1 = materialize_receipts(
        lake, since="2026-08-16", until="2026-08-17", now=day
    )
    assert stats1.rows_written == 2
    assert stats1.days_touched == 1
    rows1 = scan_ops_run_receipts(lake)
    assert {r["attempt_id"] for r in rows1} == {"aaaa1111", "bbbb2222"}

    stats2 = materialize_receipts(
        lake, since="2026-08-16", until="2026-08-17", now=day
    )
    assert stats2.rows_written == 2
    rows2 = scan_ops_run_receipts(lake)
    ids = [r["attempt_id"] for r in rows2]
    assert sorted(ids) == ["aaaa1111", "bbbb2222"]
    assert len(ids) == 2


def test_materialize_empty_day_clears_partition(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    day = datetime(2026, 8, 16, 10, 0, tzinfo=UTC)
    _write_json_receipt(lake, attempt_id="cccc3333", started_at=day)
    materialize_receipts(lake, since="2026-08-16", until="2026-08-17", now=day)
    assert len(scan_ops_run_receipts(lake)) == 1

    # Remove JSON source of truth for that day, rematerialize → partition cleared.
    runs = lake / "runs" / "dt=2026-08-16"
    for child in runs.rglob("*.json"):
        child.unlink()
    materialize_receipts(lake, since="2026-08-16", until="2026-08-17", now=day)
    assert scan_ops_run_receipts(lake) == []


def test_runs_materialize_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import structlog
    from typer.testing import CliRunner

    from det.cli import app
    from det.logging import configure_logging

    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    day = datetime(2026, 8, 16, 10, 0, tzinfo=UTC)
    _write_json_receipt(lake, attempt_id="cli00001", started_at=day)
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    try:
        result = runner.invoke(
            app,
            [
                "runs-materialize",
                "-s",
                "2026-08-16",
                "-e",
                "2026-08-17",
                "--lake-path",
                str(tmp_path / "lake"),
                "--project-root",
                str(tmp_path),
            ],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0, result.output
    assert "rows=1" in result.output
    assert "ops/run_receipts" in result.output
