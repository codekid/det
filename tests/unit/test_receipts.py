from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from det.errors import DetPluginError
from det.ingestion.det_backend import DetBackend
from det.logging import register_secret_value
from det.runtime.coerce import CoerceError
from det.runtime.config import load_pipeline_config
from det.runtime.lake import LakeRef, clear_memory_lakes, open_lake
from det.runtime.lease import LeaseFencedError, LeaseHeldError, pipeline_lease
from det.runtime.manifest import is_committed_raw_dir
from det.runtime.meta import resolve_interval
from det.runtime.receipts import (
    ReceiptDraft,
    attempt_window,
    classify_error,
    list_receipts,
    receipts_enabled,
    record_attempt,
    sum_artifact_bytes,
    summarize_receipts,
    write_receipt,
)
from det.runtime.runner import PipelineRunner
from det.runtime.secrets import SecretNotSetError
from det.sources.example_api.events import ExampleApiSource
from det.sources.http import HttpError, HttpIntegrityError


@pytest.fixture(autouse=True)
def _reset_memory():
    clear_memory_lakes()
    yield
    clear_memory_lakes()


def _example_pipe(
    tmp_path: Path,
    project_root: Path,
    *,
    destination: dict | None = None,
) -> Path:
    schema_src = project_root / "schemas/example_api/events/events.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema_dst.parent.mkdir(parents=True, exist_ok=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True, exist_ok=True)
    dest = destination or {"type": "filesystem", "path": str(tmp_path / "lake")}
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {
                    "type": "example_api.events",
                    "overrides": {
                        "fixture_records": [
                            {
                                "id": "e1",
                                "occurred_at": "2026-08-06T12:00:00Z",
                                "severity": "low",
                                "state": "TX",
                                "status": "1",
                            }
                        ]
                    },
                },
                "schema": "schemas/example_api/events/events.schema.yaml",
                "destination": dest,
            }
        ),
        encoding="utf-8",
    )
    return pipe


def _draft(**kwargs) -> ReceiptDraft:
    started = kwargs.pop("started_at", datetime(2026, 8, 16, 12, 0, tzinfo=UTC))
    return ReceiptDraft(
        attempt_id=kwargs.pop("attempt_id", "abcd1234efgh5678"),
        started_at=started,
        pipeline=kwargs.pop("pipeline", "example_api.events"),
        command=kwargs.pop("command", "extract"),
        interval_start=kwargs.pop("interval_start", "2026-08-06T00:00:00+00:00"),
        interval_end=kwargs.pop("interval_end", "2026-08-07T00:00:00+00:00"),
        extract_run_datetime=kwargs.pop("extract_run_datetime", None),
        wire_version=kwargs.pop("wire_version", 1),
        destination=kwargs.pop("destination", "filesystem"),
        owner=kwargs.pop("owner", "pid:1"),
    )


def _put(
    lake: LakeRef,
    *,
    dt: str,
    pipeline: str,
    filename: str,
    body: dict,
) -> None:
    path = lake / "runs" / f"dt={dt}" / pipeline / filename
    path.write_text(json.dumps(body) + "\n", encoding="utf-8")


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (HttpIntegrityError("len"), "integrity_error"),
        (HttpError("404"), "http_error"),
        (LeaseHeldError("held"), "lease_held"),
        (LeaseFencedError("lost"), "lease_fenced"),
        (SecretNotSetError("missing"), "secret_not_set"),
        (CoerceError("bad"), "schema_invalid"),
        (JsonSchemaValidationError("nope", validator="type", schema={}), "schema_invalid"),
        (FileNotFoundError("raw"), "raw_missing"),
        (FileExistsError("exists"), "raw_exists"),
        (ValueError("yaml"), "config_invalid"),
        (RuntimeError("boom"), "unknown"),
    ],
)
def test_classify_error_taxonomy(exc, code):
    got, cls, _msg = classify_error(exc)
    assert got == code
    assert cls == type(exc).__name__


def test_receipts_enabled_opt_out():
    assert receipts_enabled(env={}) is True
    assert receipts_enabled(env={"DET_RUN_RECEIPTS": "0"}) is False
    assert receipts_enabled(env={"DET_RUN_RECEIPTS": "false"}) is False


def test_sum_artifact_bytes_ignores_junk():
    assert (
        sum_artifact_bytes([{"bytes": 10}, {"bytes": "2"}, "x", {"bytes": None}]) == 12
    )


def test_write_receipt_memory_lake_one_object():
    lake = open_lake("memory://receipts", Path("/tmp"))
    draft = _draft()
    draft.artifacts = 2
    draft.raw_bytes = 40
    path = write_receipt(lake, draft)
    assert path is not None
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["receipt_version"] == 1
    assert payload["lake_layout"] == 1
    assert payload["status"] == "ok"
    assert payload["artifacts"] == 2
    assert payload["raw_bytes"] == 40
    assert payload["destination"] == "filesystem"
    assert "dt=2026-08-16" in str(path)
    assert payload["duration_ms"] >= 0
    listed = list_receipts(
        lake,
        now=datetime(2026, 8, 16, 18, 0, tzinfo=UTC),
        since="2026-08-16",
        until="2026-08-17",
    )
    assert len(listed) == 1
    assert listed[0]["attempt_id"] == draft.attempt_id


def test_write_receipt_scrubs_dsn_from_error_message():
    dsn = "postgresql://det:hunter2pw@db.internal:5432/det"
    register_secret_value(dsn)
    lake = open_lake("memory://scrub", Path("/tmp"))
    draft = _draft(destination="postgres")
    path = write_receipt(lake, draft, error=RuntimeError(f'connection failed for "{dsn}"'))
    text = path.read_text(encoding="utf-8")
    assert "hunter2pw" not in text
    assert dsn not in text
    payload = json.loads(text)
    assert payload["destination"] == "postgres"
    assert payload["status"] == "error"
    assert payload["error_code"] == "unknown"
    assert "connection" not in payload


def test_write_receipt_disabled_writes_nothing(monkeypatch):
    monkeypatch.setenv("DET_RUN_RECEIPTS", "0")
    lake = open_lake("memory://off", Path("/tmp"))
    assert write_receipt(lake, _draft()) is None
    assert list(list_receipts(lake, since="2026-08-16", until="2026-08-17")) == []


def test_reader_filters_bounds_and_limit():
    lake = open_lake("memory://list", Path("/tmp"))
    now = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
    _put(
        lake,
        dt="2026-08-01",
        pipeline="old.pipe",
        filename="extract__a__1.json",
        body={
            "pipeline": "old.pipe",
            "command": "extract",
            "status": "ok",
            "started_at": "2026-08-01T00:00:00+00:00",
            "duration_ms": 5,
        },
    )
    _put(
        lake,
        dt="2026-08-16",
        pipeline="example_api.events",
        filename="extract__b__2.json",
        body={
            "pipeline": "example_api.events",
            "command": "extract",
            "status": "error",
            "error_code": "http_error",
            "started_at": "2026-08-16T10:00:00+00:00",
            "duration_ms": 10,
        },
    )
    _put(
        lake,
        dt="2026-08-16",
        pipeline="example_api.events",
        filename="load__b__3.json",
        body={
            "pipeline": "example_api.events",
            "command": "load",
            "status": "ok",
            "started_at": "2026-08-16T11:00:00+00:00",
            "duration_ms": 20,
            "rows": 4,
        },
    )
    _put(
        lake,
        dt="2026-08-16",
        pipeline="noaa.storm_events",
        filename="extract__c__4.json",
        body={
            "pipeline": "noaa.storm_events",
            "command": "extract",
            "status": "ok",
            "started_at": "2026-08-16T09:00:00+00:00",
            "duration_ms": 30,
        },
    )
    bounded = list_receipts(
        lake, since="2026-08-16", until="2026-08-17", now=now
    )
    assert {r["pipeline"] for r in bounded} == {
        "example_api.events",
        "noaa.storm_events",
    }
    assert all(r["started_at"].startswith("2026-08-16") for r in bounded)
    assert bounded[0]["started_at"] >= bounded[-1]["started_at"]

    filtered = list_receipts(
        lake,
        pipeline="example_api.events",
        command="extract",
        status="error",
        since="2026-08-16",
        until="2026-08-17",
        now=now,
    )
    assert len(filtered) == 1
    assert filtered[0]["error_code"] == "http_error"

    capped = list_receipts(
        lake, since="2026-08-16", until="2026-08-17", limit=1, now=now
    )
    assert len(capped) == 1
    assert capped[0]["started_at"] == "2026-08-16T11:00:00+00:00"


def test_summarize_percentiles_and_error_codes():
    lake = open_lake("memory://sum", Path("/tmp"))
    now = datetime(2026, 8, 16, tzinfo=UTC)
    for i, ms in enumerate((10, 20, 30, 40, 100), start=1):
        status = "error" if i == 5 else "ok"
        _put(
            lake,
            dt="2026-08-16",
            pipeline="example_api.events",
            filename=f"extract__x__{i}.json",
            body={
                "pipeline": "example_api.events",
                "command": "extract",
                "status": status,
                "error_code": "http_error" if status == "error" else None,
                "started_at": f"2026-08-16T0{i}:00:00+00:00",
                "duration_ms": ms,
                "rows": 2 if status == "ok" else 0,
            },
        )
    summary = summarize_receipts(
        lake, since="2026-08-16", until="2026-08-17", now=now
    )
    group = summary["groups"][0]
    assert group["attempts"] == 5
    assert group["ok"] == 4
    assert group["error"] == 1
    assert group["error_codes"] == {"http_error": 1}
    assert group["p50_ms"] == 30
    assert group["p95_ms"] == 100
    assert group["rows"] == 8


def test_attempt_window_defaults_to_seven_days():
    now = datetime(2026, 8, 16, 15, 0, tzinfo=UTC)
    start, end = attempt_window(now=now)
    assert start.isoformat() == "2026-08-10"
    assert end.isoformat() == "2026-08-17"


def test_extract_success_receipt(project_root: Path, tmp_path: Path):
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    result = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    rows = list_receipts(lake, pipeline="example_api.events", command="extract")
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "ok"
    assert row["artifacts"] == result.artifacts
    assert row["raw_bytes"] > 0
    assert row["duration_ms"] >= 0
    assert row["destination"] == "filesystem"
    assert "dt=" in row["path"]
    assert row["owner"]


def test_load_success_receipt(project_root: Path, tmp_path: Path):
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    rows = list_receipts(lake, pipeline="example_api.events", command="load")
    assert len(rows) == 1
    row = rows[0]
    assert row["status"] == "ok"
    assert row["rows"] == loaded.rows == 1
    assert row["schema_sha256"]
    assert row["duration_ms"] >= 0
    assert row["extract_run_datetime"] == extracted.extract_run_datetime


def test_run_emits_extract_and_load_not_a_third_receipt(
    project_root: Path, tmp_path: Path
):
    pipe = _example_pipe(tmp_path, project_root)
    PipelineRunner(tmp_path).run(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    rows = list_receipts(lake, pipeline="example_api.events")
    assert sorted(r["command"] for r in rows) == ["extract", "load"]


def test_failed_extract_receipt_survives_rmtree(project_root: Path, tmp_path: Path):
    pipe = _example_pipe(tmp_path, project_root)

    def boom(self, *, config, interval, data_dir):
        (data_dir / "partial.bin").write_bytes(b"truncated")
        raise HttpError("download failed")

    runner = PipelineRunner(tmp_path)
    with (
        patch.object(ExampleApiSource, "extract_to_raw", boom),
        pytest.raises(DetPluginError, match="download failed"),
    ):
        runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    raw_root = tmp_path / "lake" / "raw"
    assert list(raw_root.rglob("manifest.json")) == []
    assert list(raw_root.rglob("partial.bin")) == []
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    rows = list_receipts(lake, pipeline="example_api.events")
    assert len(rows) == 1
    assert rows[0]["status"] == "error"
    assert rows[0]["error_code"] == "http_error"
    assert rows[0]["error_class"] == "DetPluginError"


def test_lease_held_receipt_wrapper_is_outside_lease(
    project_root: Path, tmp_path: Path
):
    pipe = _example_pipe(tmp_path, project_root)
    config = load_pipeline_config(pipe)
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    start, end = resolve_interval("2026-08-06", "2026-08-07")
    caught: list[BaseException] = []

    def other() -> None:
        try:
            PipelineRunner(tmp_path).extract(
                pipe, interval_start="2026-08-06", interval_end="2026-08-07"
            )
        except BaseException as exc:
            caught.append(exc)

    with pipeline_lease(
        lake,
        pipeline=config.name,
        interval_start=start,
        interval_end=end,
        command="extract",
    ):
        t = threading.Thread(target=other)
        t.start()
        t.join()
    assert caught
    assert isinstance(caught[0], LeaseHeldError)
    rows = list_receipts(lake, pipeline=config.name)
    assert any(r.get("error_code") == "lease_held" for r in rows)


def test_raising_writer_does_not_fail_the_run(project_root: Path, tmp_path: Path):
    pipe = _example_pipe(tmp_path, project_root)
    orig = LakeRef.write_text

    def boom(self, data: str, encoding: str = "utf-8") -> None:
        if "/runs/" in str(self).replace("\\", "/"):
            raise OSError("disk full")
        orig(self, data, encoding=encoding)

    with patch.object(LakeRef, "write_text", boom):
        result = PipelineRunner(tmp_path).extract(
            pipe, interval_start="2026-08-06", interval_end="2026-08-07"
        )
    assert result.artifacts >= 1
    assert is_committed_raw_dir(result.raw_dir)
    runs = tmp_path / "lake" / "runs"
    assert not runs.exists() or list(runs.rglob("*.json")) == []


def test_opt_out_writes_nothing(project_root: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DET_RUN_RECEIPTS", "0")
    pipe = _example_pipe(tmp_path, project_root)
    PipelineRunner(tmp_path).extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    assert list((tmp_path / "lake" / "runs").rglob("*.json")) == []


def test_postgres_destination_receipt_has_no_dsn(
    project_root: Path, tmp_path: Path, monkeypatch
):
    dsn = "postgresql://det:hunter2pw@db.internal:5432/det"
    monkeypatch.setenv("DET_POSTGRES_DSN", dsn)
    register_secret_value(dsn)
    pipe = _example_pipe(
        tmp_path,
        project_root,
        destination={
            "type": "postgres",
            "path": str(tmp_path / "lake"),
            "connection_env": "DET_POSTGRES_DSN",
        },
    )
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )

    def boom(*args, **kwargs):
        raise RuntimeError(f'connection failed for "{dsn}"')

    with (
        patch.object(DetBackend, "write", boom),
        pytest.raises(DetPluginError, match="connection failed"),
    ):
        runner.load(
            pipe,
            interval_start=extracted.interval_start,
            interval_end=extracted.interval_end,
            extract_run_datetime=extracted.extract_run_datetime,
        )
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    rows = list_receipts(lake, pipeline="example_api.events")
    blob = json.dumps(rows)
    assert "hunter2pw" not in blob
    assert dsn not in blob
    extract_row = next(r for r in rows if r["command"] == "extract")
    load_row = next(r for r in rows if r["command"] == "load")
    assert extract_row["destination"] == "postgres"
    assert load_row["destination"] == "postgres"
    assert load_row["status"] == "error"
    assert "connection" not in load_row


def test_record_attempt_still_writes_on_success_and_error():
    lake = open_lake("memory://ctx", Path("/tmp"))
    with record_attempt(
        lake,
        pipeline="example_api.events",
        command="extract",
        interval_start="2026-08-06T00:00:00+00:00",
        interval_end="2026-08-07T00:00:00+00:00",
        destination="filesystem",
    ) as receipt:
        receipt.artifacts = 1
        receipt.raw_bytes = 8
    with pytest.raises(FileNotFoundError):
        with record_attempt(
            lake,
            pipeline="example_api.events",
            command="load",
            interval_start="2026-08-06T00:00:00+00:00",
            interval_end="2026-08-07T00:00:00+00:00",
            destination="filesystem",
        ):
            raise FileNotFoundError("missing raw")
    rows = list_receipts(lake, since=datetime.now(UTC).date().isoformat())
    assert {r["command"] for r in rows} == {"extract", "load"}
    assert {r["status"] for r in rows} == {"ok", "error"}
    err = next(r for r in rows if r["status"] == "error")
    assert err["error_code"] == "raw_missing"


def test_cli_runs_json(project_root: Path, tmp_path: Path):
    import structlog
    from typer.testing import CliRunner

    from det.cli import app
    from det.logging import configure_logging

    pipe = _example_pipe(tmp_path, project_root)
    PipelineRunner(tmp_path).extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    runner = CliRunner()
    try:
        result = runner.invoke(
            app,
            [
                "runs",
                "-p",
                "example_api.events",
                "--project-root",
                str(tmp_path),
                "--json",
            ],
        )
        summary = runner.invoke(
            app,
            [
                "runs",
                "-p",
                "example_api.events",
                "--project-root",
                str(tmp_path),
                "--summary",
                "--json",
            ],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert rows[0]["command"] == "extract"
    assert summary.exit_code == 0, summary.output
    payload = json.loads(summary.stdout)
    assert payload["groups"][0]["ok"] == 1


def test_cli_runs_human_table(project_root: Path, tmp_path: Path):
    import structlog
    from typer.testing import CliRunner

    from det.cli import app
    from det.logging import configure_logging

    pipe = _example_pipe(tmp_path, project_root)
    PipelineRunner(tmp_path).extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    runner = CliRunner()
    try:
        listed = runner.invoke(
            app,
            [
                "runs",
                "-p",
                "example_api.events",
                "--project-root",
                str(tmp_path),
            ],
        )
        verbose = runner.invoke(
            app,
            [
                "runs",
                "-p",
                "example_api.events",
                "--project-root",
                str(tmp_path),
                "--verbose",
            ],
        )
        summary = runner.invoke(
            app,
            [
                "runs",
                "-p",
                "example_api.events",
                "--project-root",
                str(tmp_path),
                "--summary",
            ],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")

    assert listed.exit_code == 0, listed.output
    assert "STATUS  COMMAND  DURATION  STARTED" in listed.stdout
    assert "OK      extract" in listed.stdout
    assert "duration_ms=" not in listed.stdout
    assert "PIPELINE" not in listed.stdout

    assert verbose.exit_code == 0, verbose.output
    assert "Owner:" in verbose.stdout
    assert "Interval:" in verbose.stdout
    assert "Attempt ID:" in verbose.stdout

    assert summary.exit_code == 0, summary.output
    assert "Attempt window:" in summary.stdout
    assert "COMMAND  ATTEMPTS  OK  ERRORS  P50" in summary.stdout
    assert "extract" in summary.stdout


def test_human_run_output_shows_error_detail(capsys):
    from det.cli import _print_run_list

    _print_run_list(
        [
            {
                "status": "error",
                "command": "load",
                "pipeline": "example_api.events",
                "duration_ms": 4120,
                "started_at": "2026-08-17T10:04:02+00:00",
                "error_code": "schema_invalid",
                "error_message": "'severity' is a required property",
                "error_class": "ValidationError",
                "owner": "airflow:det_extract_bronze:manual__2026-08-17",
                "destination": "postgres",
                "interval_start": "2026-08-06T00:00:00+00:00",
                "interval_end": "2026-08-07T00:00:00+00:00",
                "extract_run_datetime": "2026-08-17T10:03:58+00:00",
                "attempt_id": "abc123",
            }
        ],
        include_pipeline=True,
        verbose=True,
    )
    output = capsys.readouterr().out
    assert "ERROR   load     example_api.events" in output
    assert "4.1s" in output
    assert "schema_invalid: 'severity' is a required property" in output
    assert "Error class: ValidationError" in output
