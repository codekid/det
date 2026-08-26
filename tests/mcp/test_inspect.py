from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.ingestion.duckdb_writer import write_duckdb_table
from det.mcp.context import PathSandboxError
from det.mcp.inspect import (
    MAX_SAMPLE_LIMIT,
    clamp_sample_limit,
    diagnose_pipeline,
    diff_partitions,
    sample_bronze,
    sample_raw,
    validate_sample,
)
from det.mcp.server import create_server
from det.runtime.meta import to_partition_value


def _write_pipeline(
    root: Path,
    canonical: str = "example_api.events",
    *,
    destination: dict | None = None,
    schema_props: dict | None = None,
) -> Path:
    provider, source = canonical.split(".", 1)
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True, exist_ok=True)
    schema_rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
    schema_path = root / schema_rel
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    props = schema_props or {
        "id": {"type": "integer"},
        "event_name": {"type": "string"},
    }
    schema_path.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["id"],
                "properties": props,
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    path = pipe_dir / f"{source}.yaml"
    dest = destination or {"type": "filesystem", "path": "./data/lake"}
    path.write_text(
        yaml.safe_dump(
            {
                "name": canonical,
                "source": {"type": canonical},
                "schema": schema_rel,
                "ingestion": {"library": "thin"},
                "destination": dest,
                "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
            }
        ),
        encoding="utf-8",
    )
    return path


def _mk_hive_run(base: Path, *, start: str, end: str, run: str) -> Path:
    run_dir = (
        base
        / f"__interval_start_datetime={to_partition_value(start)}"
        / f"__interval_end_datetime={to_partition_value(end)}"
        / f"__extract_run_datetime={to_partition_value(run)}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "data").mkdir(exist_ok=True)
    (run_dir / "meta").mkdir(exist_ok=True)
    (run_dir / "meta" / "manifest.json").write_text(
        json.dumps({"extract_run_datetime": run}),
        encoding="utf-8",
    )
    return run_dir


def _write_example_raw(
    root: Path,
    *,
    start: str,
    end: str,
    run: str,
    events: list[dict],
) -> Path:
    raw_base = root / "data" / "lake" / "raw" / "example_api" / "events_v1"
    run_dir = _mk_hive_run(raw_base, start=start, end=end, run=run)
    page = run_dir / "data" / "pages" / "0001.json"
    page.parent.mkdir(parents=True, exist_ok=True)
    body = {"data": {"events": events}}
    page.write_text(json.dumps(body), encoding="utf-8")
    rel = page.relative_to(run_dir).as_posix()
    (run_dir / "meta" / "manifest.json").write_text(
        json.dumps(
            {
                "source": "example_api.events",
                "interval_start": start,
                "interval_end": end,
                "extract_run_datetime": run,
                "artifacts": [
                    {
                        "path": rel,
                        "origin": "fixture_records",
                        "format": "json_page",
                        "format_check": "ok",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def test_clamp_sample_limit():
    assert clamp_sample_limit(None) == 5
    assert clamp_sample_limit(0) == 1
    assert clamp_sample_limit(999) == MAX_SAMPLE_LIMIT
    assert clamp_sample_limit(10) == 10


def test_diff_partitions_filesystem(tmp_path: Path):
    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    raw = tmp_path / "data" / "lake" / "raw" / "example_api" / "events_v1"
    bronze = tmp_path / "data" / "lake" / "bronze" / "example_api" / "events_v1"
    _mk_hive_run(raw, start=start, end=end, run="2026-08-06T10:00:00+00:00")
    _mk_hive_run(raw, start=start, end=end, run="2026-08-06T11:00:00+00:00")
    _mk_hive_run(bronze, start=start, end=end, run="2026-08-06T10:00:00+00:00")
    _mk_hive_run(bronze, start=start, end=end, run="2026-08-06T12:00:00+00:00")

    diff = diff_partitions("example_api.events", root=tmp_path)
    assert diff["only_raw_count"] == 1
    assert diff["only_bronze_count"] == 1
    assert diff["both_count"] == 1
    assert diff["only_raw"][0]["extract_run_datetime"].startswith("2026-08-06T11:00:00")
    assert diff["only_bronze"][0]["extract_run_datetime"].startswith("2026-08-06T12:00:00")


def test_diff_partitions_duckdb(tmp_path: Path):
    db = tmp_path / "data" / "analytics.duckdb"
    _write_pipeline(
        tmp_path,
        destination={
            "type": "duckdb",
            "path": "./data/lake",
            "connection": "./data/analytics.duckdb",
            "dataset": "bronze",
        },
    )
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    raw = tmp_path / "data" / "lake" / "raw" / "example_api" / "events_v1"
    _mk_hive_run(raw, start=start, end=end, run="2026-08-06T10:00:00+00:00")
    _mk_hive_run(raw, start=start, end=end, run="2026-08-06T11:00:00+00:00")
    write_duckdb_table(
        [
            {
                "id": 1,
                "__interval_start_datetime": start,
                "__interval_end_datetime": end,
                "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
            }
        ],
        connection_path=db,
        schema="bronze_example_api",
        table="events_v1",
        json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
    )
    diff = diff_partitions("example_api.events", root=tmp_path)
    assert diff["destination_type"] == "duckdb"
    assert diff["only_raw_count"] == 1
    assert diff["both_count"] == 1
    assert diff["only_raw"][0]["extract_run_datetime"].startswith("2026-08-06T11:00:00")


def test_sample_raw_and_validate(tmp_path: Path):
    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    run = "2026-08-06T10:00:00+00:00"
    run_dir = _write_example_raw(
        tmp_path,
        start=start,
        end=end,
        run=run,
        events=[
            {"id": 1, "eventName": "alpha"},
            {"id": 2, "eventName": "beta"},
            {"id": 3, "eventName": "gamma"},
        ],
    )
    wire = sample_raw(
        "example_api.events",
        stage="wire",
        limit=2,
        run_path=str(run_dir.relative_to(tmp_path)),
        root=tmp_path,
    )
    assert wire["limit"] == 2
    assert wire["rows"]

    named = sample_raw(
        "example_api.events",
        stage="named",
        limit=2,
        interval_start=start,
        root=tmp_path,
    )
    assert len(named["rows"]) == 2
    assert named["truncated"] is True
    assert "event_name" in named["rows"][0]["data"]

    ok = validate_sample(
        "example_api.events",
        limit=10,
        run_path=str(run_dir.relative_to(tmp_path)),
        root=tmp_path,
    )
    assert ok["ok"] is True
    assert ok["rows_checked"] == 3

    # Bad type → coerce/schema errors as data
    bad_dir = _write_example_raw(
        tmp_path,
        start=start,
        end=end,
        run="2026-08-06T11:00:00+00:00",
        events=[{"id": "not-an-int", "eventName": "x"}],
    )
    bad = validate_sample(
        "example_api.events",
        limit=5,
        run_path=str(bad_dir.relative_to(tmp_path)),
        root=tmp_path,
    )
    assert bad["ok"] is False
    assert bad["coerce_errors"] or bad["schema_errors"]


def test_sample_raw_wire_oserror_does_not_leak_abs_path(tmp_path: Path, monkeypatch):
    """Wire-sample OSError messages must not expose absolute filesystem paths."""
    from det.mcp.inspect import _sample_wire

    data = tmp_path / "run" / "data"
    data.mkdir(parents=True)
    wire_file = data / "payload.json"
    wire_file.write_text('{"ok": true}\n', encoding="utf-8")
    leak = str(tmp_path / "secret" / "creds.db")
    orig = Path.read_text

    def selective(self, *args, **kwargs):
        if self.name == "payload.json":
            raise OSError(f"[Errno 2] No such file or directory: '{leak}'")
        return orig(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", selective)
    peeks, _ = _sample_wire(tmp_path / "run", root=tmp_path, limit=5)
    blob = json.dumps(peeks)
    assert leak not in blob
    assert str(tmp_path) not in blob
    assert peeks and "<path>" in peeks[0]["error"]


def test_diagnose_manifest_error_is_sanitized(tmp_path: Path, monkeypatch):
    """Manifest read failures in diagnose must not leak absolute paths."""
    from det.mcp import inspect as inspect_mod

    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    run = "2026-08-06T10:00:00+00:00"
    _write_example_raw(
        tmp_path,
        start=start,
        end=end,
        run=run,
        events=[{"id": 1, "eventName": "alpha"}],
    )
    leak = str(tmp_path / "hidden" / "lake" / "manifest.json")

    def boom(*args, **kwargs):
        raise OSError(f"Permission denied: {leak}")

    monkeypatch.setattr(inspect_mod, "read_raw_manifest", boom)
    report = diagnose_pipeline("example_api.events", root=tmp_path)
    blob = json.dumps(report)
    assert leak not in blob
    evidence = report.get("evidence") or {}
    if "manifest_error" in evidence:
        assert leak not in evidence["manifest_error"]
        assert "<path>" in evidence["manifest_error"]


def test_unreachable_note_sanitizes_exception_paths():
    from det.mcp.airflow_inspect import _unreachable_note

    note = _unreachable_note(
        "http://localhost:8080",
        OSError("Connection failed reading /Users/alice/dev/secret/token"),
    )
    assert "/Users/alice" not in note
    assert "<path>" in note


def test_sample_bronze_filesystem(tmp_path: Path):
    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    bronze = tmp_path / "data" / "lake" / "bronze" / "example_api" / "events_v1"
    run_dir = _mk_hive_run(bronze, start=start, end=end, run="2026-08-06T10:00:00+00:00")
    (run_dir / "data.jsonl").write_text(
        json.dumps({"id": 1, "__extract_run_datetime": "2026-08-06T10:00:00+00:00"})
        + "\n"
        + json.dumps({"id": 2, "__extract_run_datetime": "2026-08-06T10:00:00+00:00"})
        + "\n",
        encoding="utf-8",
    )
    sample = sample_bronze("example_api.events", limit=1, root=tmp_path)
    assert sample["limit"] == 1
    assert len(sample["rows"]) == 1
    assert sample["truncated"] is True
    assert sample["rows"][0]["data"]["id"] == 1
    assert "migrate" in sample["note"].lower()


def test_sample_bronze_duckdb(tmp_path: Path):
    db = tmp_path / "data" / "analytics.duckdb"
    _write_pipeline(
        tmp_path,
        destination={
            "type": "duckdb",
            "path": "./data/lake",
            "connection": "./data/analytics.duckdb",
            "dataset": "bronze",
        },
    )
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    write_duckdb_table(
        [
            {
                "id": 1,
                "__interval_start_datetime": start,
                "__interval_end_datetime": end,
                "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
            },
            {
                "id": 2,
                "__interval_start_datetime": start,
                "__interval_end_datetime": end,
                "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
            },
        ],
        connection_path=db,
        schema="bronze_example_api",
        table="events_v1",
        json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
    )
    sample = sample_bronze("example_api.events", limit=1, root=tmp_path)
    assert sample["destination_type"] == "duckdb"
    assert len(sample["rows"]) == 1
    assert sample["truncated"] is True


class _FakePgCursor:
    def __init__(self, conn):
        self._conn = conn
        self._rows: list[tuple] = []
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self._conn.statements.append(sql)
        if "information_schema.tables" in sql:
            self._rows = [(1,)]
            self.description = None
            return
        self._rows = [(1, "2026-08-06T10:00:00+00:00")]
        self.description = [
            type("Col", (), {"name": "id"}),
            type("Col", (), {"name": "__extract_run_datetime"}),
        ]

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakePgConn:
    def __init__(self, dsn):
        self.dsn = dsn
        self.statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return _FakePgCursor(self)


def _install_fake_psycopg(monkeypatch) -> list[str]:
    import sys
    import types

    seen: list[str] = []
    module = types.ModuleType("psycopg")

    def connect(dsn, **_kwargs):
        seen.append(dsn)
        return _FakePgConn(dsn)

    module.connect = connect
    monkeypatch.setitem(sys.modules, "psycopg", module)
    return seen


def test_sample_bronze_postgres_resolves_connection_env(monkeypatch, tmp_path: Path):
    seen = _install_fake_psycopg(monkeypatch)
    dsn = "postgresql://det:hunter2pw@db.internal/det"
    monkeypatch.setenv("DET_POSTGRES_DSN", dsn)
    _write_pipeline(
        tmp_path,
        destination={
            "type": "postgres",
            "path": "./data/lake",
            "connection_env": "DET_POSTGRES_DSN",
            "dataset": "bronze",
        },
    )
    sample = sample_bronze("example_api.events", limit=5, root=tmp_path)
    assert seen == [dsn]
    assert sample["destination_type"] == "postgres"
    assert sample["rows"][0]["data"]["id"] == 1
    assert "hunter2pw" not in json.dumps(sample)


def test_sample_bronze_postgres_reports_an_unset_secret(monkeypatch, tmp_path: Path):
    _install_fake_psycopg(monkeypatch)
    monkeypatch.delenv("DET_POSTGRES_DSN", raising=False)
    _write_pipeline(
        tmp_path,
        destination={
            "type": "postgres",
            "path": "./data/lake",
            "connection_env": "DET_POSTGRES_DSN",
            "dataset": "bronze",
        },
    )
    sample = sample_bronze("example_api.events", limit=5, root=tmp_path)
    assert sample["rows"] == []
    assert "DET_POSTGRES_DSN" in sample["errors"][0]["message"]


def test_sample_bronze_iceberg(tmp_path: Path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from det.destinations.models import bronze_dataset_dir, lake_root
    from det.ingestion.iceberg_writer import write_iceberg_table
    from det.mcp.inspect import list_bronze_runs
    from det.runtime.config import load_pipeline_config

    _write_pipeline(tmp_path, destination={"type": "iceberg", "path": "./data/lake"})
    config = load_pipeline_config(
        tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    )
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    write_iceberg_table(
        [
            {
                "id": 1,
                "event_name": "alpha",
                "__row_hash": "a",
                "__filename": "x.json",
                "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
                "__bronze_loaded_at": "2026-08-06T10:00:01+00:00",
                "__interval_start_datetime": start,
                "__interval_end_datetime": end,
                "__data_interval_date": "2026-08-06",
            },
            {
                "id": 2,
                "event_name": "beta",
                "__row_hash": "b",
                "__filename": "x.json",
                "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
                "__bronze_loaded_at": "2026-08-06T10:00:01+00:00",
                "__interval_start_datetime": start,
                "__interval_end_datetime": end,
                "__data_interval_date": "2026-08-06",
            },
        ],
        lake=lake_root(config.destination, tmp_path),
        table_location=bronze_dataset_dir(config, tmp_path),
        namespace="bronze_example_api",
        table="events_v1",
        json_schema={
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "event_name": {"type": "string"},
            },
        },
    )
    sample = sample_bronze("example_api.events", limit=1, root=tmp_path)
    assert sample["destination_type"] == "iceberg"
    assert len(sample["rows"]) == 1
    assert sample["truncated"] is True
    assert sample["rows"][0]["data"]["id"] == 1
    runs, note = list_bronze_runs(config, root=tmp_path, limit=10)
    assert note is None
    assert len(runs) == 1
    assert runs[0]["extract_run_datetime"] == "2026-08-06T10:00:00+00:00"


def test_diagnose_raw_ahead(tmp_path: Path):
    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    _write_example_raw(
        tmp_path,
        start=start,
        end=end,
        run="2026-08-06T10:00:00+00:00",
        events=[{"id": 1, "eventName": "alpha"}],
    )
    report = diagnose_pipeline("example_api.events", root=tmp_path)
    codes = {f["code"] for f in report["findings"]}
    assert "raw_without_bronze" in codes
    assert any("det load" in c for c in report["suggested_commands"])
    assert "raw ahead" in report["summary"]


def test_sample_raw_rejects_escape(tmp_path: Path):
    _write_pipeline(tmp_path)
    with pytest.raises(PathSandboxError):
        sample_raw(
            "example_api.events",
            run_path=str(tmp_path.parent / "nope"),
            root=tmp_path,
        )


def test_create_server_registers_inspect_tools():
    server = create_server()
    names = sorted(server._tool_manager._tools)
    for expected in (
        "diff_partitions",
        "sample_raw",
        "validate_sample",
        "sample_bronze",
        "diagnose_pipeline",
    ):
        assert expected in names
