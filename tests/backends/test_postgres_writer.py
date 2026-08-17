from __future__ import annotations

import builtins
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from pydantic import ValidationError

from det.ingestion.det_backend import DetBackend
from det.ingestion.postgres_writer import write_postgres_table
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.secrets import SecretNotSetError

_EVENT_SCHEMA = {
    "type": "object",
    "properties": {"event_id": {"type": ["integer", "null"]}},
}

_COL_DEF = re.compile(
    r'"([^"]+)"\s+((?:DOUBLE PRECISION)|(?:[A-Z][A-Z0-9]*))'
)


def _install_fake_pg(monkeypatch, calls: list):
    live: list[tuple[str, str]] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            text = " ".join(str(sql).split())
            calls.append(("execute", text, params))
            self._result: list[tuple[str, str]] = []
            lower = text.lower()
            if "information_schema.columns" in lower:
                self._result = list(live)
            elif lower.startswith("create table"):
                start, end = text.find("("), text.rfind(")")
                live[:] = [
                    (m.group(1), m.group(2))
                    for m in _COL_DEF.finditer(text[start:end])
                ]
            elif "add column" in lower:
                match = re.search(
                    r'ADD COLUMN "([^"]+)" (.+)$', text, re.IGNORECASE
                )
                if match:
                    live.append((match.group(1), match.group(2).strip()))

        def fetchall(self):
            return list(self._result)

        def executemany(self, sql, rows):
            calls.append(("executemany", " ".join(str(sql).split()), rows))

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append(("commit", None, None))

        def rollback(self):
            calls.append(("rollback", None, None))

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            return FakeConn()

    monkeypatch.setattr(
        "det.ingestion.postgres_writer._import_psycopg", lambda: FakePsycopg
    )
    return live


def test_postgres_destination_requires_connection():
    with pytest.raises(ValidationError, match="destination.connection_env"):
        DestinationConfig(type="postgres", path="./data/lake")


def test_connection_and_connection_env_together_are_rejected():
    with pytest.raises(ValidationError, match="not both"):
        DestinationConfig(
            type="postgres",
            connection="postgresql://db/det",
            connection_env="DET_POSTGRES_DSN",
        )


def test_connection_env_is_postgres_only():
    with pytest.raises(ValidationError, match="only supported"):
        DestinationConfig(type="duckdb", connection_env="DET_POSTGRES_DSN")


def test_connection_env_must_be_a_name_not_a_dsn():
    with pytest.raises(ValidationError, match="env var name"):
        DestinationConfig(
            type="postgres", connection_env="postgresql://det:pw@db/det"
        )


def test_backend_resolves_dsn_from_connection_env(monkeypatch, tmp_path: Path):
    dsn = "postgresql://det:hunter2pw@db.internal/det"
    monkeypatch.setenv("DET_POSTGRES_DSN", dsn)
    dest = DestinationConfig(
        type="postgres",
        path=str(tmp_path / "lake"),
        connection_env="DET_POSTGRES_DSN",
        dataset="bronze",
    )
    schema_file = tmp_path / "event.schema.yaml"
    schema_file.write_text(
        "type: object\nproperties:\n  event_id: {type: integer}\n", encoding="utf-8"
    )
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="event.schema.yaml",
        ingestion=IngestionConfig(library="det"),
        destination=dest,
        medallion=MedallionConfig(),
    )
    records = [
        {
            "event_id": 1,
            "__row_hash": "abc",
            "__extract_run_datetime": "2026-08-06T15:04:05+00:00",
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
        }
    ]
    with patch("det.ingestion.det_backend.write_postgres_table") as write:
        out = DetBackend().write(
            records,
            config=config,
            project_root=tmp_path,
            partition_dir=tmp_path / "unused",
            destination=dest,
        )
    assert write.call_args.kwargs["dsn"] == dsn
    # Identity is logical, never the DSN.
    assert str(out) == "postgres/bronze_noaa/storm_events_v1"


def test_unset_connection_env_fails_the_write(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("DET_POSTGRES_DSN", raising=False)
    dest = DestinationConfig(
        type="postgres",
        path=str(tmp_path / "lake"),
        connection_env="DET_POSTGRES_DSN",
        dataset="bronze",
    )
    schema_file = tmp_path / "event.schema.yaml"
    schema_file.write_text(
        "type: object\nproperties:\n  event_id: {type: integer}\n", encoding="utf-8"
    )
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="event.schema.yaml",
        ingestion=IngestionConfig(library="det"),
        destination=dest,
        medallion=MedallionConfig(),
    )
    with pytest.raises(SecretNotSetError, match="DET_POSTGRES_DSN"):
        DetBackend().write(
            [],
            config=config,
            project_root=tmp_path,
            partition_dir=tmp_path / "unused",
            destination=dest,
        )


def test_det_backend_writes_postgres_via_helper(tmp_path: Path):
    dest = DestinationConfig(
        type="postgres",
        path=str(tmp_path / "lake"),
        connection="postgresql://user:pass@localhost:5432/det",
        dataset="bronze",
    )
    schema_file = tmp_path / "event.schema.yaml"
    schema_file.write_text(
        "type: object\nproperties:\n  event_id: {type: integer}\n",
        encoding="utf-8",
    )
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="event.schema.yaml",
        ingestion=IngestionConfig(library="det"),
        destination=dest,
        medallion=MedallionConfig(),
    )
    records = [
        {
            "event_id": 1,
            "__row_hash": "abc",
            "__extract_run_datetime": "2026-08-06T15:04:05+00:00",
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
        }
    ]
    with patch("det.ingestion.det_backend.write_postgres_table") as write:
        write.return_value = dest.connection
        out = DetBackend().write(
            records,
            config=config,
            project_root=tmp_path,
            partition_dir=tmp_path / "unused",
            destination=dest,
        )
    write.assert_called_once()
    assert write.call_args.kwargs["schema"] == "bronze_noaa"
    assert write.call_args.kwargs["table"] == "storm_events_v1"
    assert out == Path("postgres") / "bronze_noaa" / "storm_events_v1"


def test_postgres_run_leaves_no_dsn_in_the_lake(
    monkeypatch, project_root: Path, tmp_path: Path
):
    """Raw bytes and DET sidecars must never carry the resolved credential."""
    from det.runtime.runner import PipelineRunner

    dsn = "postgresql://det:hunter2pw@db.internal/det"
    monkeypatch.setenv("DET_POSTGRES_DSN", dsn)
    schema_rel = "schemas/example_api/events/events.schema.yaml"
    schema_dst = tmp_path / schema_rel
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(
        (project_root / schema_rel).read_text(encoding="utf-8"), encoding="utf-8"
    )
    lake = tmp_path / "lake"
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
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
                "schema": schema_rel,
                "destination": {
                    "type": "postgres",
                    "path": str(lake),
                    "connection_env": "DET_POSTGRES_DSN",
                    "dataset": "bronze",
                },
            }
        ),
        encoding="utf-8",
    )

    with patch("det.ingestion.det_backend.write_postgres_table") as write:
        PipelineRunner(tmp_path).run(
            pipe, interval_start="2026-08-06", interval_end="2026-08-07"
        )
    assert write.call_args.kwargs["dsn"] == dsn

    landed = [p for p in lake.rglob("*") if p.is_file()]
    assert landed, "expected raw artifacts in the lake"
    for path in landed:
        assert "hunter2pw" not in path.read_text(encoding="utf-8", errors="ignore")


def test_write_postgres_table_import_error_message():
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "psycopg":
            raise ImportError("no psycopg")
        return real_import(name, *args, **kwargs)

    with (
        patch("builtins.__import__", side_effect=fake_import),
        pytest.raises(ImportError, match=r"\[postgres\]"),
    ):
        write_postgres_table(
            [{"id": 1}],
            dsn="postgresql://localhost/db",
            schema="bronze",
            table="t",
            json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
        )


def _records():
    return [
        {
            "event_id": 1,
            "__row_hash": "abc",
            "__extract_run_datetime": "2026-08-06T15:04:05+00:00",
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
        }
    ]


def test_write_postgres_table_deletes_then_inserts(monkeypatch):
    calls: list[tuple[str, str | None, object]] = []
    _install_fake_pg(monkeypatch, calls)
    write_postgres_table(
        _records(),
        dsn="postgresql://localhost/det",
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=_EVENT_SCHEMA,
        pipeline="noaa.storm_events",
    )
    execute_sql = [sql for op, sql, _ in calls if op == "execute" and sql]
    assert any("pg_advisory_lock" in sql.lower() for sql in execute_sql)
    assert any("delete from" in sql.lower() for sql in execute_sql)
    create = next(
        sql for sql in execute_sql if sql.lower().startswith("create table")
    )
    assert "INTEGER" in create
    assert "JSONB" not in create
    delete = next(c for c in calls if c[0] == "execute" and c[1] and "delete from" in c[1].lower())
    assert delete[2] == (
        "2026-08-06T00:00:00+00:00",
        "2026-08-07T00:00:00+00:00",
        "2026-08-06T15:04:05+00:00",
    )
    assert any(op == "executemany" for op, _, _ in calls)
    assert calls[-1][0] == "commit"


def test_write_postgres_table_chunks_inserts(monkeypatch):
    calls: list[tuple[str, str | None, object]] = []
    _install_fake_pg(monkeypatch, calls)
    records = [
        {**_records()[0], "__row_hash": "a"},
        {**_records()[0], "__row_hash": "b"},
        {**_records()[0], "__row_hash": "c"},
    ]
    write_postgres_table(
        records,
        dsn="postgresql://localhost/det",
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=_EVENT_SCHEMA,
        chunk_rows=1,
    )
    deletes = [
        c
        for c in calls
        if c[0] == "execute" and c[1] and "delete from" in c[1].lower()
    ]
    assert len(deletes) == 1
    inserts = [c for c in calls if c[0] == "executemany"]
    assert len(inserts) == 3
    assert all(len(c[2]) == 1 for c in inserts)
    assert calls[-1][0] == "commit"


def test_write_postgres_table_schema_types_and_alter(monkeypatch):
    calls: list[tuple[str, str | None, object]] = []
    _install_fake_pg(monkeypatch, calls)
    nested = {
        "type": "object",
        "properties": {
            "event_id": {"type": "integer"},
            "authors": {"type": "array", "items": {"type": "object"}},
        },
    }
    write_postgres_table(
        [{**_records()[0], "authors": [{"name": "Ada"}]}],
        dsn="postgresql://localhost/det",
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=nested,
    )
    create = next(
        sql
        for op, sql, _ in calls
        if op == "execute" and sql and sql.lower().startswith("create table")
    )
    assert '"event_id" INTEGER' in create
    assert '"authors" JSONB' in create

    calls.clear()
    wider = {
        "type": "object",
        "properties": {
            "event_id": {"type": "integer"},
            "authors": {"type": "array", "items": {"type": "object"}},
            "state": {"type": ["string", "null"]},
        },
    }
    write_postgres_table(
        [{**_records()[0], "state": "TX", "__row_hash": "b"}],
        dsn="postgresql://localhost/det",
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=wider,
    )
    alters = [
        sql
        for op, sql, _ in calls
        if op == "execute" and sql and "alter table" in sql.lower()
    ]
    assert len(alters) == 1
    assert "ADD COLUMN" in alters[0] and '"state"' in alters[0]
    deletes = [
        c
        for c in calls
        if c[0] == "execute" and c[1] and "delete from" in c[1].lower()
    ]
    assert len(deletes) == 1
    assert any(op == "executemany" for op, _, _ in calls)


def test_write_postgres_table_rejects_missing_run_meta(monkeypatch):
    monkeypatch.setattr(
        "det.ingestion.postgres_writer._import_psycopg",
        lambda: type("P", (), {"connect": staticmethod(lambda dsn: None)})(),
    )
    with pytest.raises(ValueError, match="replace-by-run requires"):
        write_postgres_table(
            [{"id": 1}],
            dsn="postgresql://localhost/det",
            schema="bronze",
            table="t",
            json_schema={"type": "object", "properties": {"id": {"type": "integer"}}},
        )
