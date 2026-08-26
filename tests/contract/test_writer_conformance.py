"""Backend-parametrized writer conformance for publication-contract replace-by-run."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from det.ingestion.det_backend import DetBackend
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.meta import identity_iso

Interval = tuple[str, str, str]

START = "2026-08-06T00:00:00+00:00"
END = "2026-08-07T00:00:00+00:00"
RUN_A = "2026-08-06T15:04:05.123456+00:00"
RUN_B = "2026-08-06T16:00:00+00:00"


def _json_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "event_id": {"type": ["integer", "null"]},
            "label": {"type": ["string", "null"]},
        },
    }


def _config(tmp_path: Path, destination: DestinationConfig) -> PipelineConfig:
    schema_rel = "event.schema.yaml"
    (tmp_path / schema_rel).write_text(
        "type: object\n"
        "properties:\n"
        "  event_id: {type: [integer, 'null']}\n"
        "  label: {type: [string, 'null']}\n",
        encoding="utf-8",
    )
    return PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path=schema_rel,
        ingestion=IngestionConfig(library="det"),
        destination=destination,
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
    )


def _row(
    *,
    event_id: int | None,
    label: str | None,
    extract_run: str,
    row_hash: str,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "label": label,
        "__row_hash": row_hash,
        "__filename": "x.csv",
        "__extract_run_datetime": extract_run,
        "__bronze_loaded_at": extract_run,
        "__interval_start_datetime": START,
        "__interval_end_datetime": END,
        "__data_interval_date": "2026-08-06",
    }


def _identity(extract_run: str) -> Interval:
    return (START, END, extract_run)


class _FsHarness:
    name = "filesystem"

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.dest = DestinationConfig(type="filesystem", path=str(tmp_path / "lake"))
        self.config = _config(tmp_path, self.dest)
        self.backend = DetBackend()

    def write(self, records: list[dict[str, Any]], *, identity: Interval) -> None:
        part = (
            self.tmp_path
            / "bronze"
            / f"run={identity[2].replace(':', '').replace('+', 'Z')}"
        )
        self.backend.write(
            records,
            config=self.config,
            project_root=self.tmp_path,
            partition_dir=part,
            destination=self.dest,
            run_identity=identity,
        )

    def rows_for(self, extract_run: str) -> list[dict[str, Any]]:
        part = (
            self.tmp_path
            / "bronze"
            / f"run={extract_run.replace(':', '').replace('+', 'Z')}"
        )
        data = part / "data.jsonl"
        if not data.exists():
            return []
        return [
            json.loads(line)
            for line in data.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]


class _DuckHarness:
    name = "duckdb"

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.db_path = tmp_path / "analytics.duckdb"
        self.dest = DestinationConfig(
            type="duckdb",
            path=str(tmp_path / "lake"),
            connection=str(self.db_path),
            dataset="bronze",
        )
        self.config = _config(tmp_path, self.dest)
        self.backend = DetBackend()

    def write(self, records: list[dict[str, Any]], *, identity: Interval) -> None:
        self.backend.write(
            records,
            config=self.config,
            project_root=self.tmp_path,
            partition_dir=self.tmp_path / "hive",
            destination=self.dest,
            run_identity=identity,
        )

    def rows_for(self, extract_run: str) -> list[dict[str, Any]]:
        import duckdb

        if not self.db_path.exists():
            return []
        con = duckdb.connect(str(self.db_path), read_only=True)
        try:
            out = []
            for r in con.execute(
                "select event_id, label, __row_hash, "
                "__interval_start_datetime, __interval_end_datetime, "
                "__extract_run_datetime "
                "from bronze_noaa.storm_events_v1"
            ).fetchall():
                if identity_iso(r[5]) != extract_run:
                    continue
                out.append(
                    {
                        "event_id": r[0],
                        "label": r[1],
                        "__row_hash": r[2],
                        "__interval_start_datetime": identity_iso(r[3]),
                        "__interval_end_datetime": identity_iso(r[4]),
                        "__extract_run_datetime": identity_iso(r[5]),
                    }
                )
            return out
        finally:
            con.close()


class _PgHarness:
    name = "postgres"

    def __init__(self, tmp_path: Path, dsn: str) -> None:
        pytest.importorskip("psycopg")
        self.tmp_path = tmp_path
        self.dsn = dsn
        self.dest = DestinationConfig(
            type="postgres",
            path=str(tmp_path / "lake"),
            connection=dsn,
            dataset="bronze",
        )
        self.config = _config(tmp_path, self.dest)
        self.backend = DetBackend()
        self._drop()

    def _drop(self) -> None:
        import psycopg

        with psycopg.connect(self.dsn) as con:
            con.execute("drop schema if exists bronze_noaa cascade")
            con.commit()

    def write(self, records: list[dict[str, Any]], *, identity: Interval) -> None:
        self.backend.write(
            records,
            config=self.config,
            project_root=self.tmp_path,
            partition_dir=self.tmp_path / "hive",
            destination=self.dest,
            run_identity=identity,
        )

    def rows_for(self, extract_run: str) -> list[dict[str, Any]]:
        import psycopg

        with psycopg.connect(self.dsn) as con:
            cur = con.execute(
                "select event_id, label, __row_hash, "
                "__interval_start_datetime, __interval_end_datetime, "
                "__extract_run_datetime "
                "from bronze_noaa.storm_events_v1"
            )
            out = []
            for r in cur.fetchall():
                run = identity_iso(r[5])
                if run != extract_run:
                    continue
                out.append(
                    {
                        "event_id": r[0],
                        "label": r[1],
                        "__row_hash": r[2],
                        "__interval_start_datetime": identity_iso(r[3]),
                        "__interval_end_datetime": identity_iso(r[4]),
                        "__extract_run_datetime": run,
                    }
                )
            return out


class _IcebergHarness:
    name = "iceberg"

    def __init__(self, tmp_path: Path) -> None:
        pytest.importorskip("pyiceberg")
        pytest.importorskip("pyarrow")
        from det.ingestion.iceberg_writer import (
            load_iceberg_table,
            scan_iceberg_rows,
            write_iceberg_table,
        )
        from det.runtime.lake import open_lake

        self.tmp_path = tmp_path
        self.lake = open_lake(str(tmp_path / "lake"), tmp_path)
        self.loc = self.lake / "bronze" / "noaa" / "storm_events_v1"
        self._write_iceberg_table = write_iceberg_table
        self._load = load_iceberg_table
        self._scan = scan_iceberg_rows
        self._schema = _json_schema()

    def write(self, records: list[dict[str, Any]], *, identity: Interval) -> None:
        self._write_iceberg_table(
            records,
            lake=self.lake,
            table_location=self.loc,
            namespace="bronze_noaa",
            table="storm_events_v1",
            json_schema=self._schema,
            run_identity=identity,
        )

    def rows_for(self, extract_run: str) -> list[dict[str, Any]]:
        try:
            ice = self._load(
                lake=self.lake,
                namespace="bronze_noaa",
                table="storm_events_v1",
                table_location=self.loc,
            )
        except Exception:
            return []
        out = []
        for r in self._scan(ice, limit=100):
            run = identity_iso(r["__extract_run_datetime"])
            if run != extract_run:
                continue
            out.append(
                {
                    "event_id": r.get("event_id"),
                    "label": r.get("label"),
                    "__row_hash": r.get("__row_hash"),
                    "__interval_start_datetime": identity_iso(
                        r["__interval_start_datetime"]
                    ),
                    "__interval_end_datetime": identity_iso(
                        r["__interval_end_datetime"]
                    ),
                    "__extract_run_datetime": run,
                }
            )
        return out


@pytest.fixture(params=["filesystem", "duckdb", "postgres", "iceberg"])
def harness(request: pytest.FixtureRequest, tmp_path: Path):
    name = request.param
    if name == "filesystem":
        return _FsHarness(tmp_path)
    if name == "duckdb":
        return _DuckHarness(tmp_path)
    if name == "postgres":
        dsn = os.environ.get("DET_POSTGRES_DSN")
        if not dsn:
            pytest.skip("DET_POSTGRES_DSN not set")
        return _PgHarness(tmp_path, dsn)
    if name == "iceberg":
        return _IcebergHarness(tmp_path)
    raise AssertionError(name)


def test_replace_by_run(harness: Any):
    id_a = _identity(RUN_A)
    harness.write(
        [_row(event_id=1, label="a", extract_run=RUN_A, row_hash="h1")],
        identity=id_a,
    )
    harness.write(
        [_row(event_id=2, label="b", extract_run=RUN_B, row_hash="h2")],
        identity=_identity(RUN_B),
    )
    harness.write(
        [_row(event_id=99, label="retry", extract_run=RUN_A, row_hash="h99")],
        identity=id_a,
    )
    rows_a = harness.rows_for(RUN_A)
    rows_b = harness.rows_for(RUN_B)
    assert len(rows_a) == 1
    assert rows_a[0]["event_id"] == 99
    assert rows_a[0]["__row_hash"] == "h99"
    assert len(rows_b) == 1
    assert rows_b[0]["event_id"] == 2


def test_empty_reload_clears_identity_keeps_sibling(harness: Any):
    id_a = _identity(RUN_A)
    harness.write(
        [_row(event_id=1, label="a", extract_run=RUN_A, row_hash="h1")],
        identity=id_a,
    )
    harness.write(
        [_row(event_id=2, label="b", extract_run=RUN_B, row_hash="h2")],
        identity=_identity(RUN_B),
    )
    harness.write([], identity=id_a)
    assert harness.rows_for(RUN_A) == []
    rows_b = harness.rows_for(RUN_B)
    assert len(rows_b) == 1
    assert rows_b[0]["event_id"] == 2


def test_null_handling_and_meta(harness: Any):
    id_a = _identity(RUN_A)
    harness.write(
        [_row(event_id=None, label=None, extract_run=RUN_A, row_hash="nulls")],
        identity=id_a,
    )
    rows = harness.rows_for(RUN_A)
    assert len(rows) == 1
    assert rows[0]["event_id"] is None
    assert rows[0]["label"] is None
    assert rows[0]["__interval_start_datetime"] == START
    assert rows[0]["__interval_end_datetime"] == END
    assert rows[0]["__extract_run_datetime"] == RUN_A
