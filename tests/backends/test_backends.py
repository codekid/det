from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from det.ingestion.det_backend import DetBackend
from det.ingestion.thin_backend import ThinBackend
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.meta import identity_iso


def _json_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "event_id": {"type": ["integer", "null"]},
            "begin_day": {"type": "integer"},
            "begin_time": {"type": "integer"},
        },
    }


def _config(
    tmp_path: Path,
    library: str,
    *,
    destination: DestinationConfig | None = None,
) -> PipelineConfig:
    schema_rel = "event.schema.yaml"
    (tmp_path / schema_rel).write_text(
        "type: object\n"
        "properties:\n"
        "  event_id: {type: [integer, 'null']}\n"
        "  begin_day: {type: integer}\n"
        "  begin_time: {type: integer}\n",
        encoding="utf-8",
    )
    return PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path=schema_rel,
        ingestion=IngestionConfig(library=library),  # type: ignore[arg-type]
        destination=destination
        or DestinationConfig(type="filesystem", path=str(tmp_path / "lake")),
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
    )


def _records():
    return [
        {
            "event_id": 1,
            "begin_day": 1,
            "begin_time": 0,
            "__row_hash": "abc",
            "__filename": "x.csv",
            "__extract_run_datetime": "2026-08-06T15:04:05.123456+00:00",
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
            "__data_interval_date": "2026-08-06",
        }
    ]


def test_thin_and_det_write_comparable_jsonl(tmp_path: Path):
    records = _records()
    partition_thin = tmp_path / "thin" / "__interval_start_datetime=20260806T000000Z"
    partition_det = tmp_path / "det" / "__interval_start_datetime=20260806T000000Z"

    cfg_thin = _config(tmp_path, "thin")
    cfg_det = _config(tmp_path, "det")

    ThinBackend().write(
        records,
        config=cfg_thin,
        project_root=tmp_path,
        partition_dir=partition_thin,
        destination=cfg_thin.destination,
    )
    DetBackend().write(
        records,
        config=cfg_det,
        project_root=tmp_path,
        partition_dir=partition_det,
        destination=cfg_det.destination,
    )

    thin_rows = [
        json.loads(line)
        for line in (partition_thin / "data.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    det_rows = [
        json.loads(line)
        for line in (partition_det / "data.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(thin_rows) == len(det_rows) == 1
    assert thin_rows[0]["event_id"] == det_rows[0]["event_id"]
    assert thin_rows[0]["__data_interval_date"] == "2026-08-06"
    # Every backend lands the contract byte-for-byte.
    assert thin_rows[0] == det_rows[0] == records[0]


def test_no_backend_lets_dlt_manage_state_or_unnest(tmp_path: Path):
    """
    Bronze is DET's. A dlt pipeline would persist _dlt_loads / _dlt_pipeline_state /
    _dlt_version next to the data, add _dlt_id and _dlt_load_id columns, and strip
    the leading __ from the meta columns.
    """
    for library, backend in (
        ("thin", ThinBackend()),
        ("det", DetBackend()),
        ("dlt", DetBackend()),  # deprecated alias
    ):
        config = _config(tmp_path, library)
        partition = tmp_path / library / "__interval_start_datetime=20260806T000000Z"
        backend.write(
            _records(),
            config=config,
            project_root=tmp_path,
            partition_dir=partition,
            destination=config.destination,
        )
        assert [p.name for p in partition.iterdir()] == ["data.jsonl"]
        row = json.loads((partition / "data.jsonl").read_text(encoding="utf-8").splitlines()[0])
        assert "__raw" not in row
        assert not [k for k in row if k.startswith("_dlt")]
        assert "__row_hash" in row and "row_hash" not in row


def test_duckdb_destination_requires_connection():
    with pytest.raises(ValidationError, match="destination.connection is required"):
        DestinationConfig(type="duckdb", path="./data/lake")


def test_iceberg_destination_does_not_require_connection():
    dest = DestinationConfig(type="iceberg", path="./data/lake")
    assert dest.type == "iceberg"
    assert dest.connection is None


def test_det_backend_writes_duckdb_append(tmp_path: Path):
    db_path = tmp_path / "analytics.duckdb"
    dest = DestinationConfig(
        type="duckdb",
        path=str(tmp_path / "lake"),
        connection=str(db_path),
        dataset="bronze",
    )
    config = _config(tmp_path, "det", destination=dest)
    backend = DetBackend()
    written = backend.write(
        _records(),
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "unused_hive",
        destination=dest,
    )
    assert written == db_path.resolve()
    assert not (tmp_path / "unused_hive").exists()

    # Sibling extract run is kept (replace is per extract_run, not the whole table).
    second = [
        {
            **_records()[0],
            "__row_hash": "def",
            "__extract_run_datetime": "2026-08-06T16:00:00+00:00",
        }
    ]
    backend.write(
        second,
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "unused_hive2",
        destination=dest,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        count = con.execute("select count(*) from bronze_noaa.storm_events_v1").fetchone()[0]
        assert count == 2
        hashes = {
            r[0]
            for r in con.execute(
                "select __row_hash from bronze_noaa.storm_events_v1 order by __row_hash"
            ).fetchall()
        }
        assert hashes == {"abc", "def"}
        event_id = con.execute(
            "select event_id from bronze_noaa.storm_events_v1 where __row_hash = 'abc'"
        ).fetchone()[0]
        assert event_id == 1
    finally:
        con.close()


def test_det_backend_duckdb_replaces_same_extract_run(tmp_path: Path):
    db_path = tmp_path / "analytics.duckdb"
    dest = DestinationConfig(
        type="duckdb",
        path=str(tmp_path / "lake"),
        connection=str(db_path),
        dataset="bronze",
    )
    config = _config(tmp_path, "det", destination=dest)
    backend = DetBackend()
    first = _records()
    sibling = [
        {
            **first[0],
            "event_id": 2,
            "__row_hash": "sib",
            "__extract_run_datetime": "2026-08-06T16:00:00+00:00",
        }
    ]
    backend.write(
        first,
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "hive1",
        destination=dest,
    )
    backend.write(
        sibling,
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "hive-sib",
        destination=dest,
    )
    retry = [
        {
            **first[0],
            "event_id": 99,
            "__row_hash": "abc-retry",
        }
    ]
    backend.write(
        retry,
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "hive2",
        destination=dest,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = {
            (r[0], r[1], identity_iso(r[2]))
            for r in con.execute(
                "select event_id, __row_hash, __extract_run_datetime "
                "from bronze_noaa.storm_events_v1"
            ).fetchall()
        }
        assert rows == {
            (99, "abc-retry", first[0]["__extract_run_datetime"]),
            (2, "sib", "2026-08-06T16:00:00+00:00"),
        }
    finally:
        con.close()


def test_write_duckdb_table_rejects_mixed_extract_run(tmp_path: Path):
    from det.ingestion.duckdb_writer import write_duckdb_table

    mixed = [
        _records()[0],
        {
            **_records()[0],
            "__row_hash": "other",
            "__extract_run_datetime": "2026-08-06T16:00:00+00:00",
        },
    ]
    with pytest.raises(ValueError, match="does not match the batch"):
        write_duckdb_table(
            mixed,
            connection_path=tmp_path / "analytics.duckdb",
            schema="bronze_noaa",
            table="storm_events_v1",
            json_schema=_json_schema(),
        )
    assert not (tmp_path / "analytics.duckdb").exists()


def test_write_duckdb_chunk_rows_one_keeps_sibling_and_replaces_same_run(
    tmp_path: Path,
):
    from det.ingestion.duckdb_writer import write_duckdb_table

    db_path = tmp_path / "analytics.duckdb"
    first_run = "2026-08-06T15:04:05.123456+00:00"
    sibling_run = "2026-08-06T16:00:00+00:00"

    def first_rows():
        for h in ("abc", "abc-2"):
            yield {**_records()[0], "__row_hash": h}

    def sibling_rows():
        yield {
            **_records()[0],
            "__row_hash": "sib",
            "__extract_run_datetime": sibling_run,
        }

    write_duckdb_table(
        first_rows(),
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
        chunk_rows=1,
    )
    write_duckdb_table(
        sibling_rows(),
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
        chunk_rows=1,
    )
    write_duckdb_table(
        [{**_records()[0], "event_id": 99, "__row_hash": "abc-retry"}],
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
        chunk_rows=1,
    )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = {
            (r[0], r[1], identity_iso(r[2]))
            for r in con.execute(
                "select event_id, __row_hash, __extract_run_datetime "
                "from bronze_noaa.storm_events_v1"
            ).fetchall()
        }
        assert rows == {
            (99, "abc-retry", first_run),
            (1, "sib", sibling_run),
        }
    finally:
        con.close()


def test_write_duckdb_mid_stream_error_rolls_back(tmp_path: Path):
    from det.ingestion.duckdb_writer import write_duckdb_table

    db_path = tmp_path / "analytics.duckdb"
    write_duckdb_table(
        _records(),
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
        chunk_rows=1,
    )

    def boom():
        yield {**_records()[0], "event_id": 99, "__row_hash": "new"}
        raise RuntimeError("validate failed")

    with pytest.raises(RuntimeError, match="validate failed"):
        write_duckdb_table(
            boom(),
            connection_path=db_path,
            schema="bronze_noaa",
            table="storm_events_v1",
            json_schema=_json_schema(),
            chunk_rows=1,
        )

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "select event_id, __row_hash from bronze_noaa.storm_events_v1"
        ).fetchall()
        assert rows == [(1, "abc")]
    finally:
        con.close()


def test_det_backend_postgres_delegates_to_writer(tmp_path: Path):
    """Postgres landing is DET-owned (same as duckdb), not a dlt pipeline."""
    from unittest.mock import patch

    backend = DetBackend()
    dest = DestinationConfig(
        type="postgres",
        path=str(tmp_path / "lake"),
        connection="postgresql://localhost/det",
    )
    config = _config(tmp_path, "det", destination=dest)
    with patch("det.ingestion.det_backend.write_postgres_table") as write:
        write.return_value = dest.connection
        out = backend.write(
            _records(),
            config=config,
            project_root=tmp_path,
            partition_dir=tmp_path / "pg",
            destination=dest,
        )
    write.assert_called_once()
    assert out == Path("postgres") / "bronze_noaa" / "storm_events_v1"


def _meta(**overrides):
    row = {
        "__row_hash": "abc",
        "__filename": "x.csv",
        "__extract_run_datetime": "2026-08-06T15:04:05.123456+00:00",
        "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
        "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
        "__data_interval_date": "2026-08-06",
        **overrides,
    }
    return row


def test_write_duckdb_null_first_chunk_uses_schema_integer(tmp_path: Path):
    from det.ingestion.duckdb_writer import write_duckdb_table

    db_path = tmp_path / "analytics.duckdb"
    write_duckdb_table(
        [{**_meta(), "event_id": None}],
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema={
            "type": "object",
            "properties": {"event_id": {"type": ["integer", "null"]}},
        },
    )
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        dtype = con.execute(
            "select data_type from information_schema.columns "
            "where table_schema = 'bronze_noaa' and table_name = 'storm_events_v1' "
            "and column_name = 'event_id'"
        ).fetchone()[0]
        assert str(dtype).upper() == "INTEGER"
    finally:
        con.close()


def test_write_duckdb_nested_object_is_json(tmp_path: Path):
    from det.ingestion.duckdb_writer import write_duckdb_table

    db_path = tmp_path / "analytics.duckdb"
    write_duckdb_table(
        [{**_meta(), "authors": [{"name": "Ada"}]}],
        connection_path=db_path,
        schema="bronze_ol",
        table="subjects_v1",
        json_schema={
            "type": "object",
            "properties": {
                "authors": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"name": {"type": "string"}}},
                }
            },
        },
    )
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        dtype = con.execute(
            "select data_type from information_schema.columns "
            "where table_schema = 'bronze_ol' and table_name = 'subjects_v1' "
            "and column_name = 'authors'"
        ).fetchone()[0]
        assert str(dtype).upper() == "JSON"
    finally:
        con.close()


def test_write_duckdb_alter_adds_missing_column(tmp_path: Path):
    from det.ingestion.duckdb_writer import write_duckdb_table

    db_path = tmp_path / "analytics.duckdb"
    base = {
        "type": "object",
        "properties": {"event_id": {"type": "integer"}},
    }
    write_duckdb_table(
        [{**_meta(), "event_id": 1}],
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema=base,
    )
    write_duckdb_table(
        [{**_meta(), "event_id": 2, "state": "TX", "__row_hash": "two",
          "__extract_run_datetime": "2026-08-06T16:00:00+00:00"}],
        connection_path=db_path,
        schema="bronze_noaa",
        table="storm_events_v1",
        json_schema={
            "type": "object",
            "properties": {
                "event_id": {"type": "integer"},
                "state": {"type": ["string", "null"]},
            },
        },
    )
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        dtype = con.execute(
            "select data_type from information_schema.columns "
            "where table_schema = 'bronze_noaa' and table_name = 'storm_events_v1' "
            "and column_name = 'state'"
        ).fetchone()[0]
        assert str(dtype).upper() in {"VARCHAR", "TEXT"}
        states = {
            r[0]
            for r in con.execute(
                "select state from bronze_noaa.storm_events_v1"
            ).fetchall()
        }
        assert states == {None, "TX"}
    finally:
        con.close()


def test_write_duckdb_refuses_varchar_vs_integer(tmp_path: Path):
    from det.ingestion.duckdb_writer import write_duckdb_table

    db_path = tmp_path / "analytics.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute('CREATE SCHEMA bronze_noaa')
        con.execute('CREATE TABLE bronze_noaa.storm_events_v1 ("event_id" VARCHAR)')
    finally:
        con.close()
    with pytest.raises(ValueError, match="has type VARCHAR, expected INTEGER"):
        write_duckdb_table(
            [{**_meta(), "event_id": 1}],
            connection_path=db_path,
            schema="bronze_noaa",
            table="storm_events_v1",
            json_schema={
                "type": "object",
                "properties": {"event_id": {"type": "integer"}},
            },
        )
