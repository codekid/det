from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from det.ingestion.dlt_backend import DltBackend
from det.ingestion.thin_backend import ThinBackend
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)


def _config(
    tmp_path: Path,
    library: str,
    *,
    destination: DestinationConfig | None = None,
) -> PipelineConfig:
    return PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
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


def test_thin_and_dlt_write_comparable_jsonl(tmp_path: Path):
    records = _records()
    partition_thin = tmp_path / "thin" / "__interval_start_datetime=20260806T000000Z"
    partition_dlt = tmp_path / "dlt" / "__interval_start_datetime=20260806T000000Z"

    cfg_thin = _config(tmp_path, "thin")
    cfg_dlt = _config(tmp_path, "dlt")

    ThinBackend().write(
        records,
        config=cfg_thin,
        project_root=tmp_path,
        partition_dir=partition_thin,
        destination=cfg_thin.destination,
    )
    DltBackend().write(
        records,
        config=cfg_dlt,
        project_root=tmp_path,
        partition_dir=partition_dlt,
        destination=cfg_dlt.destination,
    )

    thin_rows = [
        json.loads(line)
        for line in (partition_thin / "data.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    dlt_rows = [
        json.loads(line)
        for line in (partition_dlt / "data.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(thin_rows) == len(dlt_rows) == 1
    assert thin_rows[0]["event_id"] == dlt_rows[0]["event_id"]
    assert thin_rows[0]["__data_interval_date"] == "2026-08-06"
    # Every backend lands the contract byte-for-byte.
    assert thin_rows[0] == dlt_rows[0] == records[0]


def test_no_backend_lets_dlt_manage_state_or_unnest(tmp_path: Path):
    """
    Bronze is DET's. A dlt pipeline would persist _dlt_loads / _dlt_pipeline_state /
    _dlt_version next to the data, add _dlt_id and _dlt_load_id columns, and strip
    the leading __ from the meta columns.
    """
    for library, backend in (("thin", ThinBackend()), ("dlt", DltBackend())):
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


def test_dlt_backend_writes_duckdb_append(tmp_path: Path):
    db_path = tmp_path / "analytics.duckdb"
    dest = DestinationConfig(
        type="duckdb",
        path=str(tmp_path / "lake"),
        connection=str(db_path),
        dataset="bronze",
    )
    config = _config(tmp_path, "dlt", destination=dest)
    backend = DltBackend()
    written = backend.write(
        _records(),
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "unused_hive",
        destination=dest,
    )
    assert written == db_path.resolve()
    assert not (tmp_path / "unused_hive").exists()

    # Second append keeps both runs (no overwrite).
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
        count = con.execute("select count(*) from bronze_noaa.storm_events").fetchone()[0]
        assert count == 2
        hashes = {
            r[0]
            for r in con.execute(
                "select __row_hash from bronze_noaa.storm_events order by __row_hash"
            ).fetchall()
        }
        assert hashes == {"abc", "def"}
        event_id = con.execute(
            "select event_id from bronze_noaa.storm_events where __row_hash = 'abc'"
        ).fetchone()[0]
        assert event_id == 1
    finally:
        con.close()


def test_dlt_backend_postgres_delegates_to_writer(tmp_path: Path):
    """Postgres landing is DET-owned (same as duckdb), not a dlt pipeline."""
    from unittest.mock import patch

    backend = DltBackend()
    dest = DestinationConfig(
        type="postgres",
        path=str(tmp_path / "lake"),
        connection="postgresql://localhost/det",
    )
    config = _config(tmp_path, "dlt", destination=dest)
    with patch("det.ingestion.dlt_backend.write_postgres_table") as write:
        write.return_value = dest.connection
        out = backend.write(
            _records(),
            config=config,
            project_root=tmp_path,
            partition_dir=tmp_path / "pg",
            destination=dest,
        )
    write.assert_called_once()
    assert out == Path("postgres") / "bronze_noaa" / "storm_events"
