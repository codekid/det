from __future__ import annotations

import builtins
from pathlib import Path
from unittest.mock import patch

import pytest
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


def test_postgres_destination_requires_connection():
    with pytest.raises(ValidationError, match="destination.connection is required"):
        DestinationConfig(type="postgres", path="./data/lake")


def test_det_backend_writes_postgres_via_helper(tmp_path: Path):
    dest = DestinationConfig(
        type="postgres",
        path=str(tmp_path / "lake"),
        connection="postgresql://user:pass@localhost:5432/det",
        dataset="bronze",
    )
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
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
        )
