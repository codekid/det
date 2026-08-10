from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.runtime.migrate import BronzeMigrator
from det.runtime.runner import PipelineRunner


def _storm_pipeline(project_root: Path, tmp_path: Path) -> Path:
    pipeline = {
        "name": "noaa.storm_events",
        "source": {
            "type": "noaa.storm_events",
            "overrides": {
                "local_csv_dir": str(project_root / "fixtures/storm_events"),
                "filename_substr": "details",
            },
        },
        "schema": str(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml"),
        "ingestion": {"library": "thin"},
        "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "pipeline.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    return pipe_path


@pytest.mark.integration
def test_run_noaa_fixture_to_bronze(project_root: Path, tmp_path: Path):
    pipe_path = _storm_pipeline(project_root, tmp_path)

    result = PipelineRunner(tmp_path).run(
        pipe_path,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == 2
    assert result.raw_dir is not None
    assert (result.raw_dir / "meta" / "manifest.json").exists()
    assert (result.raw_dir / "data").is_dir()

    start, end, run = result.partition_dir.parts[-3:]
    assert start == "__interval_start_datetime=20260806T000000Z"
    assert end == "__interval_end_datetime=20260807T000000Z"
    assert run.startswith("__extract_run_datetime=")
    assert not any(p.startswith("__data_interval_date=") for p in result.partition_dir.parts)
    lines = (result.partition_dir / "data.jsonl").read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[0])
    assert "__raw" not in row
    assert "__row_hash" in row
    assert row["begin_day"] == 15
    assert row["event_id"] == 9001



def _fatalities_pipeline(project_root: Path, tmp_path: Path) -> Path:
    pipeline = {
        "name": "noaa.fatalities",
        "source": {
            "type": "noaa.fatalities",
            "overrides": {
                "local_csv_dir": str(project_root / "fixtures/fatalities"),
                "filename_substr": "fatalities",
            },
        },
        "schema": str(project_root / "schemas/noaa/fatalities/fatalities.schema.yaml"),
        "ingestion": {"library": "thin"},
        "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "fatalities.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    return pipe_path


@pytest.mark.integration
def test_run_noaa_fatalities_fixture_to_bronze(project_root: Path, tmp_path: Path):
    pipe_path = _fatalities_pipeline(project_root, tmp_path)

    result = PipelineRunner(tmp_path).run(
        pipe_path,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == 2
    assert result.raw_dir is not None
    assert "fatalities" in result.raw_dir.parts
    assert (result.raw_dir / "meta" / "manifest.json").exists()
    row = json.loads(
        (result.partition_dir / "data.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    assert row["fatality_id"] == 90001
    assert row["event_id"] == 9001
    assert row["fatality_type"] == "D"
    assert row["fatality_location"] == "In Water"
    assert "__raw" not in row


@pytest.mark.integration
def test_extract_then_load_shares_run_stamp(project_root: Path, tmp_path: Path):
    pipe_path = _storm_pipeline(project_root, tmp_path)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe_path, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    loaded = runner.load(
        pipe_path,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert loaded.rows == 2
    assert loaded.extract_run_datetime == extracted.extract_run_datetime
    assert loaded.raw_dir == extracted.raw_dir


@pytest.mark.integration
def test_multi_day_window_lands_one_partition(project_root: Path, tmp_path: Path):
    """One det run = one partition for the full requested window."""
    pipe_path = _storm_pipeline(project_root, tmp_path)

    result = PipelineRunner(tmp_path).run(
        pipe_path,
        interval_start="2026-08-01",
        interval_end="2026-08-04",
    )
    assert result.partition_dir.parts[-3:-1] == (
        "__interval_start_datetime=20260801T000000Z",
        "__interval_end_datetime=20260804T000000Z",
    )
    bronze = tmp_path / "lake" / "bronze" / "noaa" / "storm_events"
    assert len(list(bronze.rglob("data.jsonl"))) == 1
    row = json.loads((result.partition_dir / "data.jsonl").read_text().splitlines()[0])
    assert row["__interval_start_datetime"] == "2026-08-01T00:00:00+00:00"
    assert row["__interval_end_datetime"] == "2026-08-04T00:00:00+00:00"


@pytest.mark.integration
def test_example_api_fixture_and_migrate(project_root: Path, tmp_path: Path):
    pipeline = {
        "name": "example_api.events",
        "source": {
            "type": "example_api.events",
            "overrides": {
                "fixture_records": [
                    {
                        "id": "e1",
                        "occurred_at": "2026-08-06T12:00:00Z",
                        "severity": "high",
                        "state": "TX",
                    }
                ]
            },
        },
        "schema": str(project_root / "schemas/example_api/events/events.schema.yaml"),
        "ingestion": {"library": "thin"},
        "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "api.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")

    run = PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")
    assert run.rows == 1
    assert run.raw_dir is not None
    assert (run.raw_dir / "meta" / "manifest.json").exists()
    assert (run.raw_dir / "data").is_dir()
    assert "example_api" in run.raw_dir.parts and "events" in run.raw_dir.parts

    mig = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v2",
        schema_path=project_root / "schemas/example_api/events/events_v2.schema.yaml",
        mapper_name="example_api_v1_to_v2",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        ingestion_library="thin",
    )
    assert mig.rows == 1
    out = next(
        (tmp_path / "lake" / "bronze" / "example_api" / "events_v2").rglob("data.jsonl")
    )
    assert out.parent.name.startswith("__extract_run_datetime=")
    migrated = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert migrated["level"] == "high"
    assert "severity" not in migrated
    assert "__raw" not in migrated


@pytest.mark.integration
def test_migrate_rebuilds_from_raw_wire(project_root: Path, tmp_path: Path):
    """Migrate ignores bronze payload and re-parses raw wire."""
    pipe_path = _storm_pipeline(project_root, tmp_path)
    run = PipelineRunner(tmp_path).run(
        pipe_path, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    # Corrupt bronze so a bronze-based migrate would fail / diverge.
    (run.partition_dir / "data.jsonl").write_text("{}\n", encoding="utf-8")

    mig = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="noaa.storm_events_rebuilt",
        schema_path=project_root / "schemas/noaa/storm_events/storm_events.schema.yaml",
        mapper_name="storm_events_identity",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        lake_path=str(tmp_path / "lake"),
        ingestion_library="thin",
    )
    assert mig.rows == 2
    out = next(
        (tmp_path / "lake" / "bronze" / "noaa" / "storm_events_rebuilt").rglob(
            "data.jsonl"
        )
    )
    row = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert row["event_id"] == 9001
    assert row["__interval_start_datetime"] == "2026-08-06T00:00:00+00:00"
    assert row["__interval_end_datetime"] == "2026-08-07T00:00:00+00:00"
