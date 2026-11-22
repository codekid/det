"""Integration: destination.type bigquery round-trip (opt-in).

Skipped unless ``DET_BQ_INTEGRATION=1`` and ``DET_GCP_PROJECT`` are set.
Uses a disposable dataset prefix so CI/local runs do not collide.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from det.ingestion.bq_writer import (
    bq_dataset_for_config,
    bq_table_for_config,
    write_to_bigquery,
)
from det.optional_deps import try_import_bigquery
from det.runtime.config import load_pipeline_config
from det.runtime.runner import PipelineRunner
from det.runtime.settings import DetSettings, use_settings
from det.validation.jsonschema_validator import load_json_schema

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("DET_BQ_INTEGRATION", "").strip() != "1",
        reason="set DET_BQ_INTEGRATION=1 to run BigQuery destination soaks",
    ),
    pytest.mark.skipif(
        try_import_bigquery() is None,
        reason="google-cloud-bigquery not installed (uv sync --extra bigquery)",
    ),
    pytest.mark.skipif(
        not (
            os.environ.get("DET_GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT") or ""
        ).strip(),
        reason="DET_GCP_PROJECT (or GOOGLE_CLOUD_PROJECT) required",
    ),
]


def _storm_bq_pipeline(project_root: Path, tmp_path: Path) -> Path:
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
        "ingestion": {"library": "det"},
        "destination": {"type": "bigquery"},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
        "wire_version": 1,
    }
    pipe_path = tmp_path / "pipeline_bq.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    return pipe_path


def test_run_noaa_fixture_to_bigquery(project_root: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "lake"))
    monkeypatch.setenv("DET_LOCK", "0")
    # Isolate from other suites with a unique dataset prefix.
    prefix = f"det_bq_it_{os.getpid()}"
    monkeypatch.setenv("DET_BQ_DATASET_PREFIX", prefix)

    pipe_path = _storm_bq_pipeline(project_root, tmp_path)
    settings = DetSettings.from_env(project_root=tmp_path)
    with use_settings(settings):
        result = PipelineRunner(settings=settings).run(
            pipe_path,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
        )
    assert result.rows == 2
    assert str(result.partition_dir).startswith("bq/")

    config = load_pipeline_config(pipe_path)
    dataset = bq_dataset_for_config(config, settings)
    table = bq_table_for_config(config)
    assert dataset.startswith(prefix)
    assert table == "storm_events_v1"

    bigquery = try_import_bigquery()
    assert bigquery is not None
    client = bigquery.Client(project=settings.bq_project)
    table_id = f"{settings.bq_project}.{dataset}.{table}"
    rows = list(client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result())
    assert rows[0]["n"] == 2

    # Cleanup disposable table/dataset (best-effort).
    client.delete_table(table_id, not_found_ok=True)
    client.delete_dataset(f"{settings.bq_project}.{dataset}", not_found_ok=True)


def test_export_bq_from_filesystem_bronze(project_root: Path, tmp_path: Path, monkeypatch):
    """Path B: land filesystem bronze, then export to BigQuery."""
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "lake"))
    monkeypatch.setenv("DET_LOCK", "0")
    prefix = f"det_bq_export_{os.getpid()}"
    monkeypatch.setenv("DET_BQ_DATASET_PREFIX", prefix)

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
        "destination": {"type": "filesystem"},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
        "wire_version": 1,
    }
    pipe_path = tmp_path / "pipeline_fs.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")

    settings = DetSettings.from_env(project_root=tmp_path)
    with use_settings(settings):
        landed = PipelineRunner(settings=settings).run(
            pipe_path,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
        )
    assert landed.rows == 2

    from det.runtime.bronze_export import load_bronze_export_rows

    config = load_pipeline_config(pipe_path)
    schema = load_json_schema(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml")
    batches = load_bronze_export_rows(
        config,
        project_root=tmp_path,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        settings=settings,
    )
    assert len(batches) == 1
    identity, rows = batches[0]
    assert len(rows) == 2

    with use_settings(settings):
        written = write_to_bigquery(
            rows,
            config=config,
            json_schema=schema,
            settings=settings,
            run_identity=identity,
        )
    assert str(written).startswith("bq/")

    dataset = bq_dataset_for_config(config, settings)
    table = bq_table_for_config(config)
    bigquery = try_import_bigquery()
    assert bigquery is not None
    client = bigquery.Client(project=settings.bq_project)
    table_id = f"{settings.bq_project}.{dataset}.{table}"
    counted = list(client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result())
    assert counted[0]["n"] == 2
    client.delete_table(table_id, not_found_ok=True)
    client.delete_dataset(f"{settings.bq_project}.{dataset}", not_found_ok=True)
