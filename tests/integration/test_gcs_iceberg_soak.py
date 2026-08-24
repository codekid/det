"""GCS object-lake soak: extract → Iceberg bronze → PyIceberg verify.

Skipped unless ``STORAGE_EMULATOR_HOST`` is set (CI fake-gcs-server / localgcp).
Requires ``det[gcs]`` + ``det[iceberg]``. BigLake / dbt-BigQuery are out of scope
here (real GCP sandbox).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from det.ingestion.iceberg_writer import load_iceberg_table, scan_iceberg_rows
from det.runtime.lake import ENV_LAKE_MODE, open_lake
from det.runtime.runner import PipelineRunner

_HOST = (os.environ.get("STORAGE_EMULATOR_HOST") or "").strip()
_BUCKET = (os.environ.get("DET_GCS_BUCKET") or "det-ci").strip()
_PROJECT = (os.environ.get("GOOGLE_CLOUD_PROJECT") or "det-local").strip()
_LAKE_URI = f"gs://{_BUCKET}/det-lake"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.gcs,
    pytest.mark.skipif(not _HOST, reason="STORAGE_EMULATOR_HOST not set"),
]

SOAK_ROWS = 25


def _ensure_bucket() -> None:
    pytest.importorskip("gcsfs")
    import fsspec

    from det.runtime.object_store import fsspec_gcs_kwargs

    fs = fsspec.filesystem("gcs", **fsspec_gcs_kwargs())
    if not fs.exists(_BUCKET):
        fs.mkdir(_BUCKET)


def _pipe(tmp_path: Path, project_root: Path) -> Path:
    schema_src = project_root / "schemas/example_api/events/events.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    fixtures = [
        {
            "id": f"e{i}",
            "occurred_at": "2026-08-06T12:00:00Z",
            "severity": "low",
            "state": "TX",
            "status": "1",
        }
        for i in range(SOAK_ROWS)
    ]
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {
                    "type": "example_api.events",
                    "overrides": {"fixture_records": fixtures},
                },
                "schema": "schemas/example_api/events/events.schema.yaml",
                "ingestion": {"chunk_rows": 10},
                "destination": {"type": "iceberg", "partition": "none"},
            }
        ),
        encoding="utf-8",
    )
    return pipe


def test_gcs_extract_load_iceberg_pyiceberg_scan(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("gcsfs")

    lake_uri = f"{_LAKE_URI}/pyiceberg-{tmp_path.name}"
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("DET_LAKE_PATH", lake_uri)
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", _HOST)
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", _PROJECT)

    _ensure_bucket()
    pipe = _pipe(tmp_path, project_root)
    result = PipelineRunner(tmp_path).run(
        pipe,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == SOAK_ROWS
    assert result.raw_dir is not None
    assert (result.raw_dir / "meta" / "manifest.json").exists()

    lake = open_lake(lake_uri, tmp_path)
    bronze = lake / "bronze" / "example_api" / "events_v1"
    assert bronze.exists()
    hint = (bronze / "metadata" / "version-hint.text").read_text().strip()
    assert "://" not in hint
    assert not hint.endswith(".metadata.json")

    ice = load_iceberg_table(
        lake=lake,
        namespace="bronze_example_api",
        table="events_v1",
        table_location=bronze,
    )
    assert ice is not None
    rows = scan_iceberg_rows(ice, limit=SOAK_ROWS + 5)
    assert len(rows) == SOAK_ROWS
