"""GCP Lakehouse REST catalog soak: greenfield extract/load on gs://.

Skipped unless a real GCP project is configured (no ``STORAGE_EMULATOR_HOST``)
and ``DET_ICEBERG_REST_WAREHOUSE`` is a ``bl://projects/…/catalogs/…`` URI.

Setup/teardown: docs/gcp-lakehouse-soak.md
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from det.ingestion.iceberg_catalog_factory import ENV_CATALOG, resolve_iceberg_catalog
from det.ingestion.iceberg_writer import scan_iceberg_rows
from det.runtime.lake import ENV_LAKE_MODE, open_lake
from det.runtime.runner import PipelineRunner

_PROJECT = (
    os.environ.get("DET_GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or os.environ.get("GCLOUD_PROJECT")
    or ""
).strip()
_WAREHOUSE = (os.environ.get("DET_ICEBERG_REST_WAREHOUSE") or "").strip()
_REST_URI = (
    os.environ.get("DET_ICEBERG_REST_URI")
    or "https://biglake.googleapis.com/iceberg/v1/restcatalog"
).strip()
_LAKE_PATH = (os.environ.get("DET_LAKE_PATH") or "").strip()
_EMULATOR = (os.environ.get("STORAGE_EMULATOR_HOST") or "").strip()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.lakehouse,
    pytest.mark.skipif(bool(_EMULATOR), reason="STORAGE_EMULATOR_HOST set (use -m gcs)"),
    pytest.mark.skipif(not _PROJECT, reason="DET_GCP_PROJECT / GOOGLE_CLOUD_PROJECT not set"),
    pytest.mark.skipif(
        not _WAREHOUSE.startswith("bl://"),
        reason="DET_ICEBERG_REST_WAREHOUSE bl:// URI not set",
    ),
    pytest.mark.skipif(
        not _LAKE_PATH.startswith("gs://"),
        reason="DET_LAKE_PATH gs:// not set",
    ),
]

SOAK_ROWS = 12


def _pipe(tmp_path: Path, project_root: Path) -> Path:
    schema_src = project_root / "schemas/example_api/events/events.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    fixtures = [
        {
            "id": f"lh{i}",
            "occurred_at": "2026-08-06T12:00:00Z",
            "severity": "low",
            "state": "TX",
            "status": "1",
        }
        for i in range(SOAK_ROWS)
    ]
    wire_version = abs(hash(tmp_path.name)) % 900_000 + 100_000
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
                "wire_version": wire_version,
            }
        ),
        encoding="utf-8",
    )
    return pipe


def _rest_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv("DET_ICEBERG_REST_URI", _REST_URI)
    monkeypatch.setenv("DET_ICEBERG_REST_WAREHOUSE", _WAREHOUSE)
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", _PROJECT)
    monkeypatch.delenv("STORAGE_EMULATOR_HOST", raising=False)
    monkeypatch.delenv("DET_ICEBERG_REST_CREDENTIAL", raising=False)


def test_gcp_lakehouse_greenfield_rest_write(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    pytest.importorskip("gcsfs")

    lake_uri = f"{_LAKE_PATH.rstrip('/')}/soak-{tmp_path.name}"
    _rest_env(monkeypatch)
    monkeypatch.setenv("DET_LAKE_PATH", lake_uri)

    pipe = _pipe(tmp_path, project_root)
    runner = PipelineRunner(project_root=tmp_path)
    result = runner.run(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
    assert result.rows == SOAK_ROWS

    from det.runtime.config import load_pipeline_config
    from det.runtime.ids import sql_names_for_config

    config = load_pipeline_config(pipe)
    ns, table = sql_names_for_config(config)
    lake = open_lake(lake_uri, tmp_path)
    catalog = resolve_iceberg_catalog(lake)
    ice = catalog.load_table((ns, table))
    rows = scan_iceberg_rows(ice, limit=SOAK_ROWS + 5)
    assert len(rows) == SOAK_ROWS
