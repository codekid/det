"""Polaris + MinIO soak: Hadoop write → register, and greenfield REST write.

Skipped unless ``DET_ICEBERG_REST_URI`` and ``AWS_ENDPOINT_URL`` are set
(``make polaris-up`` / CI polaris-minio compose).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from det.ingestion.iceberg_catalog_factory import (
    ENV_CATALOG,
    resolve_iceberg_catalog,
)
from det.ingestion.iceberg_writer import scan_iceberg_rows
from det.runtime.iceberg_register import apply_iceberg_register, build_iceberg_register_plan
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import ENV_LAKE_MODE, open_lake
from det.runtime.runner import PipelineRunner

_ENDPOINT = (os.environ.get("AWS_ENDPOINT_URL") or "").strip()
_REST_URI = (os.environ.get("DET_ICEBERG_REST_URI") or "").strip()
_KEY = (os.environ.get("AWS_ACCESS_KEY_ID") or "minioadmin").strip()
_SECRET = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "minioadmin").strip()
_REGION = (
    os.environ.get("AWS_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
    or "us-east-1"
).strip()
_BUCKET = (os.environ.get("DET_MINIO_BUCKET") or "det-ci").strip()
_LAKE_URI = f"s3://{_BUCKET}/det-lake"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.polaris,
    pytest.mark.skipif(not _ENDPOINT, reason="AWS_ENDPOINT_URL not set"),
    pytest.mark.skipif(not _REST_URI, reason="DET_ICEBERG_REST_URI not set"),
]

SOAK_ROWS = 12


def _ensure_bucket() -> None:
    pytest.importorskip("s3fs")
    import fsspec

    from det.runtime.object_store import fsspec_s3_kwargs

    fs = fsspec.filesystem("s3", **fsspec_s3_kwargs())
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
    # Unique Iceberg table id per soak (shared Polaris catalog); keep name/source.
    wire_version = abs(hash(tmp_path.name)) % 900_000 + 100_000
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "wire_version": wire_version,
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


def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("AWS_ENDPOINT_URL", _ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRET)
    monkeypatch.setenv("AWS_REGION", _REGION)


def _rest_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv("DET_ICEBERG_REST_URI", _REST_URI)
    warehouse = (os.environ.get("DET_ICEBERG_REST_WAREHOUSE") or "det_lake").strip()
    monkeypatch.setenv("DET_ICEBERG_REST_WAREHOUSE", warehouse)
    cred = (os.environ.get("DET_ICEBERG_REST_CREDENTIAL") or "root:s3cr3t").strip()
    monkeypatch.setenv("DET_ICEBERG_REST_CREDENTIAL", cred)
    scope = (os.environ.get("DET_ICEBERG_REST_SCOPE") or "PRINCIPAL_ROLE:ALL").strip()
    monkeypatch.setenv("DET_ICEBERG_REST_SCOPE", scope)
    realm = (os.environ.get("DET_ICEBERG_REST_REALM") or "POLARIS").strip()
    monkeypatch.setenv("DET_ICEBERG_REST_REALM", realm)


def test_polaris_hadoop_write_then_register(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    pytest.importorskip("s3fs")
    _ensure_bucket()

    lake_uri = f"{_LAKE_URI}/register-{tmp_path.name}"
    _aws_env(monkeypatch)
    monkeypatch.setenv("DET_LAKE_PATH", lake_uri)
    monkeypatch.delenv(ENV_CATALOG, raising=False)
    monkeypatch.delenv("DET_ICEBERG_REST_URI", raising=False)

    pipe = _pipe(tmp_path, project_root)
    runner = PipelineRunner(project_root=tmp_path)
    result = runner.run(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
    assert result.rows == SOAK_ROWS

    _rest_env(monkeypatch)
    plan = build_iceberg_register_plan(
        project_root=tmp_path,
        lake_path=lake_uri,
        pipeline=pipe,
        include_ops=False,
    )
    assert len(plan.tables) == 1
    result_reg = apply_iceberg_register(plan, project_root=tmp_path)
    assert result_reg["count"] == 1
    assert result_reg["applied"][0]["status"] == "registered"

    from det.runtime.config import load_pipeline_config

    config = load_pipeline_config(pipe)
    ns, table = sql_names_for_config(config)
    lake = open_lake(lake_uri, tmp_path)
    catalog = resolve_iceberg_catalog(lake)
    ice = catalog.load_table((ns, table))
    # Avoid scan().to_arrow() — PyArrow dataset collides on DET __filename meta.
    rows = scan_iceberg_rows(ice, limit=SOAK_ROWS + 5)
    assert len(rows) == SOAK_ROWS


def test_polaris_greenfield_rest_write(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    pytest.importorskip("s3fs")
    _ensure_bucket()

    lake_uri = f"{_LAKE_URI}/greenfield-{tmp_path.name}"
    _aws_env(monkeypatch)
    _rest_env(monkeypatch)
    monkeypatch.setenv("DET_LAKE_PATH", lake_uri)

    pipe = _pipe(tmp_path, project_root)
    runner = PipelineRunner(project_root=tmp_path)
    result = runner.run(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
    assert result.rows == SOAK_ROWS

    from det.runtime.config import load_pipeline_config

    config = load_pipeline_config(pipe)
    ns, table = sql_names_for_config(config)
    lake = open_lake(lake_uri, tmp_path)
    catalog = resolve_iceberg_catalog(lake)
    ice = catalog.load_table((ns, table))
    rows = scan_iceberg_rows(ice, limit=SOAK_ROWS + 5)
    assert len(rows) == SOAK_ROWS
