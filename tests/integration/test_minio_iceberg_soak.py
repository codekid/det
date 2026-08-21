"""MinIO object-lake soak: extract → Iceberg bronze → DuckDB iceberg_scan.

Skipped unless ``AWS_ENDPOINT_URL`` is set (CI MinIO service). Requires
``det[s3]`` + ``det[iceberg]``.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from det.ingestion.iceberg_writer import load_iceberg_table, scan_iceberg_rows
from det.runtime.lake import ENV_LAKE_MODE, open_lake
from det.runtime.runner import PipelineRunner

_ENDPOINT = (os.environ.get("AWS_ENDPOINT_URL") or "").strip()
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
    pytest.mark.minio,
    pytest.mark.skipif(not _ENDPOINT, reason="AWS_ENDPOINT_URL not set"),
]

SOAK_ROWS = 25


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


def _configure_duckdb_s3(con) -> None:
    host = urlparse(_ENDPOINT)
    endpoint_host = host.netloc or host.path
    use_ssl = "true" if host.scheme == "https" else "false"
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET det_minio (
            TYPE s3,
            KEY_ID '{_KEY}',
            SECRET '{_SECRET}',
            REGION '{_REGION}',
            ENDPOINT '{endpoint_host}',
            URL_STYLE 'path',
            USE_SSL {use_ssl}
        )
        """
    )


def test_minio_extract_load_iceberg_duckdb_scan(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("s3fs")
    duckdb = pytest.importorskip("duckdb")

    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("DET_LAKE_PATH", _LAKE_URI)
    monkeypatch.setenv("AWS_ENDPOINT_URL", _ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRET)
    monkeypatch.setenv("AWS_REGION", _REGION)

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

    lake = open_lake(_LAKE_URI, tmp_path)
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

    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con)
    except Exception as exc:  # pragma: no cover - extension / secret quirks
        pytest.skip(f"duckdb s3/iceberg setup failed: {exc}")
    scan_uri = f"{_LAKE_URI.rstrip('/')}/bronze/example_api/events_v1"
    n = con.execute(f"SELECT count(*) FROM iceberg_scan('{scan_uri}')").fetchone()[0]
    assert n == SOAK_ROWS
