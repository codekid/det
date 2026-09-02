"""MinIO layout-2 soak: three arbitrary buckets for raw / bronze / ops.

Skipped unless ``AWS_ENDPOINT_URL`` is set (CI MinIO service). Requires
``det[s3]`` + ``det[iceberg]``. Bucket names are random UUIDs so DET never
depends on fixed names like ``det-raw``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import yaml

from det.ingestion.iceberg_writer import load_iceberg_table, scan_iceberg_rows
from det.runtime.lake import ENV_LAKE_MODE, open_lake, resolve_lake_roots
from det.runtime.manifest import read_manifest
from det.runtime.runner import PipelineRunner
from det.runtime.settings import DetSettings

_ENDPOINT = (os.environ.get("AWS_ENDPOINT_URL") or "").strip()
_KEY = (os.environ.get("AWS_ACCESS_KEY_ID") or "minioadmin").strip()
_SECRET = (os.environ.get("AWS_SECRET_ACCESS_KEY") or "minioadmin").strip()
_REGION = (
    os.environ.get("AWS_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
    or "us-east-1"
).strip()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.minio,
    pytest.mark.skipif(not _ENDPOINT, reason="AWS_ENDPOINT_URL not set"),
]

SOAK_ROWS = 10


def _ensure_bucket(name: str) -> None:
    pytest.importorskip("s3fs")
    import fsspec

    from det.runtime.object_store import fsspec_s3_kwargs

    fs = fsspec.filesystem("s3", **fsspec_s3_kwargs())
    if not fs.exists(name):
        fs.mkdir(name)


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


def test_minio_split_three_buckets_extract_load(
    project_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("s3fs")

    suffix = uuid.uuid4().hex[:12]
    raw_bucket = f"ci-raw-{suffix}"
    bronze_bucket = f"ci-bronze-{suffix}"
    ops_bucket = f"ci-ops-{suffix}"
    raw_uri = f"s3://{raw_bucket}"
    bronze_uri = f"s3://{bronze_bucket}"
    ops_uri = f"s3://{ops_bucket}"

    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("AWS_ENDPOINT_URL", _ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRET)
    monkeypatch.setenv("AWS_REGION", _REGION)
    for key in (
        "DET_LAKE_PATH",
        "DET_LAKE_PATH_RAW",
        "DET_LAKE_PATH_BRONZE",
        "DET_LAKE_PATH_OPS",
    ):
        monkeypatch.delenv(key, raising=False)

    for name in (raw_bucket, bronze_bucket, ops_bucket):
        _ensure_bucket(name)

    pipe = _pipe(tmp_path, project_root)
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=raw_uri,
        lake_path_bronze=bronze_uri,
        lake_path_ops=ops_uri,
        locks_enabled=False,
    )
    runner = PipelineRunner(settings=settings)
    result = runner.run(
        pipe,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == SOAK_ROWS
    assert result.raw_dir is not None
    assert raw_bucket in str(result.raw_dir)
    assert "/raw/" not in str(result.raw_dir).replace(raw_uri, "")
    manifest = read_manifest(result.raw_dir)
    assert manifest["lake_layout"] == 2

    assert result.partition_dir is not None
    assert bronze_bucket in str(result.partition_dir)
    assert "/bronze/" not in str(result.partition_dir).replace(bronze_uri, "")

    roots = resolve_lake_roots(settings, project_root=tmp_path)
    bronze_lake = open_lake(str(roots.bronze), tmp_path)
    ice = load_iceberg_table(
        lake=bronze_lake,
        namespace="bronze_example_api",
        table="events_v1",
        table_location=result.partition_dir,
    )
    assert ice is not None
    rows = scan_iceberg_rows(ice, limit=SOAK_ROWS + 5)
    assert len(rows) == SOAK_ROWS
