"""Layout 2 split roots: extract raw and load bronze on separate lake URIs."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.runtime.lake import clear_memory_lakes, reset_lake_mode_warning_for_tests
from det.runtime.manifest import read_manifest
from det.runtime.runner import PipelineRunner
from det.runtime.settings import DetSettings


@pytest.fixture(autouse=True)
def _reset_memory(monkeypatch: pytest.MonkeyPatch):
    clear_memory_lakes()
    reset_lake_mode_warning_for_tests()
    monkeypatch.delenv("DET_LAKE_MODE", raising=False)
    for key in (
        "DET_LAKE_PATH",
        "DET_LAKE_PATH_RAW",
        "DET_LAKE_PATH_BRONZE",
        "DET_LAKE_PATH_OPS",
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    clear_memory_lakes()
    reset_lake_mode_warning_for_tests()


def test_split_memory_lakes_extract_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_LOCK", "0")
    monkeypatch.setenv("DET_RUN_RECEIPTS", "0")

    raw_uri = "memory://split-raw"
    bronze_uri = "memory://split-bronze"
    ops_uri = "memory://split-ops"

    # Minimal pipeline under project root.
    pipe_dir = tmp_path / "configs" / "pipelines" / "example_api"
    pipe_dir.mkdir(parents=True)
    schema_dir = tmp_path / "schemas" / "example_api" / "events"
    schema_dir.mkdir(parents=True)
    (schema_dir / "events.schema.yaml").write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "occurred_at": {"type": "string"},
                    "severity": {"type": "string"},
                    "state": {"type": "string"},
                    "status": {"type": "string"},
                },
                "required": ["id", "occurred_at"],
            }
        ),
        encoding="utf-8",
    )
    (pipe_dir / "events.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {
                    "type": "example_api.events",
                    "overrides": {
                        "fixture_records": [
                            {
                                "id": "e1",
                                "occurred_at": "2026-08-06T12:00:00Z",
                                "severity": "low",
                                "state": "TX",
                                "status": "1",
                            }
                        ]
                    },
                },
                "destination": {"type": "filesystem"},
                "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
                "wire_version": 1,
            }
        ),
        encoding="utf-8",
    )

    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=raw_uri,
        lake_path_bronze=bronze_uri,
        lake_path_ops=ops_uri,
        locks_enabled=False,
    )
    runner = PipelineRunner(settings=settings)
    extracted = runner.extract("example_api.events", interval_start="2026-08-06")
    assert "memory://split-raw" in str(extracted.raw_dir)
    assert "/raw/" not in str(extracted.raw_dir).replace("memory://split-raw", "")
    manifest = read_manifest(extracted.raw_dir)
    assert manifest["lake_layout"] == 2

    loaded = runner.load(
        "example_api.events",
        interval_start="2026-08-06",
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert "memory://split-bronze" in str(loaded.partition_dir)
    assert loaded.rows == 1
