from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from det.errors import DetConflictError, DetPluginError
from det.runtime.manifest import is_committed_raw_dir, read_manifest, write_manifest
from det.runtime.runner import PipelineRunner
from det.sources.example_api.events import ExampleApiSource


def _example_pipe(tmp_path: Path, project_root: Path) -> Path:
    schema_src = project_root / "schemas/example_api/events/events.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
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
                "schema": "schemas/example_api/events/events.schema.yaml",
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
            }
        ),
        encoding="utf-8",
    )
    return pipe


def test_write_manifest_atomic_replace_leaves_no_tmp(tmp_path: Path):
    raw_dir = tmp_path / "run"
    dest = write_manifest(raw_dir, {"extract_run_datetime": "2026-08-06T12:00:00+00:00"})
    assert dest.name == "manifest.json"
    assert dest.exists()
    assert not dest.with_name("manifest.json.tmp").exists()
    assert is_committed_raw_dir(raw_dir)
    assert read_manifest(raw_dir)["extract_run_datetime"] == "2026-08-06T12:00:00+00:00"


def test_incomplete_prefix_is_not_committed(tmp_path: Path):
    raw_dir = tmp_path / "run"
    (raw_dir / "data").mkdir(parents=True)
    (raw_dir / "data" / "partial.bin").write_bytes(b"x")
    assert not is_committed_raw_dir(raw_dir)
    (raw_dir / "meta").mkdir()
    (raw_dir / "meta" / "manifest.json.tmp").write_text("{}", encoding="utf-8")
    assert not is_committed_raw_dir(raw_dir)


def test_failed_extract_does_not_commit_and_cleans_prefix(
    project_root: Path, tmp_path: Path
):
    pipe = _example_pipe(tmp_path, project_root)

    def boom(self, *, config, interval, data_dir):
        (data_dir / "partial.bin").write_bytes(b"truncated")
        raise RuntimeError("download failed")

    runner = PipelineRunner(tmp_path)
    with (
        patch.object(ExampleApiSource, "extract_to_raw", boom),
        pytest.raises(DetPluginError, match="download failed"),
    ):
        runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    raw_root = tmp_path / "lake" / "raw"
    assert list(raw_root.rglob("manifest.json")) == []
    assert list(raw_root.rglob("partial.bin")) == []


def test_load_skips_newer_incomplete_sibling(project_root: Path, tmp_path: Path):
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    incomplete = extracted.raw_dir.parent / "__extract_run_datetime=20990101T000000Z"
    (incomplete / "data").mkdir(parents=True)
    (incomplete / "data" / "orphan.bin").write_bytes(b"x")

    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
    )
    assert loaded.raw_dir == extracted.raw_dir
    assert loaded.rows == 1


def test_extract_refuses_overwrite_of_committed_run(
    project_root: Path, tmp_path: Path
):
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    stamp = "2026-08-06T12:00:00+00:00"
    first = runner.extract(
        pipe,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        extract_run_datetime=stamp,
    )
    assert is_committed_raw_dir(first.raw_dir)
    with pytest.raises(DetConflictError, match="Committed raw extract"):
        runner.extract(
            pipe,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            extract_run_datetime=stamp,
        )
    assert is_committed_raw_dir(first.raw_dir)
    assert read_manifest(first.raw_dir)["extract_run_datetime"] == stamp
    assert read_manifest(first.raw_dir)["lake_layout"] == 1


def test_extract_retries_incomplete_same_run_id(project_root: Path, tmp_path: Path):
    from det.runtime.meta import to_partition_value

    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    stamp = "2026-08-06T12:00:00+00:00"
    # Simulate kill -9: bytes at final keys, no commit object.
    run_dir = (
        tmp_path
        / "lake"
        / "raw"
        / "example_api"
        / "events_v1"
        / f"__interval_start_datetime={to_partition_value('2026-08-06T00:00:00+00:00')}"
        / f"__interval_end_datetime={to_partition_value('2026-08-07T00:00:00+00:00')}"
        / f"__extract_run_datetime={to_partition_value(stamp)}"
    )
    (run_dir / "data").mkdir(parents=True)
    (run_dir / "data" / "partial.bin").write_bytes(b"x")
    assert not is_committed_raw_dir(run_dir)

    result = runner.extract(
        pipe,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        extract_run_datetime=stamp,
    )
    assert is_committed_raw_dir(result.raw_dir)
    assert result.raw_dir == run_dir
    assert not (result.raw_dir / "data" / "partial.bin").exists()
