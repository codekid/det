"""Failure-injection tests for docs/publication-contract.md §4.

Already covered elsewhere (not duplicated here):

- Crash during ``data/`` + incomplete retry:
  ``tests/unit/test_atomic_extract.py``
- Lease holder dead → steal: ``tests/unit/test_lease.py``
  (``test_expired_steal``)
- Commit visibility / load gate / conflict:
  ``tests/contract/test_publication.py``
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from det.errors import DetNotFoundError
from det.runtime.manifest import is_committed_raw_dir, read_manifest
from det.runtime.meta import to_partition_value
from det.runtime.runner import PipelineRunner


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


def test_crash_before_manifest_publish_cleans_prefix(
    project_root: Path, tmp_path: Path
):
    """Catalog: crash before manifest → orphan cleaned, not committed."""
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)

    with (
        patch(
            "det.runtime.runner.write_manifest",
            side_effect=RuntimeError("manifest publish crashed"),
        ),
        pytest.raises(RuntimeError, match="manifest publish crashed"),
    ):
        runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    raw_root = tmp_path / "lake" / "raw"
    assert list(raw_root.rglob("manifest.json")) == []
    # Prefix under the interval must be gone (rmtree of incomplete extract-run).
    assert list(raw_root.rglob("data")) == []


def test_crash_after_manifest_load_without_reextract(
    project_root: Path, tmp_path: Path
):
    """Catalog: crash after manifest → COMMITTED; operator runs load."""
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    assert is_committed_raw_dir(extracted.raw_dir)

    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert loaded.rows == 1
    assert loaded.raw_dir == extracted.raw_dir
    assert read_manifest(extracted.raw_dir).get("validation", {}).get("ok") is True


def test_crash_during_bronze_write_reload_succeeds(
    project_root: Path, tmp_path: Path
):
    """Catalog: crash mid-bronze → raw committed; re-load replace-by-run."""
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    assert is_committed_raw_dir(extracted.raw_dir)

    def boom_write(*args, **kwargs):
        raise RuntimeError("bronze write crashed")

    with (
        patch(
            "det.ingestion.det_backend.write_jsonl_partition",
            side_effect=boom_write,
        ),
        pytest.raises(Exception, match="bronze write crashed"),
    ):
        runner.load(
            pipe,
            interval_start=extracted.interval_start,
            interval_end=extracted.interval_end,
            extract_run_datetime=extracted.extract_run_datetime,
        )

    assert is_committed_raw_dir(extracted.raw_dir)
    bronze_jsonl = list((tmp_path / "lake" / "bronze").rglob("data.jsonl"))
    assert bronze_jsonl == []

    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert loaded.rows == 1
    assert (loaded.partition_dir / "data.jsonl").is_file()


def test_crash_after_bronze_before_validation_stamp(
    project_root: Path, tmp_path: Path
):
    """Catalog: bronze may be complete without validation; re-load stamps."""
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )

    with (
        patch(
            "det.runtime.runner.stamp_validation_success",
            side_effect=RuntimeError("validation stamp crashed"),
        ),
        pytest.raises(RuntimeError, match="validation stamp crashed"),
    ):
        runner.load(
            pipe,
            interval_start=extracted.interval_start,
            interval_end=extracted.interval_end,
            extract_run_datetime=extracted.extract_run_datetime,
        )

    assert is_committed_raw_dir(extracted.raw_dir)
    assert "validation" not in read_manifest(extracted.raw_dir)
    bronze_files = list((tmp_path / "lake" / "bronze").rglob("data.jsonl"))
    assert len(bronze_files) == 1
    assert bronze_files[0].is_file()

    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert loaded.rows == 1
    assert read_manifest(extracted.raw_dir)["validation"]["ok"] is True


def test_corrupt_manifest_not_committed_reextract_recovers(
    project_root: Path, tmp_path: Path
):
    """Catalog: corrupt manifest JSON → not committed; re-extract recovers."""
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    stamp = "2026-08-06T12:00:00+00:00"
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
    (run_dir / "meta").mkdir(parents=True)
    (run_dir / "meta" / "manifest.json").write_text("{not-json", encoding="utf-8")
    assert not is_committed_raw_dir(run_dir)

    with pytest.raises(DetNotFoundError, match="No committed raw extract"):
        runner.load(
            pipe,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            extract_run_datetime=stamp,
        )

    result = runner.extract(
        pipe,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        extract_run_datetime=stamp,
    )
    assert is_committed_raw_dir(result.raw_dir)
    assert result.raw_dir == run_dir
    assert not (result.raw_dir / "data" / "partial.bin").exists()
    read_manifest(result.raw_dir)  # valid JSON commit
