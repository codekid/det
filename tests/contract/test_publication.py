"""Contract tests for docs/publication-contract.md invariants."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.errors import DetConflictError, DetNotFoundError
from det.ingestion.sql_replace import require_bronze_run_identity
from det.runtime.manifest import (
    committed_extract_run_dirs,
    is_committed_raw_dir,
    write_manifest,
)
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


def test_incomplete_prefix_and_tmp_are_not_committed(tmp_path: Path):
    """Invariant 1: data or manifest.json.tmp alone is not a commit."""
    raw_dir = tmp_path / "run"
    (raw_dir / "data").mkdir(parents=True)
    (raw_dir / "data" / "partial.bin").write_bytes(b"x")
    assert not is_committed_raw_dir(raw_dir)

    (raw_dir / "meta").mkdir()
    (raw_dir / "meta" / "manifest.json.tmp").write_text("{}", encoding="utf-8")
    assert not is_committed_raw_dir(raw_dir)

    write_manifest(raw_dir, {"extract_run_datetime": "2026-08-06T12:00:00+00:00"})
    assert is_committed_raw_dir(raw_dir)


def test_committed_extract_run_dirs_skips_orphans(tmp_path: Path):
    """Latest/list committed runs ignore incomplete sibling prefixes."""
    interval_end = tmp_path / "interval_end"
    orphan = interval_end / "__extract_run_datetime=20990101T000000Z"
    (orphan / "data").mkdir(parents=True)
    (orphan / "data" / "orphan.bin").write_bytes(b"x")
    assert not is_committed_raw_dir(orphan)

    committed = interval_end / "__extract_run_datetime=20260806T120000Z"
    write_manifest(
        committed,
        {"extract_run_datetime": "2026-08-06T12:00:00+00:00"},
    )
    runs = committed_extract_run_dirs(interval_end)
    assert runs == [committed]


def test_load_requires_committed_raw(project_root: Path, tmp_path: Path):
    """Invariant 3: load fails closed when the extract-run prefix is not committed."""
    pipe = _example_pipe(tmp_path, project_root)
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
    assert not is_committed_raw_dir(run_dir)

    runner = PipelineRunner(tmp_path)
    with pytest.raises(DetNotFoundError, match="No committed raw extract"):
        runner.load(
            pipe,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            extract_run_datetime=stamp,
        )


def test_extract_refuses_overwrite_of_committed_run(
    project_root: Path, tmp_path: Path
):
    """Invariant 2: re-extract of a committed prefix conflicts."""
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


def test_replace_by_run_requires_stable_run_identity():
    """Invariant 3 (bronze): replace-by-run rejects missing or mixed run meta."""
    with pytest.raises(ValueError, match="replace-by-run requires"):
        require_bronze_run_identity([{"id": 1}])
    with pytest.raises(ValueError, match="does not match the batch"):
        require_bronze_run_identity(
            [
                {
                    "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
                    "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
                    "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
                },
                {
                    "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
                    "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
                    "__extract_run_datetime": "2026-08-06T11:00:00+00:00",
                },
            ]
        )


def test_filesystem_bronze_commit_gates_list_bronze_runs(
    project_root: Path, tmp_path: Path
):
    """Filesystem bronze: data.jsonl alone is invisible; manifest publishes it."""
    from det.runtime.bronze_runs import list_bronze_runs
    from det.runtime.config import load_pipeline_config
    from det.runtime.manifest import is_committed_raw_dir

    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )
    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert is_committed_raw_dir(loaded.partition_dir)
    assert (loaded.partition_dir / "data.jsonl").is_file()
    assert (loaded.partition_dir / "meta" / "manifest.json").is_file()

    config = load_pipeline_config(pipe)
    listed, note = list_bronze_runs(config, root=tmp_path, limit=10)
    assert note is None
    assert len(listed) == 1

    # Strip commit → listing must hide the incomplete/uncommitted prefix.
    (loaded.partition_dir / "meta" / "manifest.json").unlink()
    assert not is_committed_raw_dir(loaded.partition_dir)
    listed2, _ = list_bronze_runs(config, root=tmp_path, limit=10)
    assert listed2 == []
