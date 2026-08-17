"""Streaming load soak: thousands of fixture rows, not a 5-row peek."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.runtime.runner import PipelineRunner

SOAK_ROWS = 2500
CHUNK_ROWS = 250


def _soak_pipe(tmp_path: Path, project_root: Path) -> Path:
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
                "ingestion": {"chunk_rows": CHUNK_ROWS},
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
            }
        ),
        encoding="utf-8",
    )
    return pipe


@pytest.mark.integration
def test_streaming_load_soak_keeps_all_rows(project_root: Path, tmp_path: Path):
    pipe = _soak_pipe(tmp_path, project_root)
    result = PipelineRunner(tmp_path).run(
        pipe,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == SOAK_ROWS
    lines = (result.partition_dir / "data.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == SOAK_ROWS
    first = json.loads(lines[0])
    last = json.loads(lines[-1])
    assert first["id"] == "e0"
    assert last["id"] == f"e{SOAK_ROWS - 1}"
    assert "__row_hash" in first
    assert "__extract_run_datetime" in first
