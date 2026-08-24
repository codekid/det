"""Refuse dlt-shaped raw/bronze at the DET boundary."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from det.errors import DetContractError
from det.runtime.check import check_pipeline_config
from det.runtime.dlt_hygiene import (
    check_raw_hygiene,
    first_dlt_key,
    refuse_dlt_keys,
)
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


def test_first_dlt_key_nested() -> None:
    assert first_dlt_key({"id": 1}) is None
    assert first_dlt_key({"_dlt_id": "x"}) == "_dlt_id"
    assert first_dlt_key({"data": {"records": [{"_dlt_load_id": "1"}]}}) == "_dlt_load_id"


def test_refuse_dlt_keys_raises() -> None:
    with pytest.raises(DetContractError, match="dlt.pipeline"):
        refuse_dlt_keys({"_dlt_id": "x"})


def test_check_raw_hygiene_rejects_dlt_page(tmp_path: Path) -> None:
    raw = tmp_path / "raw_run"
    page = raw / "data" / "pages" / "0001.json"
    page.parent.mkdir(parents=True)
    page.write_text('{"data": {"events": [{"id": "1", "_dlt_id": "abc"}]}}', encoding="utf-8")
    with pytest.raises(DetContractError, match="_dlt_id"):
        check_raw_hygiene(
            raw,
            [{"path": "data/pages/0001.json", "origin": "fixture"}],
        )


def test_check_raw_hygiene_rejects_state_dir(tmp_path: Path) -> None:
    raw = tmp_path / "raw_run"
    (raw / "data").mkdir(parents=True)
    (raw / "_dlt_pipeline_state").mkdir()
    with pytest.raises(DetContractError, match="_dlt_pipeline_state"):
        check_raw_hygiene(raw, [])


def test_extract_refuses_dlt_shaped_page(project_root: Path, tmp_path: Path) -> None:
    pipe = _example_pipe(tmp_path, project_root)

    def plant_dlt(self, *, config, interval, data_dir):
        pages = data_dir / "pages"
        pages.mkdir(parents=True, exist_ok=True)
        dest = pages / "0001.json"
        dest.write_text(
            '{"data": {"events": [{"id": "e1", "_dlt_id": "bad"}]}}',
            encoding="utf-8",
        )
        return [
            {
                "path": "data/pages/0001.json",
                "origin": "fixture_records",
                "sha256": "x",
                "bytes": dest.stat().st_size,
                "format": "json_page",
                "content_encoding": "identity",
                "format_check": "ok",
            }
        ]

    runner = PipelineRunner(tmp_path)
    with (
        patch.object(ExampleApiSource, "extract_to_raw", plant_dlt),
        pytest.raises(DetContractError, match="dlt.pipeline"),
    ):
        runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    raw_root = tmp_path / "lake" / "raw"
    assert list(raw_root.rglob("manifest.json")) == []


def test_load_refuses_dlt_row_keys(project_root: Path, tmp_path: Path) -> None:
    pipe = _example_pipe(tmp_path, project_root)
    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )

    def bad_rows(self, *, config, raw_dir, manifest):
        from det.sources.base import SourceRow

        yield SourceRow(
            data={
                "id": "e1",
                "_dlt_load_id": "1",
                "occurred_at": "2026-08-06T12:00:00Z",
                "severity": "low",
                "state": "TX",
                "status": "1",
            }
        )
    with (
        patch.object(ExampleApiSource, "records_from_raw", bad_rows),
        pytest.raises(DetContractError, match="_dlt_load_id"),
    ):
        runner.load(
            pipe,
            interval_start=extracted.interval_start,
            interval_end=extracted.interval_end,
            extract_run_datetime=extracted.extract_run_datetime,
        )


def test_check_flags_dlt_state_on_lake(tmp_path: Path) -> None:
    schema = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        yaml.safe_dump(
            {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
            }
        ),
        encoding="utf-8",
    )
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
    lake = tmp_path / "lake"
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {"type": "example_api.events"},
                "schema": "schemas/example_api/events/events.schema.yaml",
                "destination": {"type": "filesystem", "path": str(lake)},
            }
        ),
        encoding="utf-8",
    )
    # Dataset layout: bronze/example_api/events_v1/...
    state = lake / "bronze" / "example_api" / "events_v1" / "_dlt_loads"
    state.mkdir(parents=True)

    findings = check_pipeline_config(pipe, project_root=tmp_path)
    assert any(f.code == "dlt_state_on_lake" for f in findings)
