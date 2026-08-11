"""Raw manifest validation success receipt."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.plugins import load_plugins
from det.runtime.manifest import (
    read_manifest,
    sha256_file,
    stamp_validation_success,
    write_manifest,
)
from det.runtime.runner import PipelineRunner
from det.validation.jsonschema_validator import SchemaValidationError


def test_stamp_validation_success_merges_receipt(tmp_path: Path):
    raw_dir = tmp_path / "raw_part"
    write_manifest(
        raw_dir,
        {
            "source": "example_api.events",
            "wire_version": 1,
            "artifacts": [],
        },
    )
    stamp_validation_success(
        raw_dir,
        schema_path="schemas/example_api/events/events.schema.yaml",
        schema_sha256="deadbeef",
        row_count=2,
        wire_version=1,
        validated_at="2026-08-11T12:00:00+00:00",
    )
    payload = read_manifest(raw_dir)
    assert payload["source"] == "example_api.events"
    assert payload["validation"] == {
        "ok": True,
        "validated_at": "2026-08-11T12:00:00+00:00",
        "schema_path": "schemas/example_api/events/events.schema.yaml",
        "schema_sha256": "deadbeef",
        "row_count": 2,
        "wire_version": 1,
    }


def test_extract_has_no_validation_load_stamps_success(
    project_root: Path, tmp_path: Path
):
    load_plugins()
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
                "wire_version": 1,
            }
        ),
        encoding="utf-8",
    )

    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
    pre = read_manifest(extracted.raw_dir)
    assert "validation" not in pre

    loaded = runner.load(
        pipe,
        interval_start=extracted.interval_start,
        interval_end=extracted.interval_end,
        extract_run_datetime=extracted.extract_run_datetime,
    )
    assert loaded.rows == 1
    post = read_manifest(extracted.raw_dir)
    assert post["validation"]["ok"] is True
    assert post["validation"]["row_count"] == 1
    assert post["validation"]["schema_path"] == "schemas/example_api/events/events.schema.yaml"
    assert post["validation"]["schema_sha256"] == sha256_file(schema_dst)
    assert post["validation"]["wire_version"] == 1
    assert "validated_at" in post["validation"]


def test_failed_validate_does_not_stamp(project_root: Path, tmp_path: Path):
    load_plugins()
    # Closed schema that rejects the fixture's severity field name... use a tiny
    # schema that requires an unknown property so fixture rows fail validation.
    schema_dst = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["id", "must_exist"],
                "properties": {
                    "id": {"type": "string"},
                    "must_exist": {"type": "string"},
                    "occurred_at": {"type": ["string", "null"]},
                    "severity": {"type": ["string", "null"]},
                    "state": {"type": ["string", "null"]},
                    "status": {"type": ["string", "null"]},
                },
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )

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

    runner = PipelineRunner(tmp_path)
    extracted = runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
    with pytest.raises(SchemaValidationError):
        runner.load(
            pipe,
            interval_start=extracted.interval_start,
            interval_end=extracted.interval_end,
            extract_run_datetime=extracted.extract_run_datetime,
        )
    assert "validation" not in read_manifest(extracted.raw_dir)
    bronze = tmp_path / "lake" / "bronze" / "example_api" / "events"
    assert not list(bronze.rglob("data.jsonl")) if bronze.exists() else True
