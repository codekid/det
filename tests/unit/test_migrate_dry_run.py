from __future__ import annotations

import json
from pathlib import Path

import yaml

from det.mcp import tools as mcp_tools
from det.mcp.server import create_server
from det.runtime.migrate import BronzeMigrator, MigratePlan
from det.runtime.runner import PipelineRunner


def _example_pipeline(project_root: Path, tmp_path: Path, *, severity: str = "high") -> Path:
    pipeline = {
        "name": "example_api.events",
        "source": {
            "type": "example_api.events",
            "overrides": {
                "fixture_records": [
                    {
                        "id": "e1",
                        "occurred_at": "2026-08-06T12:00:00Z",
                        "severity": severity,
                        "state": "TX",
                    }
                ]
            },
        },
        "schema": str(project_root / "schemas/example_api/events/events.schema.yaml"),
        "ingestion": {"library": "thin"},
        "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "api.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    return pipe_path


def test_migrate_dry_run_reports_plan_without_writing(
    project_root: Path, tmp_path: Path
):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    bronze_v2 = tmp_path / "lake" / "bronze" / "example_api" / "events_v2"
    assert not bronze_v2.exists()

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v2",
        schema_path=project_root / "schemas/example_api/events/events_v2.schema.yaml",
        mapper_name="example_api_v1_to_v2",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.dry_run is True
    assert plan.ok is True
    assert plan.partitions_planned == 1
    assert plan.rows_checked == 1
    assert plan.mapper_name == "example_api_v1_to_v2"
    part = plan.partitions[0]
    assert part.ok is True
    assert part.rows == 1
    assert part.would_write_bronze_path is not None
    assert "events_v2" in part.would_write_bronze_path
    assert not bronze_v2.exists() or not any(bronze_v2.rglob("data.jsonl"))


def test_migrate_dry_run_validation_errors_no_write(
    project_root: Path, tmp_path: Path
):
    """Identity mapper leaves severity; v2 schema requires level → fail, no write."""
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v2",
        schema_path=project_root / "schemas/example_api/events/events_v2.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.ok is False
    assert plan.partitions[0].ok is False
    assert plan.partitions[0].errors
    bronze_v2 = tmp_path / "lake" / "bronze" / "example_api" / "events_v2"
    assert not bronze_v2.exists() or not any(bronze_v2.rglob("data.jsonl"))


def test_migrate_live_still_writes(project_root: Path, tmp_path: Path):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    result = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v2",
        schema_path=project_root / "schemas/example_api/events/events_v2.schema.yaml",
        mapper_name="example_api_v1_to_v2",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=False,
    )
    assert result.rows == 1
    out = next(
        (tmp_path / "lake" / "bronze" / "example_api" / "events_v2").rglob("data.jsonl")
    )
    assert json.loads(out.read_text(encoding="utf-8").splitlines()[0])["level"] == "high"


def test_mcp_migrate_dry_run_tool(project_root: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    pipe_dir = tmp_path / "configs" / "pipelines" / "example_api"
    pipe_dir.mkdir(parents=True)
    schema_v2 = (
        project_root / "schemas/example_api/events/events_v2.schema.yaml"
    ).read_text(encoding="utf-8")
    schema_v1 = (
        project_root / "schemas/example_api/events/events.schema.yaml"
    ).read_text(encoding="utf-8")
    (tmp_path / "schemas/example_api/events").mkdir(parents=True)
    (tmp_path / "schemas/example_api/events/events.schema.yaml").write_text(
        schema_v1, encoding="utf-8"
    )
    (tmp_path / "schemas/example_api/events/events_v2.schema.yaml").write_text(
        schema_v2, encoding="utf-8"
    )
    pipe_path = pipe_dir / "events.yaml"
    pipe_path.write_text(
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
                                "severity": "high",
                                "state": "TX",
                            }
                        ]
                    },
                },
                "schema": "schemas/example_api/events/events.schema.yaml",
                "ingestion": {"library": "thin"},
                "destination": {
                    "type": "filesystem",
                    "path": str(tmp_path / "lake"),
                },
                "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
            }
        ),
        encoding="utf-8",
    )
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    out = mcp_tools.migrate_dry_run(
        "example_api.events",
        "example_api.events_v2",
        "schemas/example_api/events/events_v2.schema.yaml",
        "example_api_v1_to_v2",
        "2026-08-06",
        root=tmp_path,
    )
    assert out["dry_run"] is True
    assert out["ok"] is True
    assert out["rows_checked"] == 1
    assert out["validate_limit"] == 50
    bronze_v2 = tmp_path / "lake" / "bronze" / "example_api" / "events_v2"
    assert not bronze_v2.exists() or not any(bronze_v2.rglob("data.jsonl"))


def test_create_server_registers_migrate_dry_run():
    server = create_server()
    names = sorted(server._tool_manager._tools)
    assert "migrate_dry_run" in names
