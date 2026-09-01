from __future__ import annotations

import json
from pathlib import Path

import pytest
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

    bronze_v2 = tmp_path / "lake" / "bronze" / "example_api" / "events_level"
    assert not bronze_v2.exists()

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_level",
        schema_path=project_root / "tests/fixtures/example_api/events_level.schema.yaml",
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
    assert "events_level" in part.would_write_bronze_path
    assert not bronze_v2.exists() or not any(bronze_v2.rglob("data.jsonl"))


def test_migrate_dry_run_validation_errors_no_write(
    project_root: Path, tmp_path: Path
):
    """Identity mapper leaves severity; v2 schema requires level → fail, no write."""
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_level",
        schema_path=project_root / "tests/fixtures/example_api/events_level.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.ok is False
    assert plan.partitions[0].ok is False
    assert plan.partitions[0].errors
    bronze_v2 = tmp_path / "lake" / "bronze" / "example_api" / "events_level"
    assert not bronze_v2.exists() or not any(bronze_v2.rglob("data.jsonl"))


def test_migrate_live_still_writes(project_root: Path, tmp_path: Path):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    result = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_level",
        schema_path=project_root / "tests/fixtures/example_api/events_level.schema.yaml",
        mapper_name="example_api_v1_to_v2",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=False,
    )
    assert result.rows == 1
    out = next(
        (tmp_path / "lake" / "bronze" / "example_api" / "events_level").rglob("data.jsonl")
    )
    assert json.loads(out.read_text(encoding="utf-8").splitlines()[0])["level"] == "high"


def test_mcp_migrate_dry_run_tool(project_root: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    pipe_dir = tmp_path / "configs" / "pipelines" / "example_api"
    pipe_dir.mkdir(parents=True)
    schema_v2 = (
        project_root / "tests/fixtures/example_api/events_level.schema.yaml"
    ).read_text(encoding="utf-8")
    schema_v1 = (
        project_root / "schemas/example_api/events/events.schema.yaml"
    ).read_text(encoding="utf-8")
    (tmp_path / "schemas/example_api/events").mkdir(parents=True)
    (tmp_path / "schemas/example_api/events/events.schema.yaml").write_text(
        schema_v1, encoding="utf-8"
    )
    level_schema = tmp_path / "tests/fixtures/example_api/events_level.schema.yaml"
    level_schema.parent.mkdir(parents=True)
    level_schema.write_text(schema_v2, encoding="utf-8")
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
        "example_api.events_level",
        "tests/fixtures/example_api/events_level.schema.yaml",
        "example_api_v1_to_v2",
        "2026-08-06",
        root=tmp_path,
    )
    assert out["dry_run"] is True
    assert out["ok"] is True
    assert out["rows_checked"] == 1
    assert out["validate_limit"] == 50
    ap = out["approval_plan"]
    assert ap["command"] == "migrate"
    assert "--to-bronze" in ap["argv"]
    bronze_v2 = tmp_path / "lake" / "bronze" / "example_api" / "events_level"
    assert not bronze_v2.exists() or not any(bronze_v2.rglob("data.jsonl"))


def test_mcp_migrate_dry_run_full_validate_requires_confirm(
    project_root: Path, tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DET_ALLOW_FULL_VALIDATE", "1")
    pipe_dir = tmp_path / "configs" / "pipelines" / "example_api"
    pipe_dir.mkdir(parents=True)
    schema_v1 = (
        project_root / "schemas/example_api/events/events.schema.yaml"
    ).read_text(encoding="utf-8")
    (tmp_path / "schemas/example_api/events").mkdir(parents=True)
    (tmp_path / "schemas/example_api/events/events.schema.yaml").write_text(
        schema_v1, encoding="utf-8"
    )
    level_schema = tmp_path / "tests/fixtures/example_api/events_level.schema.yaml"
    level_schema.parent.mkdir(parents=True)
    level_schema.write_text(
        (
            project_root / "tests/fixtures/example_api/events_level.schema.yaml"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
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

    with pytest.raises(ValueError, match="confirm_full_validate"):
        mcp_tools.migrate_dry_run(
            "example_api.events",
            "example_api.events_level",
            "tests/fixtures/example_api/events_level.schema.yaml",
            "example_api_v1_to_v2",
            "2026-08-06",
            validate_limit=0,
            root=tmp_path,
        )


def test_mcp_migrate_dry_run_full_validate_requires_env(
    project_root: Path, tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    monkeypatch.delenv("DET_ALLOW_FULL_VALIDATE", raising=False)
    pipe_dir = tmp_path / "configs" / "pipelines" / "example_api"
    pipe_dir.mkdir(parents=True)
    schema_v1 = (
        project_root / "schemas/example_api/events/events.schema.yaml"
    ).read_text(encoding="utf-8")
    (tmp_path / "schemas/example_api/events").mkdir(parents=True)
    (tmp_path / "schemas/example_api/events/events.schema.yaml").write_text(
        schema_v1, encoding="utf-8"
    )
    level_schema = tmp_path / "tests/fixtures/example_api/events_level.schema.yaml"
    level_schema.parent.mkdir(parents=True)
    level_schema.write_text(
        (
            project_root / "tests/fixtures/example_api/events_level.schema.yaml"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
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

    with pytest.raises(ValueError, match="DET_ALLOW_FULL_VALIDATE"):
        mcp_tools.migrate_dry_run(
            "example_api.events",
            "example_api.events_level",
            "tests/fixtures/example_api/events_level.schema.yaml",
            "example_api_v1_to_v2",
            "2026-08-06",
            validate_limit=0,
            confirm_full_validate=True,
            root=tmp_path,
        )


def test_mcp_migrate_dry_run_full_validate_with_gate(
    project_root: Path, tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DET_ALLOW_FULL_VALIDATE", "1")
    pipe_dir = tmp_path / "configs" / "pipelines" / "example_api"
    pipe_dir.mkdir(parents=True)
    schema_v1 = (
        project_root / "schemas/example_api/events/events.schema.yaml"
    ).read_text(encoding="utf-8")
    (tmp_path / "schemas/example_api/events").mkdir(parents=True)
    (tmp_path / "schemas/example_api/events/events.schema.yaml").write_text(
        schema_v1, encoding="utf-8"
    )
    level_schema = tmp_path / "tests/fixtures/example_api/events_level.schema.yaml"
    level_schema.parent.mkdir(parents=True)
    level_schema.write_text(
        (
            project_root / "tests/fixtures/example_api/events_level.schema.yaml"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
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
        "example_api.events_level",
        "tests/fixtures/example_api/events_level.schema.yaml",
        "example_api_v1_to_v2",
        "2026-08-06",
        validate_limit=0,
        confirm_full_validate=True,
        root=tmp_path,
    )
    assert out["validate_limit"] == 0
    assert out["confirm_full_validate"] is True
    assert out["validate_max_rows"] == 100_000
    assert out["ok"] is True


def test_migrate_dry_run_validate_max_rows_cap(
    project_root: Path, tmp_path: Path
):
    records = [
        {
            "id": f"e{i}",
            "occurred_at": "2026-08-06T12:00:00Z",
            "severity": "high",
            "state": "TX",
        }
        for i in range(5)
    ]
    pipeline = {
        "name": "example_api.events",
        "source": {
            "type": "example_api.events",
            "overrides": {"fixture_records": records},
        },
        "schema": str(project_root / "schemas/example_api/events/events.schema.yaml"),
        "ingestion": {"library": "thin"},
        "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "api.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        validate_limit=None,
        validate_max_rows=3,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.validate_capped is True
    assert plan.validate_max_rows == 3
    assert plan.partitions[0].truncated is True
    assert plan.partitions[0].rows == 3
    payload = plan.to_dict()
    assert payload["validate_capped"] is True
    assert "validate_cap_message" in payload


def test_migrate_dry_run_validate_max_rows_rejects_non_positive(
    project_root: Path, tmp_path: Path
):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")
    with pytest.raises(ValueError, match="validate_max_rows must be >= 1"):
        BronzeMigrator(tmp_path).migrate(
            pipeline=pipe_path,
            to_bronze="example_api.events_v1",
            schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
            mapper_name="identity",
            interval_start="2026-08-06",
            lake_path=str(tmp_path / "lake"),
            dry_run=True,
            validate_limit=None,
            validate_max_rows=0,
        )


def test_create_server_registers_migrate_dry_run():
    server = create_server()
    names = sorted(server._tool_manager._tools)
    assert "migrate_dry_run" in names


def test_extract_stamps_wire_version_and_migrate_filters(
    project_root: Path, tmp_path: Path
):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    raw_root = tmp_path / "lake" / "raw" / "example_api" / "events_v1"
    parts = sorted(raw_root.rglob("manifest.json"))
    assert len(parts) == 1
    manifest = json.loads(parts[0].read_text(encoding="utf-8"))
    assert manifest["wire_version"] == 1

    # Sibling partition stamped as wire_version 2 (botched mixed-era tree).
    import shutil

    v1_dir = parts[0].parent.parent  # …/__extract_run_datetime=…
    sibling = v1_dir.parent / "__extract_run_datetime=20990101T000000Z"
    shutil.copytree(v1_dir, sibling)
    man2 = json.loads((sibling / "meta" / "manifest.json").read_text(encoding="utf-8"))
    man2["wire_version"] = 2
    man2["extract_run_datetime"] = "2099-01-01T00:00:00+00:00"
    (sibling / "meta" / "manifest.json").write_text(
        json.dumps(man2), encoding="utf-8"
    )

    plan_latest = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
    )
    assert isinstance(plan_latest, MigratePlan)
    assert plan_latest.partitions_planned == 1
    assert plan_latest.partitions[0].wire_version == 2

    plan_all = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        all_raw_runs=True,
    )
    assert isinstance(plan_all, MigratePlan)
    assert plan_all.partitions_planned == 2
    assert {p.wire_version for p in plan_all.partitions} == {1, 2}

    plan_v1 = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        wire_version=1,
        all_raw_runs=True,
    )
    assert isinstance(plan_v1, MigratePlan)
    assert plan_v1.wire_version_filter == 1
    assert plan_v1.partitions_planned == 1
    assert plan_v1.partitions[0].wire_version == 1


def test_legacy_manifest_missing_wire_version_defaults_to_1(
    project_root: Path, tmp_path: Path
):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")
    man_path = next(
        (tmp_path / "lake" / "raw" / "example_api" / "events_v1").rglob("manifest.json")
    )
    payload = json.loads(man_path.read_text(encoding="utf-8"))
    del payload["wire_version"]
    man_path.write_text(json.dumps(payload), encoding="utf-8")

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        wire_version=1,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.partitions_planned == 1
    assert plan.partitions[0].wire_version == 1


def test_migrate_recreate_iceberg_rejected_for_filesystem(
    project_root: Path, tmp_path: Path
):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")
    with pytest.raises(ValueError, match="requires destination.type iceberg"):
        BronzeMigrator(tmp_path).migrate(
            pipeline=pipe_path,
            to_bronze="example_api.events_v1",
            schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
            mapper_name="identity",
            interval_start="2026-08-06",
            lake_path=str(tmp_path / "lake"),
            dry_run=True,
            recreate_iceberg=True,
        )


def test_migrate_dry_run_recreate_iceberg_plan_fields(
    project_root: Path, tmp_path: Path
):
    pytest.importorskip("pyiceberg")
    pipeline = {
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
        "schema": str(project_root / "schemas/example_api/events/events.schema.yaml"),
        "ingestion": {"library": "det"},
        "destination": {
            "type": "iceberg",
            "path": str(tmp_path / "lake"),
            "partition": "none",
        },
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "ice.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        recreate_iceberg=True,
        ingestion_library="det",
    )
    assert isinstance(plan, MigratePlan)
    assert plan.recreate_iceberg is True
    assert plan.will_drop_table is True
    assert plan.yaml_partition == "none"
    assert plan.recreate_warning and "DROP" in plan.recreate_warning
    assert plan.table_location is not None


def test_migrate_recreate_iceberg_rebuilds_partition_profile(
    project_root: Path, tmp_path: Path
):
    pytest.importorskip("pyiceberg")
    from det.ingestion.iceberg_writer import load_iceberg_table

    lake = str(tmp_path / "lake")
    pipe_extract_run = {
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
        "schema": str(project_root / "schemas/example_api/events/events.schema.yaml"),
        "ingestion": {"library": "det"},
        "destination": {
            "type": "iceberg",
            "path": lake,
            "partition": "extract_run",
        },
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "ice.yaml"
    pipe_path.write_text(yaml.safe_dump(pipe_extract_run), encoding="utf-8")
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")

    pipe_none = dict(pipe_extract_run)
    pipe_none["destination"] = {
        "type": "iceberg",
        "path": lake,
        "partition": "none",
    }
    pipe_path.write_text(yaml.safe_dump(pipe_none), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match live table"):
        BronzeMigrator(tmp_path).migrate(
            pipeline=pipe_path,
            to_bronze="example_api.events_v1",
            schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
            mapper_name="identity",
            interval_start="2026-08-06",
            lake_path=lake,
            ingestion_library="det",
            recreate_iceberg=False,
        )

    result = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=lake,
        ingestion_library="det",
        recreate_iceberg=True,
    )
    assert result.rows >= 1
    from det.runtime.lake import open_lake

    lake_ref = open_lake(lake, tmp_path)
    loc = lake_ref / "bronze" / "example_api" / "events_v1"
    ice = load_iceberg_table(
        lake=lake_ref,
        namespace="bronze_example_api",
        table="events_v1",
        table_location=loc,
    )
    assert ice is not None
    assert list(ice.spec().fields) == []


def test_migrate_latest_raw_sibling_load_parity(project_root: Path, tmp_path: Path):
    """Default migrate uses latest raw extract and preserves its extract_run id."""
    from det.runtime.manifest import read_manifest
    from det.runtime.meta import to_partition_value

    pipe_path = _example_pipeline(project_root, tmp_path, severity="high")
    runner = PipelineRunner(tmp_path)
    runner.extract(
        pipe_path,
        interval_start="2026-08-06",
        extract_run_datetime="2026-08-06T10:00:00+00:00",
    )
    pipe_path = _example_pipeline(project_root, tmp_path, severity="low")
    second = runner.extract(
        pipe_path,
        interval_start="2026-08-06",
        extract_run_datetime="2026-08-06T11:00:00+00:00",
    )
    second_manifest = read_manifest(second.raw_dir)
    second_run = str(second_manifest["extract_run_datetime"])

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.partitions_planned == 1
    assert plan.partitions[0].extract_run_datetime == second_run
    assert plan.all_raw_runs is False

    result = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
    )
    assert result.partitions == 1
    bronze_run = (
        tmp_path
        / "lake"
        / "bronze"
        / "example_api"
        / "events_v1"
        / f"__interval_start_datetime={to_partition_value('2026-08-06T00:00:00+00:00')}"
    )
    run_dirs = sorted(
        p
        for p in bronze_run.rglob("__extract_run_datetime=*")
        if p.is_dir()
    )
    assert len(run_dirs) == 1
    assert to_partition_value(second_run) in run_dirs[0].name


def test_migrate_all_raw_runs_keeps_siblings(project_root: Path, tmp_path: Path):
    pipe_path = _example_pipeline(project_root, tmp_path, severity="high")
    runner = PipelineRunner(tmp_path)
    runner.extract(
        pipe_path,
        interval_start="2026-08-06",
        extract_run_datetime="2026-08-06T10:00:00+00:00",
    )
    pipe_path = _example_pipeline(project_root, tmp_path, severity="low")
    runner.extract(
        pipe_path,
        interval_start="2026-08-06",
        extract_run_datetime="2026-08-06T11:00:00+00:00",
    )

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        all_raw_runs=True,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.partitions_planned == 2
    assert plan.all_raw_runs is True
    assert plan.partitions[0].extract_run_datetime != plan.partitions[1].extract_run_datetime

    result = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        all_raw_runs=True,
    )
    assert result.partitions == 2
    bronze_root = tmp_path / "lake" / "bronze" / "example_api" / "events_v1"
    run_dirs = sorted(
        p for p in bronze_root.rglob("__extract_run_datetime=*") if p.is_dir()
    )
    assert len(run_dirs) == 2
    assert len({p.name for p in run_dirs}) == 2


def test_migrate_all_raw_requires_recreate(project_root: Path, tmp_path: Path):
    pipe_path = _example_pipeline(project_root, tmp_path)
    PipelineRunner(tmp_path).run(pipe_path, interval_start="2026-08-06")
    with pytest.raises(ValueError, match="requires --recreate-iceberg"):
        BronzeMigrator(tmp_path).migrate(
            pipeline=pipe_path,
            to_bronze="example_api.events_v1",
            schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
            mapper_name="identity",
            all_raw=True,
            lake_path=str(tmp_path / "lake"),
            dry_run=True,
        )


def test_migrate_all_raw_forbids_interval(project_root: Path, tmp_path: Path):
    pipe_path = _example_pipeline(project_root, tmp_path)
    with pytest.raises(ValueError, match="cannot be combined"):
        BronzeMigrator(tmp_path).migrate(
            pipeline=pipe_path,
            to_bronze="example_api.events_v1",
            schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
            mapper_name="identity",
            interval_start="2026-08-06",
            all_raw=True,
            recreate_iceberg=True,
            lake_path=str(tmp_path / "lake"),
            dry_run=True,
        )


def test_migrate_all_raw_plans_every_interval(project_root: Path, tmp_path: Path):
    pytest.importorskip("pyiceberg")
    lake = str(tmp_path / "lake")
    pipeline = {
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
        "schema": str(project_root / "schemas/example_api/events/events.schema.yaml"),
        "ingestion": {"library": "det"},
        "destination": {
            "type": "iceberg",
            "path": lake,
            "partition": "extract_run",
        },
        "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
    }
    pipe_path = tmp_path / "ice.yaml"
    pipe_path.write_text(yaml.safe_dump(pipeline), encoding="utf-8")
    runner = PipelineRunner(tmp_path)
    runner.run(pipe_path, interval_start="2026-08-06")
    runner.run(pipe_path, interval_start="2026-08-07")

    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        lake_path=lake,
        dry_run=True,
        recreate_iceberg=True,
        all_raw=True,
        ingestion_library="det",
    )
    assert isinstance(plan, MigratePlan)
    assert plan.all_raw is True
    assert plan.partitions_planned == 2
    assert plan.will_drop_table is True

    result = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        lake_path=lake,
        recreate_iceberg=True,
        all_raw=True,
        ingestion_library="det",
    )
    assert result.partitions == 2
    assert result.rows >= 2
