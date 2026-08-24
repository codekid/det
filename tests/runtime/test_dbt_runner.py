from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from det.runtime.config import PipelineConfig, SourceConfig
from det.runtime.dbt_runner import (
    DbtNotInstalledError,
    _run_dbt_subprocess,
    analytics_exclude,
    build_dbt_argv,
    default_select_for_pipeline,
    is_ops_selector,
    ops_dbt_target,
    run_dbt,
)


def test_default_select_for_pipeline():
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
    )
    assert default_select_for_pipeline(config) == ["stg_noaa__storm_events+"]


def test_default_select_includes_stg_relations():
    config = PipelineConfig(
        name="example_api.orders",
        source=SourceConfig(type="example_api.orders"),
        schema_path="schemas/example_api/orders/orders.schema.yaml",
        dbt={
            "stg": {
                "relations": {
                    "discount_codes": {"materialized": "view"},
                    "line_items": {
                        "materialized": "view",
                        "relations": {"tax_lines": {"materialized": "view"}},
                    },
                },
            }
        },
    )
    assert default_select_for_pipeline(config) == [
        "stg_example_api__orders+",
        "stg_example_api__orders__discount_codes+",
        "stg_example_api__orders__line_items+",
        "stg_example_api__orders__line_items__tax_lines+",
    ]


def test_build_dbt_argv():
    argv = build_dbt_argv(
        command="build",
        project_dir=Path("/proj/dbt"),
        select=["stg_mini+"],
        full_refresh=True,
    )
    assert argv[:2] == ["dbt", "build"]
    assert "--project-dir" in argv and "/proj/dbt" in argv
    assert argv[argv.index("--select") + 1] == "stg_mini+"
    assert "--full-refresh" in argv


def test_build_dbt_argv_exclude():
    argv = build_dbt_argv(
        command="build",
        project_dir=Path("/proj/dbt"),
        exclude=["tag:ops"],
    )
    assert argv[argv.index("--exclude") + 1] == "tag:ops"


def test_analytics_exclude_skips_when_selecting_ops():
    assert analytics_exclude(None) == ["tag:ops"]
    assert analytics_exclude(["stg_noaa__storm_events+"]) == ["tag:ops"]
    assert analytics_exclude(["tag:ops"]) is None
    assert analytics_exclude(["stg_det__run_receipts"]) is None
    assert is_ops_selector("path:models/ops")


def test_ops_dbt_target():
    assert ops_dbt_target(None) is None
    assert ops_dbt_target(["stg_noaa__storm_events+"]) is None
    assert ops_dbt_target(["tag:ops"]) == "ops"
    assert ops_dbt_target(["stg_det__run_receipts"]) == "ops"


def test_run_dbt_ops_select_uses_ops_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_OPS_DUCKDB", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    result = run_dbt(
        project_root=tmp_path,
        select=["tag:ops"],
        dry_run=True,
    )
    assert result.command[result.command.index("--target") + 1] == "ops"
    assert "--exclude" not in result.command


def test_run_dbt_dry_run_sets_lake_and_select(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )

    result = run_dbt(
        project_root=tmp_path,
        pipeline=pipeline,
        dry_run=True,
    )
    assert result.returncode == 0
    assert result.select == ("stg_noaa__storm_events+",)
    assert result.lake_path == str((tmp_path / "data" / "lake").resolve())
    assert result.bronze_source == "filesystem"
    assert "stg_noaa__storm_events+" in result.command


def test_run_dbt_sets_duckdb_bronze_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_BRONZE_SOURCE", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        f"""
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: duckdb
  path: ./data/lake
  connection: {tmp_path / "analytics.duckdb"}
  dataset: bronze
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(project_root=tmp_path, pipeline=pipeline, dry_run=True)
    assert result.bronze_source == "duckdb"


def test_run_dbt_sets_iceberg_bronze_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_BRONZE_SOURCE", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: iceberg
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(project_root=tmp_path, pipeline=pipeline, dry_run=True)
    assert result.bronze_source == "iceberg"


def test_run_dbt_s3_lake_uses_duckdb_s3_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9000")
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: example_api.events
source:
  type: example_api.events
schema: schemas/mini.schema.yaml
destination:
  type: iceberg
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(
        project_root=tmp_path,
        pipeline=pipeline,
        lake_path="s3://det-ci/det-lake",
        dry_run=True,
    )
    assert result.command[result.command.index("--target") + 1] == "duckdb_s3"
    assert result.lake_path == "s3://det-ci/det-lake"


def test_run_dbt_local_lake_keeps_duckdb_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(project_root=tmp_path, pipeline=pipeline, dry_run=True)
    assert "--target" not in result.command


def test_run_dbt_missing_cli(tmp_path: Path):
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    with (
        patch("det.runtime.dbt_runner.find_dbt_executable", return_value=None),
        pytest.raises(DbtNotInstalledError),
    ):
        run_dbt(project_root=tmp_path, select=["silver_noaa__storm_events"])


def test_run_dbt_subprocess_streams_to_stdout(capsys: pytest.CaptureFixture[str]):
    proc = MagicMock()
    proc.stdout = io.StringIO("PASS=1\nDone\n")
    proc.wait.return_value = 0

    with patch("det.runtime.dbt_runner.subprocess.Popen", return_value=proc) as popen:
        code, output = _run_dbt_subprocess(
            ["dbt", "build"],
            cwd="/tmp/dbt",
            env={"PATH": "/bin"},
        )

    assert code == 0
    assert output == "PASS=1\nDone\n"
    assert "PASS=1" in capsys.readouterr().out
    assert popen.call_args.kwargs["stdout"] is not None
    assert popen.call_args.kwargs["env"]["PYTHONUNBUFFERED"] == "1"


def test_run_dbt_stores_streamed_output(tmp_path: Path):
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")

    with (
        patch("det.runtime.dbt_runner.find_dbt_executable", return_value="/usr/bin/dbt"),
        patch(
            "det.runtime.dbt_runner._run_dbt_subprocess",
            return_value=(1, "Compilation Error\n"),
        ),
    ):
        result = run_dbt(project_root=tmp_path, select=["stg_x+"])

    assert result.returncode == 1
    assert result.output == "Compilation Error\n"
