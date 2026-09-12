"""CLI smoke for det silver-catchup group + DuckDB cleanup skip."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import structlog
from typer.testing import CliRunner

from det.cli.app import app
from det.logging import configure_logging


def test_silver_catchup_group_help():
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["silver-catchup", "--help"])
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0, result.output
    assert "status" in result.output
    assert "heal" in result.output
    assert "plan" in result.output
    assert "apply" in result.output
    assert "build" in result.output
    assert "verify" in result.output
    assert "cleanup" in result.output


def test_heal_rejects_census():
    runner = CliRunner()
    try:
        result = runner.invoke(
            app, ["silver-catchup", "heal", "-p", "noaa.storm_events", "--census"]
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 1
    assert "Advanced" in result.output


def test_heal_nothing_to_do(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "lake"))
    (tmp_path / "lake").mkdir()
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "noaa_storm_events.yaml").write_text(
        "name: noaa.storm_events\n"
        "source:\n  type: noaa.storm_events\n"
        "schema: schemas/noaa_storm_events.json\n"
        "destination:\n  type: filesystem\n  path: lake\n",
        encoding="utf-8",
    )

    def _preview(pipeline: str, **kwargs):
        return {
            "dry_run": True,
            "route": "mode_a",
            "boring": True,
            "pipeline": "noaa.storm_events",
            "extract_lookback": "48h",
            "catchup_count": 0,
            "catchup_runs": [],
            "nothing_to_do": True,
            "ladder_rung": "apply",
            "next_rung": None,
            "next_steps": "nothing to do",
        }

    monkeypatch.setattr(
        "det.mcp.dry_run.catchup._mode_a_heal_preview",
        _preview,
    )
    runner = CliRunner()
    try:
        result = runner.invoke(
            app,
            [
                "silver-catchup",
                "heal",
                "-p",
                "noaa.storm_events",
                "--project-root",
                str(tmp_path),
                "--json",
            ],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0, result.output
    # CLI may emit structlog lines before the JSON body.
    start = result.output.rfind("{\n")
    assert start >= 0, result.output
    payload = json.loads(result.output[start:])
    assert payload["nothing_to_do"] is True
    assert "approval_plan" not in payload
    assert "silver-catchup build" not in (payload.get("next_steps") or "")


def test_heal_continue_requires_manifest():
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["silver-catchup", "heal", "--continue"])
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code != 0
    assert "manifest-id" in result.output.lower() or "manifest" in result.output.lower()


def test_silver_catchup_cleanup_skips_duckdb(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_DBT_TARGET", raising=False)
    runner = CliRunner()
    try:
        result = runner.invoke(
            app,
            ["silver-catchup", "cleanup", "--list", "--json"],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0, result.output
    assert "duckdb" in result.output


def test_build_manifest_id_is_not_unbound_against_dbt():
    """Regression: build --manifest-id must not fail approval_unbound_flag."""
    from click.core import ParameterSource

    from det.cli.silver_catchup_cmd import _unbound_params_for_dbt_catchup_build

    class _Ctx:
        params = {
            "manifest_id": "scm_" + ("ab" * 8),
            "pipeline": "noaa.storm_events",
            "approval": "apr_x",
            "require_approval": True,
            "project_root": None,
            "project_dir": None,
            "target": None,
            "lake_path": None,
            "lake_path_raw": None,
            "lake_path_bronze": None,
            "lake_path_ops": None,
            "set_": [],
        }

        def get_parameter_source(self, name: str):
            if name in {"manifest_id", "pipeline", "approval", "require_approval"}:
                return ParameterSource.COMMANDLINE
            return ParameterSource.DEFAULT

    assert _unbound_params_for_dbt_catchup_build(_Ctx()) == []  # type: ignore[arg-type]
