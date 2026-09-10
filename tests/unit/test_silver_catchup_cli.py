"""CLI smoke for det silver-catchup group + DuckDB cleanup skip."""

from __future__ import annotations

from pathlib import Path

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
    assert "plan" in result.output
    assert "apply" in result.output
    assert "build" in result.output
    assert "verify" in result.output
    assert "cleanup" in result.output


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
