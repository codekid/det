"""Tests for det scaffold-ops (embedder ops dbt slice)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.cli import app
from det.mcp.tools import scaffold_ops_dry_run
from det.scaffold import ops as ops_mod
from det.scaffold.ops import iter_ops_template_pairs, scaffold_ops


def test_ops_templates_match_monorepo_canonical():
    for label, canonical, template in iter_ops_template_pairs():
        assert canonical.is_file(), f"missing canonical {label}"
        assert template.is_file(), f"missing template for {label}"
        assert canonical.read_text(encoding="utf-8") == template.read_text(
            encoding="utf-8"
        ), f"drift between {label} and packaged template"


def test_scaffold_ops_dry_run_and_write(tmp_path: Path):
    preview = scaffold_ops(project_root=tmp_path, dry_run=True)
    assert preview.dataset == "ops"
    assert any(a.action == "would_write" for a in preview.actions)
    assert not (tmp_path / "dbt" / "models" / "ops" / "sources.yml").exists()

    result = scaffold_ops(project_root=tmp_path, dry_run=False)
    assert result.dataset == "ops"
    assert (tmp_path / "dbt" / "models" / "ops" / "stg_det__run_receipts.sql").is_file()
    assert (tmp_path / "dbt" / "models" / "ops" / "det__ops_run_daily.sql").is_file()
    assert (tmp_path / "dbt" / "tests" / "ops" / "assert_ops_slo_recency.sql").is_file()
    assert (tmp_path / "dbt" / "macros" / "generate_schema_name.sql").is_file()
    assert (tmp_path / "dbt" / "macros" / "det_sql_compat.sql").is_file()
    assert (tmp_path / "dbt" / "seeds" / "ops_slo_expected.csv").is_file()

    project = yaml.safe_load((tmp_path / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))
    assert project["models"]["analytics"]["ops"]["+schema"] == "ops"
    assert "ops_slo_expected" in project["seeds"]["analytics"]

    profiles = yaml.safe_load((tmp_path / "dbt" / "profiles.yml").read_text(encoding="utf-8"))
    assert profiles["analytics"]["outputs"]["ops"]["type"] == "duckdb"


def test_scaffold_ops_skips_existing_without_force(tmp_path: Path):
    scaffold_ops(project_root=tmp_path, dry_run=False)
    stg = tmp_path / "dbt" / "models" / "ops" / "stg_det__run_receipts.sql"
    stg.write_text("-- custom\n", encoding="utf-8")

    again = scaffold_ops(project_root=tmp_path, force=False, dry_run=False)
    assert stg.read_text(encoding="utf-8") == "-- custom\n"
    assert any(
        a.action == "skip" and a.path == stg.resolve() for a in again.actions
    )

    forced = scaffold_ops(project_root=tmp_path, force=True, dry_run=False)
    text = stg.read_text(encoding="utf-8")
    assert "config(tags=['ops'])" in text
    assert "source('det_ops', 'run_receipts')" in text
    assert any(a.action == "write" and a.path == stg.resolve() for a in forced.actions)


def test_scaffold_ops_never_overwrites_generate_schema_name(tmp_path: Path):
    scaffold_ops(project_root=tmp_path, dry_run=False)
    macro = tmp_path / "dbt" / "macros" / "generate_schema_name.sql"
    assert macro.is_file()
    macro.write_text("-- embedder custom\n", encoding="utf-8")

    forced = scaffold_ops(project_root=tmp_path, force=True, dry_run=False)
    assert macro.read_text(encoding="utf-8") == "-- embedder custom\n"
    assert any(
        a.action == "skip" and a.path == macro.resolve() for a in forced.actions
    )
    # DET-owned macro still refreshes under --force.
    compat = tmp_path / "dbt" / "macros" / "det_sql_compat.sql"
    assert any(a.action == "write" and a.path == compat.resolve() for a in forced.actions)


def test_scaffold_ops_merges_existing_profiles(tmp_path: Path):
    dbt = tmp_path / "dbt"
    dbt.mkdir(parents=True)
    (dbt / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "analytics": {
                    "target": "duckdb",
                    "outputs": {
                        "duckdb": {
                            "type": "duckdb",
                            "path": "/custom/analytics.duckdb",
                            "schema": "main",
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    scaffold_ops(project_root=tmp_path, dry_run=False)
    profiles = yaml.safe_load((dbt / "profiles.yml").read_text(encoding="utf-8"))
    assert profiles["analytics"]["outputs"]["duckdb"]["path"] == "/custom/analytics.duckdb"
    assert "ops" in profiles["analytics"]["outputs"]


def test_scaffold_ops_uses_dbt_project_profile_name(tmp_path: Path):
    dbt = tmp_path / "dbt"
    dbt.mkdir(parents=True)
    (dbt / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {
                "name": "myco",
                "version": "1.0.0",
                "config-version": 2,
                "profile": "myco",
                "model-paths": ["models"],
                "seed-paths": ["seeds"],
                "test-paths": ["tests"],
                "macro-paths": ["macros"],
            }
        ),
        encoding="utf-8",
    )
    (dbt / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "myco": {
                    "target": "duckdb",
                    "outputs": {
                        "duckdb": {
                            "type": "duckdb",
                            "path": "/custom/myco.duckdb",
                            "schema": "main",
                        }
                    },
                },
                "other": {"target": "duckdb", "outputs": {}},
            }
        ),
        encoding="utf-8",
    )
    scaffold_ops(project_root=tmp_path, dry_run=False)
    profiles = yaml.safe_load((dbt / "profiles.yml").read_text(encoding="utf-8"))
    assert profiles["myco"]["outputs"]["duckdb"]["path"] == "/custom/myco.duckdb"
    assert profiles["myco"]["outputs"]["ops"]["type"] == "duckdb"
    assert "ops" not in profiles["other"]["outputs"]
    assert "analytics" not in profiles


def test_scaffold_ops_rejects_destination_outside_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        ops_mod,
        "_OPS_MODEL_FILES",
        (("../../outside.txt", "models/sources.yml"),),
    )
    monkeypatch.setattr(ops_mod, "_OPS_TEST_FILES", ())
    monkeypatch.setattr(ops_mod, "_OPS_MACRO_FILES", ())
    with pytest.raises(ValueError, match="escapes project root"):
        scaffold_ops(project_root=tmp_path, dry_run=True)
    assert not (tmp_path.parent / "outside.txt").exists()


def test_scaffold_ops_cli_dry_run(tmp_path: Path):
    import structlog
    from typer.testing import CliRunner

    from det.logging import configure_logging

    runner = CliRunner()
    try:
        result = runner.invoke(
            app,
            ["scaffold-ops", "--dry-run", "--project-root", str(tmp_path)],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0, result.output
    assert "DRY-RUN scaffold-ops" in result.output
    assert not (tmp_path / "dbt" / "models" / "ops").exists()


def test_scaffold_ops_mcp_dry_run(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    out = scaffold_ops_dry_run(root=tmp_path)
    assert out["dry_run"] is True
    assert out["dataset"] == "ops"
    assert out["approval_plan"]["command"] == "scaffold-ops"
    assert out["approval_plan"]["argv"][0] == "scaffold-ops"
    assert any(a["action"] == "would_write" for a in out["actions"])
