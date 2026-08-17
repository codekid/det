from __future__ import annotations

from pathlib import Path

from det.runtime.dbt_runner import analytics_exclude, build_dbt_argv


def test_silver_gold_dag_excludes_ops_tag(project_root: Path):
    """Mirror det_dbt_silver_gold argv: full project build excludes tag:ops."""
    dbt_dir = project_root / "dbt"
    argv = build_dbt_argv(
        command="build",
        project_dir=dbt_dir,
        select=None,
        exclude=analytics_exclude(None),
    )
    assert "--exclude" in argv
    assert argv[argv.index("--exclude") + 1] == "tag:ops"


def test_ops_dag_file_exists(project_root: Path):
    assert (project_root / "dags" / "det_ops_dag.py").is_file()
    text = (project_root / "dags" / "det_ops_dag.py").read_text(encoding="utf-8")
    assert 'dag_id="det_ops_receipts"' in text
    assert "tag:ops" in text
    assert 'target="ops"' in text


def test_ops_models_tagged(project_root: Path):
    stg = (project_root / "dbt" / "models" / "ops" / "stg_det__run_receipts.sql").read_text(
        encoding="utf-8"
    )
    assert "tag" in stg.lower() or "ops" in stg
    sources = (project_root / "dbt" / "models" / "ops" / "sources.yml").read_text(
        encoding="utf-8"
    )
    assert "iceberg_scan" in sources
    assert "ops/run_receipts" in sources
    project = (project_root / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "ops:" in project
    assert "ops" in project
