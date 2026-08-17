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
    daily = (project_root / "dbt" / "models" / "ops" / "det__ops_run_daily.sql").read_text(
        encoding="utf-8"
    )
    assert "quantile_cont" in daily
    assert "stg_det__run_receipts" in daily
    sources = (project_root / "dbt" / "models" / "ops" / "sources.yml").read_text(
        encoding="utf-8"
    )
    assert "iceberg_scan" in sources
    assert "ops/run_receipts" in sources
    project = (project_root / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "ops:" in project
    assert "ops_slo_expected" in project
    seed = (project_root / "dbt" / "seeds" / "ops_slo_expected.csv").read_text(
        encoding="utf-8"
    )
    assert "noaa.storm_events" in seed
    tests_dir = project_root / "dbt" / "tests" / "ops"
    names = {
        "assert_ops_slo_recency.sql",
        "assert_ops_slo_error_rate.sql",
        "assert_ops_slo_p95.sql",
        "assert_ops_slo_fail_closed.sql",
    }
    assert names <= {p.name for p in tests_dir.iterdir()}
    for name in names:
        text = (tests_dir / name).read_text(encoding="utf-8")
        assert "tags=['ops']" in text or 'tags=["ops"]' in text
        assert "ops_slo_expected" in text
    fail_closed = (tests_dir / "assert_ops_slo_fail_closed.sql").read_text(encoding="utf-8")
    assert "schema_invalid" in fail_closed
    assert "integrity_error" in fail_closed
    assert "secret_not_set" in fail_closed
    assert "'lease_held'" not in fail_closed
