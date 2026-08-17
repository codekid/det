from __future__ import annotations

import importlib
import sys
from datetime import UTC
from pathlib import Path


def _load_det_env(project_root: Path):
    dags = str(project_root / "dags")
    if dags not in sys.path:
        sys.path.insert(0, dags)
    if "det_env" in sys.modules:
        del sys.modules["det_env"]
    return importlib.import_module("det_env")


def test_pipeline_overrides_parses_comma_and_newline(project_root: Path, monkeypatch):
    det_env = _load_det_env(project_root)
    monkeypatch.setenv(
        "DET_PIPELINE_OVERRIDES",
        "source.overrides.local_csv_dir=fixtures/storm_events, ingestion.library=thin\n"
        "source.overrides.filename_substr=details",
    )
    assert det_env.pipeline_overrides() == [
        "source.overrides.local_csv_dir=fixtures/storm_events",
        "ingestion.library=thin",
        "source.overrides.filename_substr=details",
    ]


def test_pipeline_overrides_empty(project_root: Path, monkeypatch):
    det_env = _load_det_env(project_root)
    monkeypatch.delenv("DET_PIPELINE_OVERRIDES", raising=False)
    assert det_env.pipeline_overrides() == []


def test_dbt_select_and_env_for_pipeline(project_root: Path, monkeypatch):
    det_env = _load_det_env(project_root)
    monkeypatch.setenv("DET_PROJECT_ROOT", str(project_root))
    monkeypatch.setenv("DET_PIPELINE_CONFIG", "noaa.storm_events")
    monkeypatch.setenv("DET_LAKE_PATH", str(project_root / "data" / "lake"))
    monkeypatch.delenv("DET_BRONZE_SCHEMA", raising=False)
    monkeypatch.delenv("DET_BRONZE_SOURCE", raising=False)
    monkeypatch.delenv("DET_DBT_SELECT", raising=False)
    assert det_env.dbt_select_for_pipeline() == ["stg_noaa__storm_events+"]
    assert det_env.dbt_select() is None
    env = det_env.dbt_env_for_pipeline()
    assert env["DET_BRONZE_SCHEMA"] == "bronze_noaa"
    assert env["DET_BRONZE_SOURCE"] == "filesystem"
    assert env["DET_LAKE_PATH"] == str(project_root / "data" / "lake")
    assert env["DET_ANALYTICS_DUCKDB"] == str(
        (project_root / "data" / "analytics.duckdb").resolve()
    )


def test_dbt_select_from_env(project_root: Path, monkeypatch):
    det_env = _load_det_env(project_root)
    monkeypatch.setenv(
        "DET_DBT_SELECT",
        "stg_noaa__storm_events+, stg_noaa__fatalities+",
    )
    assert det_env.dbt_select() == [
        "stg_noaa__storm_events+",
        "stg_noaa__fatalities+",
    ]


def test_daily_logical_dates_for_interval(project_root: Path):
    from datetime import datetime

    det_env = _load_det_env(project_root)
    dates = det_env.daily_logical_dates_for_interval("2026-08-01", "2026-08-04")
    assert dates == [
        datetime(2026, 8, 2, tzinfo=UTC),
        datetime(2026, 8, 3, tzinfo=UTC),
        datetime(2026, 8, 4, tzinfo=UTC),
    ]


def test_daily_logical_dates_rejects_empty_range(project_root: Path):
    import pytest

    det_env = _load_det_env(project_root)
    with pytest.raises(ValueError, match="must be after"):
        det_env.daily_logical_dates_for_interval("2026-08-01", "2026-08-01")


def test_lock_ttl_sec_from_conf(project_root: Path):
    det_env = _load_det_env(project_root)
    assert det_env.lock_ttl_sec_from_conf({}) is None
    assert det_env.lock_ttl_sec_from_conf({"lock_ttl_sec": 21600}) == 21600
    assert det_env.lock_ttl_sec_from_conf({"lock_ttl_sec": "90"}) == 90
