"""Unit tests for bronze↔silver catch-up diff and manifest helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.destinations.models import to_partition_value
from det.runtime.settings import DetSettings, use_settings
from det.runtime.silver_catchup import (
    catchup_select_from_manifest,
    catchup_vars_from_manifest,
    diff_bronze_silver,
    manifest_payload_from_catchup,
    plan_catchup_manifest,
    read_catchup_manifest,
    write_catchup_manifest,
)


def _write_pipeline(root: Path) -> None:
    provider, source = "example_api", "events"
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True, exist_ok=True)
    schema_rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
    schema_path = root / schema_rel
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "integer"}},
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    (pipe_dir / f"{source}.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {"type": "example_api.events"},
                "schema": schema_rel,
                "destination": {"type": "filesystem", "path": "./data/lake"},
                "wire_version": 1,
                "dbt": {
                    "silver": {
                        "materialized": "incremental",
                        "unique_key": ["__row_hash"],
                        "order_by": ["__extract_run_datetime desc"],
                        "watermark": "__extract_run_datetime",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _write_bronze_run(
    lake: Path,
    *,
    interval_start: str,
    interval_end: str,
    extract_run: str,
) -> Path:
    run_dir = (
        lake
        / "bronze"
        / "example_api"
        / "events_v1"
        / f"__interval_start_datetime={to_partition_value(interval_start)}"
        / f"__interval_end_datetime={to_partition_value(interval_end)}"
        / f"__extract_run_datetime={to_partition_value(extract_run)}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "data.jsonl").write_text(
        json.dumps({"id": 1, "__extract_run_datetime": extract_run}) + "\n",
        encoding="utf-8",
    )
    return run_dir


def _silver_db(tmp_path: Path, runs: list[str]) -> Path:
    duckdb = pytest.importorskip("duckdb")
    db = tmp_path / "analytics.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create schema if not exists silver_example_api")
    con.execute(
        """
        create table silver_example_api.silver_example_api__events (
            id integer,
            __extract_run_datetime timestamptz,
            __row_hash varchar
        )
        """
    )
    for i, ts in enumerate(runs):
        con.execute(
            "insert into silver_example_api.silver_example_api__events values (?, ?, ?)",
            [i, ts, f"h{i}"],
        )
    con.close()
    return db


@pytest.fixture
def catchup_root(tmp_path: Path) -> Path:
    _write_pipeline(tmp_path)
    lake = tmp_path / "data" / "lake"
    lake.mkdir(parents=True)
    return tmp_path


def test_diff_hole_behind_max_watermark(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    early = "2026-09-02T12:08:00+00:00"
    late = "2026-09-02T12:10:00+00:00"
    _write_bronze_run(
        lake,
        interval_start="2026-09-01T00:00:00+00:00",
        interval_end="2026-09-02T00:00:00+00:00",
        extract_run=early,
    )
    _write_bronze_run(
        lake,
        interval_start="2026-09-02T00:00:00+00:00",
        interval_end="2026-09-03T00:00:00+00:00",
        extract_run=late,
    )
    db = _silver_db(catchup_root, [late])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        out = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
        )
    assert out["catchup_count"] == 1
    assert out["catchup_runs"][0]["extract_run_datetime"].startswith("2026-09-02T12:08")
    assert out["ok_count"] == 1
    assert out["stale_siblings_count"] == 0


def test_diff_latest_present_empty_catchup(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    run_a = "2026-09-02T10:00:00+00:00"
    run_b = "2026-09-02T12:00:00+00:00"
    start, end = "2026-09-02T00:00:00+00:00", "2026-09-03T00:00:00+00:00"
    _write_bronze_run(lake, interval_start=start, interval_end=end, extract_run=run_a)
    _write_bronze_run(lake, interval_start=start, interval_end=end, extract_run=run_b)
    db = _silver_db(catchup_root, [run_b])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        out = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
        )
    assert out["catchup_count"] == 0
    assert out["ok_count"] == 1
    assert out["stale_siblings_count"] == 1
    assert out["stale_siblings_ignored"][0]["extract_run_datetime"].startswith(
        "2026-09-02T10:00"
    )


def test_manifest_roundtrip_and_vars(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    payload = manifest_payload_from_catchup(
        [
            {
                "pipeline": "example_api.events",
                "extract_run_datetime": "2026-09-02T12:08:00+00:00",
                "interval_start": "2026-09-01T00:00:00+00:00",
                "interval_end": "2026-09-02T00:00:00+00:00",
            }
        ]
    )
    with use_settings(settings):
        path = write_catchup_manifest(
            payload, project_root=catchup_root, settings=settings
        )
        loaded = read_catchup_manifest(project_root=catchup_root, settings=settings)
    assert loaded is not None
    assert loaded["runs"][0]["pipeline"] == "example_api.events"
    assert path.exists()
    vars_map = catchup_vars_from_manifest(payload)
    assert vars_map["det_catchup"] is True
    assert vars_map["det_catchup_by_pipeline"]["example_api.events"] == [
        "2026-09-02T12:08:00+00:00"
    ]
    selects = catchup_select_from_manifest(payload, project_root=catchup_root)
    assert selects == ["silver_example_api__events"]


def test_plan_catchup_manifest_single(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    run = "2026-09-02T12:08:00+00:00"
    _write_bronze_run(
        lake,
        interval_start="2026-09-01T00:00:00+00:00",
        interval_end="2026-09-02T00:00:00+00:00",
        extract_run=run,
    )
    db = _silver_db(catchup_root, [])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        planned = plan_catchup_manifest(
            project_root=catchup_root,
            pipeline="example_api.events",
            analytics_db=db,
        )
    assert planned["dry_run"] is True
    assert planned["manifest"]["runs"][0]["extract_run_datetime"].startswith(
        "2026-09-02T12:08"
    )


def test_plan_includes_all_catchup_rows_beyond_display_limit(
    catchup_root: Path, monkeypatch
):
    """Apply manifest must not drop holes that exceed the MCP display limit."""
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    n = 5
    for i in range(n):
        day = f"2026-09-{i + 1:02d}"
        _write_bronze_run(
            lake,
            interval_start=f"{day}T00:00:00+00:00",
            interval_end=f"2026-09-{i + 2:02d}T00:00:00+00:00",
            extract_run=f"2026-09-{i + 1:02d}T12:00:00+00:00",
        )
    db = _silver_db(catchup_root, [])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        preview = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
            limit=2,
        )
        planned = plan_catchup_manifest(
            project_root=catchup_root,
            pipeline="example_api.events",
            analytics_db=db,
            limit=2,
        )
    assert preview["truncated"] is True
    assert len(preview["catchup_runs"]) == 2
    assert preview["catchup_count"] == 2  # bronze listing itself was clamped
    assert planned["diff"]["complete"] is True
    assert planned["diff"]["truncated"] is False
    assert len(planned["manifest"]["runs"]) == n
    assert planned["diff"]["catchup_count"] == n
