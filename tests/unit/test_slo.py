from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from det.runtime.check import check_project
from det.runtime.config import load_pipeline_config
from det.runtime.slo import (
    CADENCE_DEFAULTS,
    SLO_SEED_RELPATH,
    SloConfig,
    flatten_slo_rows,
    render_slo_seed_csv,
    render_slo_seed_for_project,
    slo_seed_is_stale,
)
from det.scaffold.dbt import scaffold_dbt


def test_cadence_defaults_daily_weekly_hourly():
    assert CADENCE_DEFAULTS["daily"] == (26, 24)
    assert CADENCE_DEFAULTS["weekly"] == (192, 168)
    assert CADENCE_DEFAULTS["hourly"] == (2, 24)


def test_flatten_uses_cadence_defaults():
    slo = SloConfig.model_validate({"cadence": "daily", "max_error_rate": 0})
    rows = flatten_slo_rows("noaa.storm_events", slo)
    assert [r.command for r in rows] == ["extract", "load"]
    assert all(r.recency_hours == 26 and r.score_hours == 24 for r in rows)
    assert all(r.max_error_rate == 0 for r in rows)
    assert all(r.p95_ms is None for r in rows)


def test_overlay_does_not_wipe_sibling_keys():
    slo = SloConfig.model_validate(
        {
            "cadence": "daily",
            "max_error_rate": 0,
            "p95_ms": 600000,
            "load": {"p95_ms": 900000},
        }
    )
    by_cmd = {r.command: r for r in flatten_slo_rows("noaa.storm_events", slo)}
    assert by_cmd["extract"].p95_ms == 600000
    assert by_cmd["load"].p95_ms == 900000
    assert by_cmd["load"].max_error_rate == 0
    assert by_cmd["load"].recency_hours == 26


def test_false_skips_command():
    slo = SloConfig.model_validate({"cadence": "hourly", "extract": False})
    rows = flatten_slo_rows("example_api.events", slo)
    assert [r.command for r in rows] == ["load"]
    assert rows[0].recency_hours == 2
    assert rows[0].score_hours == 24


def test_explicit_recency_without_cadence():
    slo = SloConfig.model_validate({"recency_hours": 10})
    rows = flatten_slo_rows("p.x", slo)
    assert rows[0].cadence is None
    assert rows[0].recency_hours == 10
    assert rows[0].score_hours == 10


def test_slo_requires_cadence_or_recency():
    with pytest.raises(ValidationError, match="cadence or recency_hours"):
        SloConfig.model_validate({"max_error_rate": 0})


def test_extract_true_rejected():
    with pytest.raises(ValidationError, match="mapping or false"):
        SloConfig.model_validate({"cadence": "daily", "extract": True})


def test_seed_render_noaa_shape():
    slo = SloConfig.model_validate(
        {
            "cadence": "daily",
            "max_error_rate": 0,
            "p95_ms": 600000,
            "load": {"p95_ms": 900000},
        }
    )
    csv_text = render_slo_seed_csv(flatten_slo_rows("noaa.storm_events", slo))
    assert csv_text.splitlines()[0] == (
        "pipeline,command,cadence,recency_hours,score_hours,max_error_rate,p95_ms"
    )
    assert "noaa.storm_events,extract,daily,26,24,0,600000" in csv_text
    assert "noaa.storm_events,load,daily,26,24,0,900000" in csv_text


def test_noaa_pipeline_flattens(project_root: Path):
    cfg = load_pipeline_config(project_root / "configs/pipelines/noaa/storm_events.yaml")
    assert cfg.slo is not None
    rows = flatten_slo_rows(cfg.name, cfg.slo)
    by_cmd = {r.command: r for r in rows}
    assert by_cmd["extract"].p95_ms == 600000
    assert by_cmd["load"].p95_ms == 900000
    expected = render_slo_seed_for_project(project_root)
    on_disk = (project_root / SLO_SEED_RELPATH).read_text(encoding="utf-8")
    assert on_disk == expected
    assert not slo_seed_is_stale(project_root)


def _write_slo_pipeline(root: Path) -> None:
    schema_rel = "schemas/example_api/events/events.schema.yaml"
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
    pipe = root / "configs" / "pipelines" / "example_api" / "events.yaml"
    pipe.parent.mkdir(parents=True, exist_ok=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {"type": "example_api.events"},
                "schema": schema_rel,
                "ingestion": {"library": "thin"},
                "destination": {"type": "filesystem", "path": "./data/lake"},
                "slo": {
                    "cadence": "daily",
                    "max_error_rate": 0,
                    "p95_ms": 600000,
                    "load": {"p95_ms": 900000},
                },
            }
        ),
        encoding="utf-8",
    )


def test_slo_seed_stale_when_csv_mismatches(tmp_path: Path):
    _write_slo_pipeline(tmp_path)
    seed = tmp_path / SLO_SEED_RELPATH
    seed.parent.mkdir(parents=True)
    seed.write_text("pipeline,command\nwrong,extract\n", encoding="utf-8")
    findings = check_project(tmp_path)
    assert any(f.code == "slo_seed_stale" for f in findings)
    assert slo_seed_is_stale(tmp_path)


def test_slo_seed_ok_when_csv_matches(tmp_path: Path):
    _write_slo_pipeline(tmp_path)
    seed = tmp_path / SLO_SEED_RELPATH
    seed.parent.mkdir(parents=True)
    seed.write_text(render_slo_seed_for_project(tmp_path), encoding="utf-8")
    findings = check_project(tmp_path)
    assert not any(f.code == "slo_seed_stale" for f in findings)
    assert not slo_seed_is_stale(tmp_path)


def test_slo_seed_missing_ok_when_no_slo_pipelines(tmp_path: Path):
    schema_rel = "schemas/example_api/events/events.schema.yaml"
    schema_path = tmp_path / schema_rel
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(
        yaml.safe_dump(
            {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
        ),
        encoding="utf-8",
    )
    pipe = tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {"type": "example_api.events"},
                "schema": schema_rel,
                "ingestion": {"library": "thin"},
                "destination": {"type": "filesystem", "path": "./data/lake"},
            }
        ),
        encoding="utf-8",
    )
    findings = check_project(tmp_path)
    assert not any(f.code == "slo_seed_stale" for f in findings)


def test_scaffold_rewrites_slo_seed_without_force(tmp_path: Path):
    _write_slo_pipeline(tmp_path)
    config = load_pipeline_config(
        tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    )
    models = tmp_path / "dbt" / "models" / "silver"
    first = scaffold_dbt(
        config, project_root=tmp_path, dbt_models_dir=models, warn=False
    )
    seed = tmp_path / SLO_SEED_RELPATH
    assert seed.is_file()
    assert any(
        a.path.name == "ops_slo_expected.csv" and a.action == "write"
        for a in first.actions
    )
    original = seed.read_text(encoding="utf-8")
    seed.write_text("stale\n", encoding="utf-8")
    second = scaffold_dbt(
        config, project_root=tmp_path, dbt_models_dir=models, warn=False
    )
    assert seed.read_text(encoding="utf-8") == original
    assert any(
        a.path.name == "ops_slo_expected.csv" and a.action == "write"
        for a in second.actions
    )
