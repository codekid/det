from __future__ import annotations

from pathlib import Path

import yaml

from det.runtime.config import load_pipeline_config
from det.scaffold.dbt import scaffold_dbt, stg_columns_from_schema


def _write_mini_project(tmp_path: Path) -> Path:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["event_id", "state"],
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
            "magnitude": {"type": ["number", "null"]},
        },
        "additionalProperties": False,
    }
    schema_path = tmp_path / "schemas" / "noaa" / "storm_events" / "storm_events.schema.yaml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(yaml.safe_dump(schema), encoding="utf-8")

    pipeline = tmp_path / "configs" / "pipelines" / "noaa" / "storm_events.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/noaa/storm_events/storm_events.schema.yaml
dbt:
  silver:
    materialized: incremental
    unique_key: [event_id]
    order_by: ["__extract_run_datetime desc"]
    watermark: __extract_run_datetime
    lookback: 3 days
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    return pipeline


def test_stg_columns_string_cleaned_and_meta_appended():
    schema = {
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
        }
    }
    cols = stg_columns_from_schema(schema)
    by_name = {c["name"]: c["expr"] for c in cols}
    assert by_name["event_id"] == "event_id"
    assert "nullif(trim" in by_name["state"]
    assert by_name["__row_hash"] == "__row_hash"


def test_scaffold_creates_and_skips_without_force(tmp_path: Path):
    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"

    first = scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    assert (models / "stg_noaa_storm_events.sql").exists()
    assert (models / "silver_noaa_storm_events.sql").exists()
    assert any(a.action == "write" for a in first.actions)

    stg = (models / "stg_noaa_storm_events.sql").read_text(encoding="utf-8")
    assert 'det_bronze_from("storm_events")' in stg
    assert "nullif(trim(cast(state as varchar))" in stg
    assert "__extract_run_datetime" in stg

    silver = (models / "silver_noaa_storm_events.sql").read_text(encoding="utf-8")
    assert 'materialized="incremental"' in silver
    assert 'unique_key=["event_id"]' in silver
    assert "is_incremental()" in silver
    assert "interval '3 days'" in silver
    assert 'partition_by=["event_id"]' in silver
    assert "det_dedupe_latest_run" in silver
    assert 'ref("stg_noaa_storm_events")' in silver

    sources = yaml.safe_load((models / "sources.yml").read_text(encoding="utf-8"))
    assert sources["sources"][0]["name"] == "bronze_noaa"
    names = [t["name"] for t in sources["sources"][0]["tables"]]
    assert "storm_events" in names

    second = scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    assert all(a.action == "skip" for a in second.actions)


def test_scaffold_force_refreshes_stg_when_schema_gains_property(tmp_path: Path):
    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)

    schema_path = (
        tmp_path / "schemas" / "noaa" / "storm_events" / "storm_events.schema.yaml"
    )
    schema = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    schema["properties"]["cz_name"] = {"type": ["string", "null"]}
    schema_path.write_text(yaml.safe_dump(schema), encoding="utf-8")

    scaffold_dbt(config, project_root=tmp_path, force=True, dbt_models_dir=models)
    stg = (models / "stg_noaa_storm_events.sql").read_text(encoding="utf-8")
    assert "cz_name" in stg


def test_scaffold_dry_run_writes_nothing(tmp_path: Path):
    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    result = scaffold_dbt(
        config, project_root=tmp_path, dry_run=True, dbt_models_dir=models
    )
    assert all(a.action.startswith("would_") for a in result.actions)
    assert not models.exists()
