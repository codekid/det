from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from det.runtime.config import DbtStgConfig, load_pipeline_config
from det.scaffold.dbt import (
    format_read_json_columns,
    read_json_columns_from_schema,
    scaffold_dbt,
    stg_columns_from_schema,
    widen_read_json_columns,
)


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


def test_stg_columns_typed_macros_and_meta_appended():
    schema = {
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
            "magnitude": {"type": ["number", "null"]},
        }
    }
    cols = stg_columns_from_schema(schema)
    by_name = {c["name"]: c["expr"] for c in cols}
    assert by_name["event_id"] == "{{ det_as_integer('event_id') }} as event_id"
    assert by_name["state"] == "{{ det_as_string('state') }} as state"
    assert by_name["magnitude"] == "{{ det_as_double('magnitude') }} as magnitude"
    assert by_name["__row_hash"] == "__row_hash"


def test_read_json_columns_from_schema_includes_meta():
    schema = {
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
            "magnitude": {"type": ["number", "null"]},
        }
    }
    cols = read_json_columns_from_schema(schema)
    assert cols["event_id"] == "INTEGER"
    assert cols["state"] == "VARCHAR"
    assert cols["magnitude"] == "DOUBLE"
    assert cols["__row_hash"] == "VARCHAR"
    assert cols["__extract_run_datetime"] == "TIMESTAMP"
    assert cols["__bronze_loaded_at"] == "TIMESTAMP"
    struct = format_read_json_columns(cols)
    assert "'event_id': 'INTEGER'" in struct
    assert struct.startswith("{") and struct.endswith("}")


def test_scaffold_creates_and_skips_without_force(tmp_path: Path):
    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"

    first = scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    assert (models / "stg_noaa__storm_events.sql").exists()
    assert (models / "silver_noaa__storm_events.sql").exists()
    assert any(a.action == "write" for a in first.actions)

    stg = (models / "stg_noaa__storm_events.sql").read_text(encoding="utf-8")
    assert 'det_bronze_from("storm_events_v1", "bronze_noaa")' in stg
    assert "{{ det_as_string('state') }}" in stg
    assert "{{ det_as_integer('event_id') }}" in stg
    assert "{{ det_as_double('magnitude') }}" in stg
    assert "__extract_run_datetime" in stg
    assert "__bronze_loaded_at" in stg

    silver = (models / "silver_noaa__storm_events.sql").read_text(encoding="utf-8")
    assert 'materialized="incremental"' in silver
    assert 'unique_key=["event_id"]' in silver
    assert "is_incremental()" in silver
    assert "interval '3 days'" in silver
    assert 'partition_by=["event_id"]' in silver
    assert "det_dedupe_latest_run" in silver
    assert 'ref("stg_noaa__storm_events")' in silver
    assert "__silver_processed_at" in silver
    assert "__silver_updated_at" in silver
    assert "deduped as (" in silver

    sources_text = (models / "sources.yml").read_text(encoding="utf-8")
    assert "bronze_noaa" in sources_text
    assert "read_json(" in sources_text
    assert "'event_id': 'INTEGER'" in sources_text
    assert "formatter: template" in sources_text
    assert "storm_events_v1" in sources_text
    # Jinja must survive YAML round-trip for dbt.
    assert 'env_var("DET_LAKE_PATH"' in sources_text or "env_var('DET_LAKE_PATH'" in sources_text

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
    stg = (models / "stg_noaa__storm_events.sql").read_text(encoding="utf-8")
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


def test_widen_read_json_includes_coalesce_aliases():
    schema = {
        "properties": {
            "severity": {"type": "string"},
            "state": {"type": ["string", "null"]},
        }
    }
    stg = DbtStgConfig(
        coalesce={"severity": ["severity", "severity_level", "level"]},
        exclude=["debug_info"],
    )
    cols = widen_read_json_columns(schema, stg)
    assert cols["severity"] == "VARCHAR"
    assert cols["severity_level"] == "VARCHAR"
    assert cols["level"] == "VARCHAR"
    assert cols["debug_info"] == "VARCHAR"
    assert cols["__row_hash"] == "VARCHAR"


def test_stg_columns_coalesce_sentinels_map_rename_exclude():
    schema = {
        "properties": {
            "severity": {"type": "string"},
            "state": {"type": ["string", "null"]},
            "status": {"type": "string"},
            "debug_info": {"type": "string"},
        }
    }
    stg = DbtStgConfig(
        coalesce={"severity": ["severity", "level"]},
        null_sentinels={"state": ["", "NA"]},
        map={"status": {"1": "open", "2": "closed"}},
        rename={"severity": "event_severity"},
        exclude=["debug_info"],
    )
    cols = stg_columns_from_schema(schema, stg)
    by_name = {c["name"]: c["expr"] for c in cols}
    assert "debug_info" not in by_name
    assert "severity" not in by_name
    sev = by_name["event_severity"]
    assert "coalesce(" in sev
    assert "det_as_string('severity')" in sev
    assert "det_as_string('level')" in sev
    assert sev.endswith(" as event_severity")
    assert "nullif(" in by_name["state"]
    assert "case when" in by_name["status"]
    assert "'open'" in by_name["status"]


def test_dbt_stg_config_rejects_empty_coalesce_and_rename_collision():
    with pytest.raises(ValidationError):
        DbtStgConfig(coalesce={"severity": []})
    with pytest.raises(ValidationError):
        DbtStgConfig(rename={"a": "x", "b": "x"})


def test_scaffold_applies_dbt_stg(tmp_path: Path):
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "string"},
            "severity": {"type": "string"},
            "state": {"type": ["string", "null"]},
        },
        "additionalProperties": False,
    }
    schema_path = (
        tmp_path / "schemas" / "example_api" / "events" / "events.schema.yaml"
    )
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(yaml.safe_dump(schema), encoding="utf-8")

    pipeline = tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: example_api.events
source:
  type: example_api.events
schema: schemas/example_api/events/events.schema.yaml
wire_version: 1
dbt:
  silver:
    unique_key: [__row_hash]
    order_by: ["__extract_run_datetime desc"]
    not_null: [id]
    unique: [id]
    accepted_values:
      event_severity: [low, medium, high]
  stg:
    coalesce:
      severity: [severity, level]
    null_sentinels:
      state: ["", "NA"]
    rename:
      severity: event_severity
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)

    stg = (models / "stg_example_api__events.sql").read_text(encoding="utf-8")
    assert 'schema="silver_example_api"' in stg
    assert "coalesce(" in stg
    assert "event_severity" in stg
    assert "nullif(" in stg

    silver_sql = (models / "silver_example_api__events.sql").read_text(encoding="utf-8")
    assert 'schema="silver_example_api"' in silver_sql

    sources = (models / "sources.yml").read_text(encoding="utf-8")
    assert "'level': 'VARCHAR'" in sources
    assert "'severity': 'VARCHAR'" in sources

    assert not (models / "_stg__models.yml").exists()
    silver_yml = yaml.safe_load(
        (models / "_silver__models.yml").read_text(encoding="utf-8")
    )
    model = next(
        m for m in silver_yml["models"] if m["name"] == "silver_example_api__events"
    )
    col_tests = {c["name"]: c["tests"] for c in model["columns"]}
    assert "unique" in col_tests["id"]
    assert "not_null" in col_tests["id"]
    assert any(
        isinstance(t, dict) and "accepted_values" in t
        for t in col_tests["event_severity"]
    )


def test_scaffold_propagates_schema_and_docs_descriptions(tmp_path: Path):
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "description": "Wire events contract for tests.",
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "string", "description": "Primary key from API."},
            "severity": {"type": "string", "description": "Legacy severity."},
            "level": {"type": "string", "description": "Current severity."},
            "state": {
                "type": ["string", "null"],
                "description": "Two-letter state code.",
            },
            "analytics_only_src": {"type": "string"},
        },
        "additionalProperties": False,
    }
    schema_path = (
        tmp_path / "schemas" / "example_api" / "events" / "events.schema.yaml"
    )
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(yaml.safe_dump(schema), encoding="utf-8")

    pipeline = tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: example_api.events
source:
  type: example_api.events
schema: schemas/example_api/events/events.schema.yaml
wire_version: 1
dbt:
  silver:
    unique_key: [__row_hash]
    order_by: ["__extract_run_datetime desc"]
    not_null: [id]
  stg:
    coalesce:
      severity: [severity, level]
    rename:
      severity: event_severity
    exclude: [level]
  docs:
    columns:
      event_severity: Analytics severity (docs overlay).
      state: State for reporting (docs overlay).
      report_bucket: Docs-only column with no schema property.
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)

    sources_text = (models / "sources.yml").read_text(encoding="utf-8")
    assert "Wire events contract for tests." in sources_text
    assert "Primary key from API." in sources_text
    assert "Legacy severity." in sources_text
    assert "DET content hash used for silver dedupe." in sources_text

    stg = (models / "stg_example_api__events.sql").read_text(encoding="utf-8")
    # stg SQL stays transforms-only (descriptions live in YAML only)
    assert "Analytics severity" not in stg
    assert "Wire events contract" not in stg
    assert "Primary key from API" not in stg

    silver_yml = yaml.safe_load(
        (models / "_silver__models.yml").read_text(encoding="utf-8")
    )
    model = next(
        m for m in silver_yml["models"] if m["name"] == "silver_example_api__events"
    )
    assert model["description"] == "Wire events contract for tests."
    by_name = {c["name"]: c for c in model["columns"]}
    assert by_name["event_severity"]["description"] == "Analytics severity (docs overlay)."
    assert by_name["state"]["description"] == "State for reporting (docs overlay)."
    assert by_name["id"]["description"] == "Primary key from API."
    assert by_name["report_bucket"]["description"] == (
        "Docs-only column with no schema property."
    )
    assert "tests" not in by_name["report_bucket"]
    assert by_name["__row_hash"]["description"] == (
        "DET content hash used for silver dedupe."
    )


def test_scaffold_iceberg_uses_iceberg_scan(tmp_path: Path):
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["event_id"],
        "properties": {"event_id": {"type": "integer"}},
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
destination:
  type: iceberg
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    sources_text = (models / "sources.yml").read_text(encoding="utf-8")
    assert "iceberg_scan(" in sources_text
    assert "**/data.jsonl" not in sources_text
    assert "storm_events_v1" in sources_text
    assert 'env_var("DET_LAKE_PATH"' in sources_text or "env_var('DET_LAKE_PATH'" in sources_text
