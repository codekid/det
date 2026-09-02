from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from det.runtime.config import DbtStgConfig, load_pipeline_config
from det.scaffold.dbt import (
    _is_identity_name,
    _order_stg_select_columns,
    _ordered_meta_columns,
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
    names = [c["name"] for c in cols]
    assert names[0] == "event_id"
    assert names.index("magnitude") < names.index("state")
    assert names.index("__row_hash") < names.index("__bronze_loaded_at")
    assert names.index("__row_hash") < names.index("__filename")


def test_is_identity_name_heuristic():
    assert _is_identity_name("id")
    assert _is_identity_name("event_id")
    assert _is_identity_name("key")
    assert _is_identity_name("subject_key")
    assert not _is_identity_name("status")
    assert not _is_identity_name("__row_hash")
    assert not _is_identity_name("begin_day")


def test_order_stg_select_columns_identity_payload_meta():
    cols = [
        {"name": "state", "expr": "state"},
        {"name": "begin_day", "expr": "begin_day"},
        {"name": "episode_id", "expr": "episode_id"},
        {"name": "event_id", "expr": "event_id"},
    ]
    ordered = _order_stg_select_columns(cols, unique_key=["event_id"])
    names = [c["name"] for c in ordered]
    assert names[:4] == ["episode_id", "event_id", "begin_day", "state"]
    meta = _ordered_meta_columns()
    assert meta[0] == "__row_hash"
    assert meta == ["__row_hash"] + sorted(m for m in meta if m != "__row_hash")
    assert names[names.index("__row_hash") :] == meta


def test_scaffold_stg_sql_column_order(tmp_path: Path):
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["event_id"],
        "properties": {
            "begin_day": {"type": "integer"},
            "zebra": {"type": "string"},
            "event_id": {"type": "integer"},
            "apple": {"type": "string"},
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
    unique_key: [event_id]
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    stg = (models / "stg_noaa__storm_events.sql").read_text(encoding="utf-8")
    eid = stg.index("as event_id")
    assert eid < stg.index("as apple")
    assert eid < stg.index("as begin_day")
    assert stg.index("as apple") < stg.index("as begin_day")
    assert stg.index("as begin_day") < stg.index("as zebra")
    assert stg.index("__row_hash") < stg.index("__bronze_loaded_at")
    assert stg.index("__row_hash") < stg.index("__filename")


def test_read_json_columns_from_schema_includes_meta():
    schema = {
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
            "magnitude": {"type": ["number", "null"]},
        }
    }
    cols = read_json_columns_from_schema(schema)
    assert cols["event_id"] == "BIGINT"
    assert cols["state"] == "VARCHAR"
    assert cols["magnitude"] == "DOUBLE"
    assert cols["__row_hash"] == "VARCHAR"
    assert cols["__extract_run_datetime"] == "TIMESTAMP"
    assert cols["__bronze_loaded_at"] == "TIMESTAMP"
    struct = format_read_json_columns(cols)
    assert "'event_id': 'BIGINT'" in struct
    assert struct.startswith("{") and struct.endswith("}")


def test_scaffolded_sql_is_parseable_jinja(tmp_path: Path):
    """Generated models must be syntactically valid Jinja, not just contain the
    right substrings. Guards against emitting `{{ ... }}` nested inside a
    `{{ config(...) }}` block, which dbt cannot compile."""
    from jinja2 import Environment

    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)

    generated = sorted(models.glob("*.sql"))
    assert generated, "scaffold produced no SQL models"

    env = Environment()
    for path in generated:
        env.parse(path.read_text(encoding="utf-8"))


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
    assert "'event_id': 'BIGINT'" in sources_text
    assert "formatter:" in sources_text
    assert "target.name != 'bigquery'" in sources_text
    assert "target.name == 'bigquery'" in sources_text
    assert "storm_events_v1" in sources_text
    assert not (models / "sources_bigquery.yml").exists()
    # Jinja must survive YAML round-trip for dbt.
    assert "DET_LAKE_PATH" in sources_text
    assert "det_lake_bronze_path" in sources_text or "DET_LAKE_PATH_BRONZE" in sources_text or "/bronze/" in sources_text or "~ '/bronze'" in sources_text

    second = scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    seed_actions = [a for a in second.actions if a.path.name == "ops_slo_expected.csv"]
    other = [a for a in second.actions if a.path.name != "ops_slo_expected.csv"]
    assert other and all(a.action == "skip" for a in other)
    assert seed_actions and seed_actions[0].action == "write"


def test_scaffold_bootstraps_generate_schema_name_never_force(tmp_path: Path):
    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    macro = tmp_path / "dbt" / "macros" / "generate_schema_name.sql"

    first = scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    assert macro.is_file()
    assert "custom_schema_name" in macro.read_text(encoding="utf-8")
    assert any(
        a.action == "write" and a.path == macro.resolve() and a.detail == "create"
        for a in first.actions
    )

    macro.write_text("-- embedder custom\n", encoding="utf-8")
    forced = scaffold_dbt(
        config, project_root=tmp_path, force=True, dbt_models_dir=models
    )
    assert macro.read_text(encoding="utf-8") == "-- embedder custom\n"
    assert any(
        a.action == "skip" and a.path == macro.resolve() for a in forced.actions
    )


def test_bootstrap_generate_schema_name_rejects_symlink_escape(tmp_path: Path):
    from det.scaffold.dbt import _bootstrap_generate_schema_name

    outside = tmp_path / "outside"
    outside.mkdir()
    project = tmp_path / "project"
    dbt = project / "dbt"
    dbt.mkdir(parents=True)
    (dbt / "macros").symlink_to(outside)

    with pytest.raises(ValueError, match="escapes project root"):
        _bootstrap_generate_schema_name(
            project, dry_run=False, actions=[]
        )
    assert not (outside / "generate_schema_name.sql").exists()


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
    assert any(a.path.name == "ops_slo_expected.csv" for a in result.actions)
    assert not models.exists()
    assert not (tmp_path / "dbt" / "seeds" / "ops_slo_expected.csv").exists()


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
    assert "DET_LAKE_PATH" in sources_text
    assert "det_lake_bronze_path" in sources_text or "DET_LAKE_PATH_BRONZE" in sources_text or "/bronze/" in sources_text or "~ '/bronze'" in sources_text


def test_scaffold_silver_omits_bigquery_layout_when_unset(tmp_path: Path):
    pipeline = _write_mini_project(tmp_path)
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models)
    silver = (models / "silver_noaa__storm_events.sql").read_text(encoding="utf-8")
    assert "cluster_by=" not in silver
    assert "require_partition_filter" not in silver
    assert '"field":' not in silver
    assert "partition_by={" not in silver


def test_scaffold_silver_emits_bigquery_partition_cluster(tmp_path: Path):
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["id"],
        "properties": {
            "id": {"type": "integer"},
            "abilities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "slot": {"type": "integer"},
                        "is_hidden": {"type": "boolean"},
                    },
                },
            },
        },
        "additionalProperties": False,
    }
    schema_path = (
        tmp_path / "schemas" / "pokeapi" / "pokemon" / "pokemon.schema.yaml"
    )
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(yaml.safe_dump(schema), encoding="utf-8")
    pipeline = tmp_path / "configs" / "pipelines" / "pokeapi" / "pokemon.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: pokeapi.pokemon
source:
  type: pokeapi.pokemon
schema: schemas/pokeapi/pokemon/pokemon.schema.yaml
dbt:
  silver:
    materialized: incremental
    unique_key: [id]
    order_by: ["__extract_run_datetime desc"]
    watermark: __extract_run_datetime
    bigquery:
      partition_by:
        field: __extract_run_datetime
        data_type: timestamp
        granularity: day
      cluster_by: [id]
      require_partition_filter: true
  stg:
    relations:
      abilities:
        path: abilities
        materialized: table
        parent_key: id
        grain: [slot]
        bigquery:
          partition_by:
            field: __extract_run_datetime
            data_type: timestamp
            granularity: day
          cluster_by: [id, abilities__slot]
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    assert config.dbt.silver.bigquery is not None
    assert config.dbt.silver.bigquery.require_partition_filter is True
    assert config.dbt.stg.relations["abilities"].bigquery is not None

    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models, warn=False)

    silver = (models / "silver_pokeapi__pokemon.sql").read_text(encoding="utf-8")
    assert "target.name == 'bigquery'" in silver
    assert "partition_by=" in silver
    assert '"field": "__extract_run_datetime"' in silver
    assert '"data_type": "timestamp"' in silver
    assert '"granularity": "day"' in silver
    assert 'cluster_by=["id"]' in silver
    assert "require_partition_filter=true" in silver

    rel = (models / "silver_pokeapi__pokemon__abilities.sql").read_text(
        encoding="utf-8"
    )
    assert 'materialized="table"' in rel
    assert "target.name == 'bigquery'" in rel
    assert '"field": "__extract_run_datetime"' in rel
    assert 'cluster_by=["id", "abilities__slot"]' in rel
    assert "require_partition_filter" not in rel
