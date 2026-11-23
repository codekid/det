"""Unit tests for BigQuery bronze naming and schema derivation (no BQ credentials)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from det.ingestion.bq_writer import (
    BQ_META_TYPES,
    _nested_json_fields,
    _rows_to_ndjson,
    bq_dataset_for_config,
    bq_layout_destination,
    bq_normalize_ident,
    bq_project_from_settings,
    bq_schema_columns,
    bq_schema_from_json_schema,
    bq_table_for_config,
    bq_type_for_prop,
    resolve_bq_partition_by,
)
from det.runtime.config import DestinationConfig, PipelineConfig
from det.runtime.config.destination import BigQueryDestinationPartitionConfig
from det.runtime.settings import DetSettings


def _storm_config(**dest_kwargs) -> PipelineConfig:
    return PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
            "schema": "schemas/noaa/storm_events/storm_events.schema.yaml",
            "destination": {"type": "bigquery", **dest_kwargs},
            "wire_version": 1,
        }
    )


def test_bq_normalize_ident_hyphens() -> None:
    assert bq_normalize_ident("open-library") == "open_library"


def test_bq_normalize_ident_rejects_bad() -> None:
    with pytest.raises(ValueError, match="identifier"):
        bq_normalize_ident("1bad")
    with pytest.raises(ValueError, match="identifier"):
        bq_normalize_ident("has.dot")


def test_destination_bq_dataset_bigquery_only() -> None:
    with pytest.raises(ValidationError, match="bq_dataset"):
        DestinationConfig(type="iceberg", bq_dataset="my_exploration")
    dest = DestinationConfig(type="bigquery", bq_dataset="my_exploration")
    assert dest.bq_dataset == "my_exploration"


def test_bq_dataset_derived_default() -> None:
    cfg = _storm_config()
    assert bq_dataset_for_config(cfg) == "bronze_noaa"


def test_bq_dataset_prefix_from_settings() -> None:
    cfg = _storm_config()
    settings = DetSettings.from_env(project_root=Path.cwd()).with_overrides(
        bq_dataset_prefix="staging_bronze"
    )
    assert bq_dataset_for_config(cfg, settings) == "staging_bronze_noaa"


def test_bq_dataset_yaml_override() -> None:
    cfg = _storm_config(bq_dataset="my_exploration")
    assert bq_dataset_for_config(cfg) == "my_exploration"


def test_bq_dataset_runtime_override_wins() -> None:
    cfg = _storm_config(bq_dataset="from_yaml")
    assert (
        bq_dataset_for_config(cfg, bq_dataset_override="from_cli") == "from_cli"
    )


def test_bq_table_includes_wire_version() -> None:
    cfg = _storm_config()
    assert bq_table_for_config(cfg) == "storm_events_v1"
    cfg2 = PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
            "schema": "schemas/noaa/storm_events/storm_events.schema.yaml",
            "destination": {"type": "bigquery"},
            "wire_version": 2,
        }
    )
    assert bq_table_for_config(cfg2) == "storm_events_v2"


def test_bq_project_required() -> None:
    settings = DetSettings.from_env(project_root=Path.cwd(), env={}).with_overrides(
        bq_project=None
    )
    # from_env may still pick process env; force clear:
    cleared = settings.with_overrides(bq_project="")
    with pytest.raises(ValueError, match="DET_GCP_PROJECT"):
        bq_project_from_settings(cleared)


def test_bq_project_from_settings_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DET_GCP_PROJECT", "my-gcp-project")
    settings = DetSettings.from_env(project_root=tmp_path)
    assert bq_project_from_settings(settings) == "my-gcp-project"


def test_bq_type_for_prop_scalars() -> None:
    assert bq_type_for_prop({"type": "integer"}) == "INT64"
    assert bq_type_for_prop({"type": ["integer", "null"]}) == "INT64"
    assert bq_type_for_prop({"type": "number"}) == "FLOAT64"
    assert bq_type_for_prop({"type": "boolean"}) == "BOOL"
    assert bq_type_for_prop({"type": "string"}) == "STRING"


def test_bq_type_number_precedes_integer_union() -> None:
    """JSON Schema number∪integer must load as FLOAT64 so fractions survive."""
    prop = {"type": ["integer", "number", "null"]}
    assert bq_type_for_prop(prop) == "FLOAT64"
    cols = dict(
        bq_schema_columns(
            {
                "type": "object",
                "properties": {"amount": prop, "count": {"type": "integer"}},
            }
        )
    )
    assert cols["amount"] == "FLOAT64"
    assert cols["count"] == "INT64"
    payload = json.loads(_rows_to_ndjson([{"amount": 1.5, "count": 2}]).decode())
    assert payload["amount"] == 1.5
    assert isinstance(payload["amount"], float)


def test_bq_type_for_prop_nested() -> None:
    assert bq_type_for_prop({"type": "object", "properties": {"a": {"type": "string"}}}) == "JSON"
    assert bq_type_for_prop({"type": "array", "items": {"type": "string"}}) == "JSON"


def test_rows_to_ndjson_decodes_iceberg_nested_json_strings_only() -> None:
    schema = {
        "type": "object",
        "properties": {
            "payload": {"type": "object", "properties": {"a": {"type": "integer"}}},
            "tags": {"type": "array", "items": {"type": "string"}},
            "note": {"type": "string"},
        },
    }
    nested = _nested_json_fields(schema)
    assert nested == frozenset({"payload", "tags"})
    raw = {
        "payload": json.dumps({"a": 1}),
        "tags": json.dumps(["x", "y"]),
        "note": '{"keep":"as-string"}',
        "already": {"b": 2},
    }
    line = json.loads(
        _rows_to_ndjson([raw], nested_fields=nested).decode().strip()
    )
    assert line["payload"] == {"a": 1}
    assert line["tags"] == ["x", "y"]
    assert line["note"] == '{"keep":"as-string"}'
    assert line["already"] == {"b": 2}


def test_rows_to_ndjson_rejects_invalid_nested_json_text() -> None:
    with pytest.raises(ValueError, match="invalid JSON"):
        _rows_to_ndjson(
            [{"payload": "{not-json"}],
            nested_fields=frozenset({"payload"}),
        )
    with pytest.raises(ValueError, match="object/array"):
        _rows_to_ndjson(
            [{"payload": '"just-a-string"'}],
            nested_fields=frozenset({"payload"}),
        )


def test_bq_schema_columns_appends_meta() -> None:
    schema = {
        "type": "object",
        "properties": {
            "event_id": {"type": "integer"},
            "name": {"type": "string"},
        },
    }
    cols = bq_schema_columns(schema)
    names = [n for n, _ in cols]
    assert names[:2] == ["event_id", "name"]
    for meta in BQ_META_TYPES:
        assert meta in names
    by_name = dict(cols)
    assert by_name["event_id"] == "INT64"
    assert by_name["__extract_run_datetime"] == "TIMESTAMP"
    assert by_name["__row_hash"] == "STRING"


def test_resolve_bq_partition_defaults_to_extract_run() -> None:
    dest = DestinationConfig(type="bigquery")
    part = resolve_bq_partition_by(dest)
    assert part is not None
    assert part.field == "__extract_run_datetime"
    assert part.data_type == "timestamp"
    assert part.granularity == "day"


def test_resolve_bq_partition_none() -> None:
    dest = DestinationConfig(type="bigquery", partition="none")
    assert resolve_bq_partition_by(dest) is None


def test_resolve_bq_partition_source_override_wins() -> None:
    dest = DestinationConfig.model_validate(
        {
            "type": "bigquery",
            "partition": "extract_run",
            "bigquery": {
                "partition_by": {
                    "field": "event_time",
                    "data_type": "timestamp",
                    "granularity": "hour",
                },
                "cluster_by": ["event_id"],
            },
        }
    )
    part = resolve_bq_partition_by(dest)
    assert part is not None
    assert part.field == "event_time"
    assert part.granularity == "hour"
    assert dest.bigquery is not None
    assert dest.bigquery.cluster_by == ["event_id"]


def test_bigquery_block_rejected_on_iceberg() -> None:
    with pytest.raises(ValidationError, match="destination.bigquery"):
        DestinationConfig.model_validate(
            {
                "type": "iceberg",
                "bigquery": {"partition_by": {"field": "event_time"}},
            }
        )


def test_bigquery_require_partition_filter_needs_partition_by() -> None:
    with pytest.raises(ValidationError, match="require_partition_filter"):
        DestinationConfig.model_validate(
            {
                "type": "bigquery",
                "bigquery": {"require_partition_filter": True},
            }
        )


def test_bq_layout_destination_for_export_from_filesystem() -> None:
    cfg = PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
            "schema": "schemas/noaa/storm_events/storm_events.schema.yaml",
            "destination": {"type": "filesystem"},
            "wire_version": 1,
        }
    )
    layout = bq_layout_destination(cfg)
    assert layout.type == "bigquery"
    assert layout.partition == "extract_run"
    part = resolve_bq_partition_by(layout)
    assert part is not None
    assert part.field == "__extract_run_datetime"


def test_require_partition_filter_custom_field_rejected_at_resolve() -> None:
    dest = DestinationConfig.model_validate(
        {
            "type": "bigquery",
            "bigquery": {
                "partition_by": {
                    "field": "event_time",
                    "data_type": "timestamp",
                    "granularity": "day",
                },
                "require_partition_filter": True,
            },
        }
    )
    with pytest.raises(ValueError, match="incompatible with replace-by-run"):
        resolve_bq_partition_by(dest)


def test_bq_schema_partition_overrides_payload_string_type() -> None:
    pytest.importorskip("google.cloud.bigquery")
    schema = {
        "type": "object",
        "properties": {
            "event_time": {"type": "string"},
            "event_id": {"type": "integer"},
        },
    }
    part = BigQueryDestinationPartitionConfig(
        field="event_time",
        data_type="timestamp",
        granularity="day",
    )
    fields = bq_schema_from_json_schema(schema, partition=part)
    by_name = {f.name: f.field_type for f in fields}
    assert by_name["event_time"] == "TIMESTAMP"
    assert by_name["event_id"] == "INT64"
    assert by_name["__extract_run_datetime"] == "TIMESTAMP"


def test_bq_schema_partition_missing_field_raises() -> None:
    pytest.importorskip("google.cloud.bigquery")
    schema = {"type": "object", "properties": {"event_id": {"type": "integer"}}}
    part = BigQueryDestinationPartitionConfig(
        field="event_time",
        data_type="timestamp",
        granularity="day",
    )
    with pytest.raises(ValueError, match="not a schema column"):
        bq_schema_from_json_schema(schema, partition=part)
