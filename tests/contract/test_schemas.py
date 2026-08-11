from __future__ import annotations

import pytest

from det.runtime.config import load_pipeline_config, resolve_path
from det.validation.jsonschema_validator import (
    SchemaValidationError,
    load_json_schema,
    validate_records,
)


def test_every_source_implements_raw_contract(project_root):
    from det.runtime.registry import get_source, list_sources
    from det.sources.base import Interval

    interval = Interval(start="2026-08-06T00:00:00+00:00", end="2026-08-07T00:00:00+00:00")
    for name in list_sources():
        source = get_source(name)
        assert callable(getattr(source, "extract_to_raw", None)), name
        assert callable(getattr(source, "records_from_raw", None)), name
        assert isinstance(source.defaults(), dict)
        assert interval.start < interval.end


def test_every_pipeline_points_at_an_existing_schema(project_root):
    pipelines = sorted((project_root / "configs/pipelines").rglob("*.yaml"))
    assert pipelines
    for pipeline in pipelines:
        cfg = load_pipeline_config(pipeline)
        schema_path = resolve_path(project_root, cfg.schema_path)
        assert schema_path.exists(), f"{pipeline} -> missing {cfg.schema_path}"
        load_json_schema(schema_path)


def test_storm_schema_accepts_typed_canonical(project_root):
    schema = load_json_schema(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml")
    validate_records(
        [
            {
                "begin_day": 15,
                "begin_time": 1300,
                "event_id": 9001,
                "state": "TEXAS",
            }
        ],
        schema,
    )


def test_storm_schema_rejects_string_integers(project_root):
    schema = load_json_schema(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml")
    with pytest.raises(SchemaValidationError):
        validate_records(
            [
                {
                    "begin_day": "15",
                    "begin_time": "1300",
                    "event_id": "9001",
                }
            ],
            schema,
        )


def test_storm_schema_rejects_missing_required(project_root):
    schema = load_json_schema(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml")
    with pytest.raises(SchemaValidationError):
        validate_records([{"state": "TEXAS"}], schema)


def test_validation_error_message_names_the_offending_fields(project_root):
    """A bare error count leaves no way to tell which mapper or field is wrong."""
    schema = load_json_schema(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml")
    with pytest.raises(SchemaValidationError) as excinfo:
        validate_records([{"BEGIN_DAY": "15", "EVENT_ID": "9001"}], schema)
    message = str(excinfo.value)
    assert "BEGIN_DAY" in message
    assert "'begin_day' is a required property" in message


def test_meta_fields_not_required_in_schema(project_root):
    schema = load_json_schema(project_root / "schemas/noaa/storm_events/storm_events.schema.yaml")
    props = schema.get("properties", {})
    assert "__raw" not in props
    assert "__row_hash" not in props


def test_openlibrary_work_fields_align_with_schema(project_root):
    """Curated allowlist and JSON Schema properties must stay in sync (+ subject_key)."""
    from det.sources.openlibrary.subjects import _AVAILABILITY_FIELDS, _WORK_FIELDS

    schema = load_json_schema(
        project_root / "schemas/openlibrary/subjects/subjects.schema.yaml"
    )
    props = set(schema.get("properties") or {})
    assert set(_WORK_FIELDS) | {"subject_key"} == props

    availability = (schema.get("properties") or {}).get("availability") or {}
    avail_props = set((availability.get("properties") or {}))
    assert set(_AVAILABILITY_FIELDS) == avail_props
