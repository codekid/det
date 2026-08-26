from __future__ import annotations

import pytest

from det.runtime.coerce import CoerceError, coerce_record, coerce_value
from det.runtime.naming import BronzeNamingConfig, apply_naming
from det.validation.jsonschema_validator import validate_records


def test_coerce_integer_from_string():
    assert coerce_value("15", {"type": "integer"}, field="begin_day") == 15
    assert coerce_value("51.00", {"type": "integer"}, field="tor_width") == 51


def test_coerce_blank_to_null():
    assert coerce_value("", {"type": ["number", "null"]}, field="magnitude") is None
    assert coerce_value("  ", {"type": ["number", "null"]}, field="tor_length") is None


def test_coerce_number():
    assert coerce_value("2.5", {"type": ["number", "null"]}, field="tor_length") == 2.5
    assert coerce_value("51.00", {"type": ["number", "null"]}, field="magnitude") == 51.0
    assert coerce_value(51, {"type": "number"}, field="magnitude") == 51.0
    assert coerce_value("51", {"type": "number"}, field="magnitude") == 51.0


def test_coerce_record_uses_schema_properties():
    schema = {
        "type": "object",
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
            "magnitude": {"type": ["number", "null"]},
        },
    }
    out = coerce_record(
        {"event_id": "9001", "state": "TEXAS", "magnitude": ""},
        schema,
    )
    assert out == {"event_id": 9001, "state": "TEXAS", "magnitude": None}


def test_coerce_invalid_integer_raises():
    with pytest.raises(CoerceError, match="begin_day"):
        coerce_value("x", {"type": "integer"}, field="begin_day")
    with pytest.raises(CoerceError, match="begin_day"):
        coerce_value("1.5", {"type": "integer"}, field="begin_day")


def test_coerce_nested_object():
    schema = {
        "type": "object",
        "properties": {
            "geo": {
                "type": "object",
                "properties": {
                    "lat_lon": {"type": "number"},
                    "state_code": {"type": "string"},
                },
            }
        },
    }
    out = coerce_record({"geo": {"lat_lon": "32.1", "state_code": "TX"}}, schema)
    assert out == {"geo": {"lat_lon": 32.1, "state_code": "TX"}}


def test_coerce_nested_error_includes_path():
    schema = {
        "type": "object",
        "properties": {
            "geo": {
                "type": "object",
                "properties": {"lat_lon": {"type": "integer"}},
            }
        },
    }
    with pytest.raises(CoerceError, match=r"geo\.lat_lon"):
        coerce_record({"geo": {"lat_lon": "1.5"}}, schema)


def test_coerce_array_of_objects():
    schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"foo_bar": {"type": "integer"}},
                },
            }
        },
    }
    out = coerce_record({"items": [{"foo_bar": "1"}, {"foo_bar": "2"}]}, schema)
    assert out == {"items": [{"foo_bar": 1}, {"foo_bar": 2}]}


def test_coerce_unknown_nested_key_passthrough():
    schema = {
        "type": "object",
        "properties": {
            "geo": {
                "type": "object",
                "properties": {"lat_lon": {"type": "number"}},
            }
        },
    }
    out = coerce_record(
        {"geo": {"lat_lon": "1", "extra_flag": "yes"}},
        schema,
    )
    assert out == {"geo": {"lat_lon": 1.0, "extra_flag": "yes"}}


def test_coerce_ref_left_as_is():
    prop = {"$ref": "#/$defs/geo"}
    value = {"latLon": "1"}
    assert coerce_value(value, prop, field="geo") == value


def test_load_path_nested_golden_name_coerce_validate():
    """Wire camelCase + stringly types → snake_case nested bronze contract."""
    schema = {
        "type": "object",
        "required": ["event_id", "geo"],
        "additionalProperties": False,
        "properties": {
            "event_id": {"type": "integer"},
            "geo": {
                "type": "object",
                "required": ["lat_lon", "state_code"],
                "additionalProperties": False,
                "properties": {
                    "lat_lon": {"type": "number"},
                    "state_code": {"type": "string"},
                },
            },
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["foo_bar"],
                    "additionalProperties": False,
                    "properties": {"foo_bar": {"type": "integer"}},
                },
            },
        },
    }
    wire = {
        "eventId": "9001",
        "geo": {"latLon": "32.10", "stateCode": "TX"},
        "items": [{"fooBar": "7"}],
    }
    named = apply_naming(wire, BronzeNamingConfig())
    typed = coerce_record(named, schema)
    assert typed == {
        "event_id": 9001,
        "geo": {"lat_lon": 32.1, "state_code": "TX"},
        "items": [{"foo_bar": 7}],
    }
    validate_records([typed], schema)
