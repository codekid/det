from __future__ import annotations

import pytest

from det.runtime.coerce import CoerceError, coerce_record, coerce_value


def test_coerce_integer_from_string():
    assert coerce_value("15", {"type": "integer"}, field="begin_day") == 15
    assert coerce_value("51.00", {"type": "integer"}, field="tor_width") == 51


def test_coerce_blank_to_null():
    assert coerce_value("", {"type": ["number", "null"]}, field="magnitude") is None
    assert coerce_value("  ", {"type": ["number", "null"]}, field="tor_length") is None


def test_coerce_number():
    assert coerce_value("2.5", {"type": ["number", "null"]}, field="tor_length") == 2.5
    assert coerce_value("51.00", {"type": ["number", "null"]}, field="magnitude") == 51.0


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
