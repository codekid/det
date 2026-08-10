from __future__ import annotations

import pytest

from det.runtime.naming import BronzeNamingConfig, apply_naming, to_snake_case


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("BEGIN_DAY", "begin_day"),
        ("beginDay", "begin_day"),
        ("Begin-Day", "begin_day"),
        ("HTTPResponse", "http_response"),
        ("__row_hash", "__row_hash"),
    ],
)
def test_to_snake_case(raw: str, expected: str):
    assert to_snake_case(raw) == expected


def test_apply_naming_snake_case_default():
    out = apply_naming({"BEGIN_DAY": "15", "EVENT_ID": "1"}, BronzeNamingConfig())
    assert out == {"begin_day": "15", "event_id": "1"}


def test_apply_naming_identity():
    row = {"BEGIN_DAY": "15", "geo": {"latLon": "1"}}
    assert apply_naming(row, BronzeNamingConfig(style="identity")) == row


def test_apply_naming_collision_raises():
    with pytest.raises(ValueError, match="Naming collision"):
        apply_naming({"begin_day": "1", "BEGIN_DAY": "2"}, BronzeNamingConfig())


def test_apply_naming_nested_object():
    out = apply_naming(
        {"eventId": "1", "geo": {"latLon": "32.1", "STATE_CODE": "TX"}},
        BronzeNamingConfig(),
    )
    assert out == {
        "event_id": "1",
        "geo": {"lat_lon": "32.1", "state_code": "TX"},
    }


def test_apply_naming_nested_collision_includes_path():
    with pytest.raises(ValueError, match=r"Naming collision at geo:"):
        apply_naming(
            {"geo": {"lat_lon": "1", "latLon": "2"}},
            BronzeNamingConfig(),
        )


def test_apply_naming_array_of_objects():
    out = apply_naming(
        {"items": [{"fooBar": "a"}, {"fooBar": "b"}]},
        BronzeNamingConfig(),
    )
    assert out == {"items": [{"foo_bar": "a"}, {"foo_bar": "b"}]}


def test_apply_naming_preserves_nested_meta_keys():
    out = apply_naming({"outer": {"__keep": 1, "innerKey": 2}}, BronzeNamingConfig())
    assert out == {"outer": {"__keep": 1, "inner_key": 2}}
