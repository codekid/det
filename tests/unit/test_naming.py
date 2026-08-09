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
    row = {"BEGIN_DAY": "15"}
    assert apply_naming(row, BronzeNamingConfig(style="identity")) == row


def test_apply_naming_collision_raises():
    with pytest.raises(ValueError, match="Naming collision"):
        apply_naming({"begin_day": "1", "BEGIN_DAY": "2"}, BronzeNamingConfig())
