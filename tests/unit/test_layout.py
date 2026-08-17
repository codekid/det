from __future__ import annotations

from det.runtime.layout import LAKE_LAYOUT, lake_layout_of


def test_lake_layout_constant_is_one():
    assert LAKE_LAYOUT == 1


def test_lake_layout_of_defaults_missing_to_one():
    assert lake_layout_of(None) == 1
    assert lake_layout_of({}) == 1
    assert lake_layout_of({"wire_version": 2}) == 1
    assert lake_layout_of({"lake_layout": "nope"}) == 1
    assert lake_layout_of({"lake_layout": 0}) == 1


def test_lake_layout_of_reads_int():
    assert lake_layout_of({"lake_layout": 1}) == 1
    assert lake_layout_of({"lake_layout": "1"}) == 1
    assert lake_layout_of({"lake_layout": 2}) == 2
