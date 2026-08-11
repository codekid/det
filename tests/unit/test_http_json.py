"""Unit tests for shared JSON HTTP page helpers."""

from __future__ import annotations

import json
from pathlib import Path

from det.sources.http_json import dig, nest_under_path, write_json_page


def test_dig_walks_dotted_path():
    assert dig({"data": {"events": [1]}}, "data.events") == [1]
    assert dig({"data": {}}, "data.events") is None
    assert dig([], "data.events") is None


def test_nest_under_path():
    assert nest_under_path([{"id": 1}], record_path="data.events") == {
        "data": {"events": [{"id": 1}]}
    }


def test_write_json_page(tmp_path: Path):
    data_dir = tmp_path / "data"
    pages_dir = data_dir / "pages"
    pages_dir.mkdir(parents=True)
    art = write_json_page(
        pages_dir=pages_dir,
        data_dir=data_dir,
        page_num=1,
        body={"works": []},
        origin="fixture",
    )
    assert art["path"] == "data/pages/0001.json"
    assert art["format"] == "json_page"
    assert art["origin"] == "fixture"
    payload = json.loads((tmp_path / art["path"]).read_text(encoding="utf-8"))
    assert payload == {"works": []}
