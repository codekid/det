"""Unit tests for shared JSON HTTP page helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from det.sources.http_json import dig, nest_under_path, paginate_capped, write_json_page


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


def test_paginate_capped_yields_all_within_limit():
    pages = [{"id": i} for i in range(5)]
    result = list(paginate_capped(iter(pages), max_pages=10))
    assert [(n, p) for n, p in result] == [(i + 1, {"id": i}) for i in range(5)]


def test_paginate_capped_raises_at_limit():
    with pytest.raises(RuntimeError, match="pagination cap"):
        for _ in paginate_capped(range(100), max_pages=3):
            pass


def test_paginate_capped_includes_source_name_in_error():
    with pytest.raises(RuntimeError, match="my.source"):
        for _ in paginate_capped(range(10), max_pages=2, source_name="my.source"):
            pass
