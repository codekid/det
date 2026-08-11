from __future__ import annotations

import pytest

from det.sources.openlibrary.subjects import (
    _project_work,
    _subject_key,
    _subject_path,
    _subject_slug,
)


@pytest.mark.parametrize(
    ("raw", "slug"),
    [
        ("love", "love"),
        ("/subjects/love", "love"),
        ("subjects/love", "love"),
        ("/subjects/love.json", "love"),
    ],
)
def test_subject_slug_and_path(raw: str, slug: str):
    assert _subject_slug(raw) == slug
    assert _subject_path(raw) == f"/subjects/{slug}.json"
    assert _subject_key(raw) == f"/subjects/{slug}"


def test_project_work_adds_subject_key_and_trims_availability():
    work = {
        "key": "/works/OL1W",
        "title": "Demo",
        "authors": [{"key": "/authors/OL1A", "name": "Ada", "extra": 1}],
        "availability": {
            "status": "open",
            "__src__": "ignore-me",
            "is_readable": True,
        },
        "unknown_field": True,
    }
    out = _project_work(work, subject_key="/subjects/love")
    assert out["subject_key"] == "/subjects/love"
    assert out["key"] == "/works/OL1W"
    assert "unknown_field" not in out
    assert out["authors"] == [{"key": "/authors/OL1A", "name": "Ada"}]
    assert out["availability"] == {"status": "open", "is_readable": True}
    assert "__src__" not in out["availability"]
