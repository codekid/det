from __future__ import annotations

import pytest

from det.sources.openlibrary.subjects import (
    _subject_key,
    _subject_path,
    _subject_slug,
)
from det.validation.jsonschema_validator import (
    SchemaValidationError,
    load_json_schema,
    validate_records,
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


def test_fixture_works_validate_with_subject_key(project_root):
    schema = load_json_schema(
        project_root / "schemas/openlibrary/subjects/subjects.schema.yaml"
    )
    fixtures = (
        project_root / "tests/fixtures/openlibrary/subjects_love.json"
    ).read_text(encoding="utf-8")
    import json

    rows = [
        {**row, "subject_key": "/subjects/love"}
        for row in json.loads(fixtures)
    ]
    validate_records(rows, schema)


def test_unknown_work_field_fails_schema(project_root):
    """Contract drift should be loud — do not silently strip in the source."""
    schema = load_json_schema(
        project_root / "schemas/openlibrary/subjects/subjects.schema.yaml"
    )
    row = {
        "key": "/works/OL1W",
        "subject_key": "/subjects/love",
        "title": "Demo",
        "unknown_field": True,
    }
    with pytest.raises(SchemaValidationError):
        validate_records([row], schema)
