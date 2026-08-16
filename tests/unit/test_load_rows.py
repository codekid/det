from __future__ import annotations

import pytest

from det.ingestion.chunks import iter_chunks
from det.runtime.load_rows import CountingIter, iter_bronze_rows
from det.runtime.naming import BronzeNamingConfig
from det.sources.base import SourceRow
from det.validation.jsonschema_validator import SchemaValidationError

_SCHEMA = {
    "type": "object",
    "properties": {"event_id": {"type": "integer"}},
    "required": ["event_id"],
    "additionalProperties": False,
}


def test_iter_chunks():
    assert list(iter_chunks([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]
    with pytest.raises(ValueError, match="chunk size"):
        list(iter_chunks([1], 0))


def test_counting_iter():
    counted = CountingIter(i for i in range(4))
    assert list(counted) == [0, 1, 2, 3]
    assert counted.n == 4


def test_iter_bronze_rows_fail_closed_on_second_row():
    rows = [
        SourceRow(data={"event_id": 1}, filename="a.json"),
        SourceRow(data={"event_id": 2, "nope": True}, filename="b.json"),
    ]
    stream = iter_bronze_rows(
        rows,
        schema=_SCHEMA,
        naming=BronzeNamingConfig(style="identity"),
        extract_run_datetime="2026-08-06T15:00:00+00:00",
        interval_start_datetime="2026-08-06T00:00:00+00:00",
        interval_end_datetime="2026-08-07T00:00:00+00:00",
        bronze_loaded_at="2026-08-06T16:00:00+00:00",
    )
    first = next(stream)
    assert first["event_id"] == 1
    assert first["__filename"] == "a.json"
    with pytest.raises(SchemaValidationError):
        next(stream)
