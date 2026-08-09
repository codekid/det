from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from det.destinations.models import hive_partition_dir
from det.runtime.meta import (
    attach_meta,
    data_interval_date,
    format_extract_run_datetime,
    resolve_interval,
    row_hash,
    to_interval_datetime,
    to_partition_value,
)


def test_format_extract_run_datetime_is_iso_utc():
    dt = datetime(2026, 8, 6, 15, 4, 5, 123456, tzinfo=UTC)
    assert format_extract_run_datetime(dt) == "2026-08-06T15:04:05.123456+00:00"


def test_extract_run_partition_matches_column_instant():
    """Path and column are two encodings of the same run-start instant."""
    stamp = "2026-08-06T15:04:05.123456+00:00"
    assert to_partition_value(stamp) == "20260806T150405Z"


def test_data_interval_date():
    assert data_interval_date("2026-08-06T00:00:00+00:00") == "2026-08-06"


def test_attach_meta_adds_prefixed_fields():
    canonical = {"event_id": "1", "begin_day": "1", "begin_time": "0"}
    out = attach_meta(
        canonical,
        filename="file.csv",
        extract_run_datetime="2026-08-06T15:04:05.123456+00:00",
        interval_start_datetime="2026-08-06",
        interval_end_datetime="2026-08-07",
    )
    assert out["__extract_run_datetime"] == "2026-08-06T15:04:05.123456+00:00"
    assert out["event_id"] == "1"
    assert "__raw" not in out
    assert out["__filename"] == "file.csv"
    assert out["__data_interval_date"] == "2026-08-06"
    assert out["__row_hash"] == row_hash(canonical)
    # Bare dates are normalized to ISO 8601 UTC on the landed row.
    assert out["__interval_start_datetime"] == "2026-08-06T00:00:00+00:00"
    assert out["__interval_end_datetime"] == "2026-08-07T00:00:00+00:00"


def test_to_interval_datetime_normalizes_to_utc():
    assert to_interval_datetime("2026-08-06") == "2026-08-06T00:00:00+00:00"
    assert to_interval_datetime("2026-08-06T20:00:00-05:00") == "2026-08-07T01:00:00+00:00"


def test_resolve_interval_defaults_end_to_next_day():
    assert resolve_interval("2026-08-06") == (
        "2026-08-06T00:00:00+00:00",
        "2026-08-07T00:00:00+00:00",
    )


def test_resolve_interval_rejects_end_before_start():
    with pytest.raises(ValueError):
        resolve_interval("2026-08-06", "2026-08-05")


def test_partition_value_is_path_safe():
    """Hive directory names cannot hold the '/' and ':' the meta columns use."""
    for raw, expected in (
        ("2026-08-01T00:00:00+00:00", "20260801T000000Z"),
        ("2026-08-06T19:30:00-05:00", "20260807T003000Z"),
        ("2026-08-06T23:13:44.200000+00:00", "20260806T231344Z"),
        ("2026/08/06 23:13:44.2", "20260806T231344"),  # legacy extract-run format
    ):
        value = to_partition_value(raw)
        assert value == expected
        assert "/" not in value and ":" not in value


def test_partition_dir_nests_interval_keys_without_day():
    path = hive_partition_dir(
        Path("/lake/bronze/noaa/storm_events"),
        interval_start_datetime="2026-08-01T00:00:00+00:00",
        interval_end_datetime="2026-08-02T00:00:00+00:00",
        extract_run_datetime="2026-08-06T23:13:44.200000+00:00",
    )
    assert path.parts[-3:] == (
        "__interval_start_datetime=20260801T000000Z",
        "__interval_end_datetime=20260802T000000Z",
        "__extract_run_datetime=20260806T231344Z",
    )
    assert not any(p.startswith("__data_interval_date=") for p in path.parts)
