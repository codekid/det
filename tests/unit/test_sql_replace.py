from __future__ import annotations

import pytest

from det.ingestion.sql_replace import (
    bronze_run_identity,
    delete_extract_run_sql,
    require_bronze_run_identity,
    resolve_run_identity,
)


def test_bronze_run_identity_from_first_row():
    records = [
        {
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
            "__extract_run_datetime": "2026-08-06T15:04:05+00:00",
        }
    ]
    assert bronze_run_identity(records) == (
        "2026-08-06T00:00:00+00:00",
        "2026-08-07T00:00:00+00:00",
        "2026-08-06T15:04:05+00:00",
    )


def test_bronze_run_identity_empty():
    assert bronze_run_identity([]) is None


def test_resolve_run_identity_prefers_threaded():
    threaded = (
        "2026-08-06T00:00:00+00:00",
        "2026-08-07T00:00:00+00:00",
        "2026-08-06T15:04:05+00:00",
    )
    assert resolve_run_identity(threaded, None) == threaded
    assert resolve_run_identity(threaded, [{"id": 1}]) == threaded


def test_resolve_run_identity_empty_requires_thread():
    with pytest.raises(ValueError, match="empty bronze write requires run_identity"):
        resolve_run_identity(None, None)


def test_require_rejects_missing_and_mixed():
    with pytest.raises(ValueError, match="replace-by-run requires"):
        require_bronze_run_identity([{"id": 1}])
    with pytest.raises(ValueError, match="does not match the batch"):
        require_bronze_run_identity(
            [
                {
                    "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
                    "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
                    "__extract_run_datetime": "2026-08-06T10:00:00+00:00",
                },
                {
                    "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
                    "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
                    "__extract_run_datetime": "2026-08-06T11:00:00+00:00",
                },
            ]
        )


def test_delete_sql_matches_prune_casts():
    sql = delete_extract_run_sql(
        '"bronze_noaa"."storm_events_v1"', placeholder="?"
    )
    assert "where __interval_start_datetime = ?" in sql
    assert "and __extract_run_datetime = ?" in sql
