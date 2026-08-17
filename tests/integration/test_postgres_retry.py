"""Live Postgres replace-by-run: a retried extract_run must not duplicate rows."""

from __future__ import annotations

import os
from typing import Any

import pytest

from det.ingestion.postgres_writer import write_postgres_table

pytest.importorskip("psycopg")

_DSN = (os.environ.get("DET_POSTGRES_DSN") or "").strip()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.skipif(not _DSN, reason="DET_POSTGRES_DSN not set"),
]

_SCHEMA = "bronze_ci"
_TABLE = "retry_v1"
_JSON_SCHEMA = {
    "type": "object",
    "properties": {"event_id": {"type": ["integer", "null"]}},
}

_START = "2026-08-06T00:00:00+00:00"
_END = "2026-08-07T00:00:00+00:00"
_RUN_A = "2026-08-06T15:04:05+00:00"
_RUN_B = "2026-08-06T16:00:00+00:00"


def _row(event_id: int, run: str, row_hash: str) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "__row_hash": row_hash,
        "__filename": "page.json",
        "__extract_run_datetime": run,
        "__bronze_loaded_at": run,
        "__interval_start_datetime": _START,
        "__interval_end_datetime": _END,
        "__data_interval_date": "2026-08-06",
    }


def _drop_schema() -> None:
    import psycopg

    with psycopg.connect(_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE')
        conn.commit()


def test_postgres_retry_replaces_same_extract_run_keeps_sibling():
    import psycopg

    kwargs = {
        "dsn": _DSN,
        "schema": _SCHEMA,
        "table": _TABLE,
        "json_schema": _JSON_SCHEMA,
        "chunk_rows": 1,
        "pipeline": "ci.postgres_retry",
    }
    try:
        write_postgres_table([_row(1, _RUN_A, "first")], **kwargs)
        write_postgres_table([_row(2, _RUN_B, "sib")], **kwargs)
        write_postgres_table([_row(99, _RUN_A, "abc-retry")], **kwargs)

        with psycopg.connect(_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT "event_id", "__row_hash" '
                    f'FROM "{_SCHEMA}"."{_TABLE}" '
                    f'ORDER BY "event_id"'
                )
                rows = list(cur.fetchall())
                cur.execute(
                    f'SELECT COUNT(*) FROM "{_SCHEMA}"."{_TABLE}" '
                    f'WHERE "__extract_run_datetime" = %s',
                    (_RUN_A,),
                )
                run_a_count = cur.fetchone()[0]
    finally:
        _drop_schema()

    assert rows == [(2, "sib"), (99, "abc-retry")]
    assert run_a_count == 1
