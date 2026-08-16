from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from det.ingestion.sql_ddl import ensure_bronze_table


class _FakeDb:
    def __init__(self) -> None:
        self.live: list[tuple[str, str]] = []
        self.sql: list[str] = []

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self.sql.append(sql)
        if sql.startswith("CREATE TABLE"):
            # Columns already decided by caller; mark table present via fetchall.
            self.live = [("event_id", "INTEGER")]
        elif "ADD COLUMN" in sql:
            self.live.append(("state", "TEXT"))

    def fetchall(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        if "information_schema.columns" in sql:
            return list(self.live)
        return []


def test_ensure_creates_then_alters_then_refuses():
    db = _FakeDb()
    cols = [("event_id", "INTEGER"), ("__row_hash", "TEXT")]
    ensure_bronze_table(
        sql_schema="bronze_noaa",
        table="t",
        columns=cols,
        dialect="postgres",
        execute=db.execute,
        fetchall=db.fetchall,
    )
    assert any(s.startswith("CREATE TABLE") for s in db.sql)

    db.sql.clear()
    db.live = [("event_id", "INTEGER"), ("__row_hash", "TEXT")]
    ensure_bronze_table(
        sql_schema="bronze_noaa",
        table="t",
        columns=[*cols, ("state", "TEXT")],
        dialect="postgres",
        execute=db.execute,
        fetchall=db.fetchall,
    )
    assert any("ADD COLUMN" in s and '"state"' in s for s in db.sql)

    db.live = [("event_id", "VARCHAR"), ("__row_hash", "TEXT")]
    with pytest.raises(ValueError, match="has type VARCHAR, expected INTEGER"):
        ensure_bronze_table(
            sql_schema="bronze_noaa",
            table="t",
            columns=cols,
            dialect="postgres",
            execute=db.execute,
            fetchall=db.fetchall,
        )
