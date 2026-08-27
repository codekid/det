"""Live Postgres lease store tests. Skipped unless DET_LOCK_PG_DSN is set."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import (
    LeaseHeldError,
    ResolvedLeaseOptions,
    open_lease_store,
)
from det.runtime.meta import resolve_interval

_DSN = (os.environ.get("DET_LOCK_PG_DSN") or "").strip()

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.skipif(not _DSN, reason="DET_LOCK_PG_DSN not set"),
]


@pytest.fixture(autouse=True)
def _reset_memory():
    clear_memory_lakes()
    yield
    clear_memory_lakes()


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("psycopg")
    monkeypatch.setenv("DET_LOCK_PG_DSN", _DSN)
    lake = open_lake("memory://pg-lease", Path("/tmp"))
    options = ResolvedLeaseOptions(
        backend="postgres",
        mode="exact",
        pg_dsn_env="DET_LOCK_PG_DSN",
        pg_schema="det_lease_test",
        pg_table="leases",
    )
    s = open_lease_store(lake, options, resolve_secret=lambda n: os.environ.get(n))
    s.ensure()  # type: ignore[attr-defined]
    yield s
    # cleanup table rows for this suite
    import psycopg

    with psycopg.connect(_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM "det_lease_test"."leases"')
        conn.commit()


def test_postgres_acquire_conflict(store) -> None:
    start, end = resolve_interval("2026-08-15", None)
    a = store.acquire(
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="extract",
        ttl_sec=120,
        owner="a",
    )
    with pytest.raises(LeaseHeldError):
        store.acquire(
            pipeline="example_api.events",
            interval_start=start,
            interval_end=end,
            command="load",
            ttl_sec=120,
            owner="b",
        )
    store.release(a)


def test_postgres_expire_steal_and_token_mismatch(store) -> None:
    start, end = resolve_interval("2026-08-16", None)
    a = store.acquire(
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="extract",
        ttl_sec=1,
        owner="a",
    )
    # Force expiry
    import psycopg

    past = datetime.now(UTC) - timedelta(seconds=10)
    with psycopg.connect(_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                'UPDATE "det_lease_test"."leases" SET expires_at = %s '
                "WHERE token = %s",
                (past, a.token),
            )
        conn.commit()
    b = store.acquire(
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="load",
        ttl_sec=120,
        owner="b",
    )
    assert b.token != a.token
    store.refresh(a)  # should no-op (token mismatch)
    store.release(a)  # should no-op
    held = store.inspect(
        pipeline="example_api.events", interval_start=start, interval_end=end
    )
    assert held is not None
    assert held["token"] == b.token
    store.release(b)


@pytest.fixture
def overlap_store(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("psycopg")
    monkeypatch.setenv("DET_LOCK_PG_DSN", _DSN)
    lake = open_lake("memory://pg-overlap", Path("/tmp"))
    options = ResolvedLeaseOptions(
        backend="postgres",
        mode="overlap",
        pg_dsn_env="DET_LOCK_PG_DSN",
        pg_schema="det_lease_test",
        pg_table="leases_overlap",
    )
    s = open_lease_store(lake, options, resolve_secret=lambda n: os.environ.get(n))
    s.ensure()  # type: ignore[attr-defined]
    yield s
    import psycopg

    with psycopg.connect(_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM "det_lease_test"."leases_overlap"')
        conn.commit()


def test_postgres_overlap_blocks_intersecting(overlap_store) -> None:
    store = overlap_store
    a0, a1 = resolve_interval("2026-08-15", "2026-08-17")
    b0, b1 = resolve_interval("2026-08-16", "2026-08-18")
    c0, c1 = resolve_interval("2026-08-17", "2026-08-18")  # adjacent to [15,17) at 17
    a = store.acquire(
        pipeline="example_api.events",
        interval_start=a0,
        interval_end=a1,
        command="extract",
        ttl_sec=120,
        owner="a",
    )
    with pytest.raises(LeaseHeldError):
        store.acquire(
            pipeline="example_api.events",
            interval_start=b0,
            interval_end=b1,
            command="extract",
            ttl_sec=120,
            owner="b",
        )
    # Adjacent [17,18) does not overlap [15,17)
    c = store.acquire(
        pipeline="example_api.events",
        interval_start=c0,
        interval_end=c1,
        command="extract",
        ttl_sec=120,
        owner="c",
    )
    store.release(a)
    store.release(c)
