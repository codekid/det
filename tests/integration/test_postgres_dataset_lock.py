"""Live Postgres bronze-dataset lock tests. Skipped unless a postgres DSN is set."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from det.runtime.lake import clear_memory_lakes
from det.runtime.lease import LeaseFencedError, LeaseHeldError
from det.runtime.lease.dataset_lock import assert_dataset_lock_held
from det.runtime.lease.dataset_lock_postgres import PostgresDatasetLockStore

_DSN = (
    os.environ.get("DET_LOCK_PG_DSN") or os.environ.get("DET_POSTGRES_DSN") or ""
).strip()


def _postgres_available() -> bool:
    if not _DSN:
        return False
    try:
        psycopg = pytest.importorskip("psycopg")
        with psycopg.connect(_DSN, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.skipif(not _postgres_available(), reason="postgres not available"),
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
    s = PostgresDatasetLockStore(
        resolve_secret=lambda n: os.environ.get(n),
        dsn_env="DET_LOCK_PG_DSN",
        schema="det_lease_test",
    )
    s.ensure()
    yield s
    import psycopg

    with psycopg.connect(_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM "det_lease_test"."dataset_lock_shared"')
            cur.execute('DELETE FROM "det_lease_test"."dataset_locks"')
        conn.commit()


def test_postgres_shared_acquire_and_release(store) -> None:
    dataset_id = "example_api.events_v1"
    handle = store.acquire_shared(
        dataset_id=dataset_id,
        command="load",
        ttl_sec=120,
        owner="load",
    )
    assert handle.mode == "shared"
    body = store.inspect(dataset_id=dataset_id)
    assert body is not None
    assert len(body.get("shared") or []) == 1
    store.refresh(handle)
    assert_dataset_lock_held(handle)
    store.release(handle)
    assert store.inspect(dataset_id=dataset_id) is None


def test_postgres_exclusive_blocks_shared(store) -> None:
    dataset_id = "example_api.orders_v1"
    exclusive = store.acquire_exclusive(
        dataset_id=dataset_id,
        command="migrate",
        ttl_sec=120,
        owner="migrate",
        wait=False,
    )
    with pytest.raises(LeaseHeldError):
        store.acquire_shared(
            dataset_id=dataset_id,
            command="load",
            ttl_sec=120,
            owner="load",
        )
    store.release(exclusive)


def test_postgres_force_release(store) -> None:
    dataset_id = "example_api.events_v1"
    handle = store.acquire_exclusive(
        dataset_id=dataset_id,
        command="migrate",
        ttl_sec=120,
        owner="migrate",
        wait=False,
    )
    payload = store.force_release(dataset_id=dataset_id)
    assert payload is not None
    assert store.inspect(dataset_id=dataset_id) is None
    store.release(handle)


def test_postgres_ensure_held_fences_lost_token(store) -> None:
    dataset_id = "example_api.events_v1"
    handle = store.acquire_shared(
        dataset_id=dataset_id,
        command="load",
        ttl_sec=120,
        owner="stale",
    )
    import psycopg

    past = datetime.now(UTC) - timedelta(seconds=5)
    with psycopg.connect(_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE "det_lease_test"."dataset_lock_shared"
                   SET expires_at = %s
                 WHERE dataset_id = %s AND token = %s
                """,
                (past, dataset_id, handle.token),
            )
        conn.commit()
    with pytest.raises(LeaseFencedError):
        assert_dataset_lock_held(handle)
