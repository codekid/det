from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import Lease, refresh_bronze_locks
from det.runtime.lease.dataset_lock import (
    DatasetLockHandle,
    LakeDatasetLockStore,
    assert_dataset_lock_held,
    refresh_dataset_lock,
)
from det.runtime.lease.lake_store import LakeLeaseStore, read_lock
from det.runtime.load_rows import iter_bronze_rows
from det.runtime.meta import resolve_interval
from det.runtime.naming import BronzeNamingConfig
from det.sources.base import SourceRow

_SCHEMA = {
    "type": "object",
    "properties": {"event_id": {"type": "integer"}},
    "required": ["event_id"],
    "additionalProperties": False,
}


@pytest.fixture(autouse=True)
def _reset_memory():
    clear_memory_lakes()
    yield
    clear_memory_lakes()


def test_iter_bronze_rows_on_progress_cadence():
    calls: list[int] = []
    rows = [
        SourceRow(data={"event_id": i}, filename="a.json") for i in range(25_001)
    ]
    list(
        iter_bronze_rows(
            rows,
            schema=_SCHEMA,
            naming=BronzeNamingConfig(style="identity"),
            extract_run_datetime="2026-08-06T15:00:00+00:00",
            interval_start_datetime="2026-08-06T00:00:00+00:00",
            interval_end_datetime="2026-08-07T00:00:00+00:00",
            bronze_loaded_at="2026-08-06T16:00:00+00:00",
            log_every=10_000,
            on_progress=calls.append,
        )
    )
    assert calls == [1, 10_000, 20_000]


def test_refresh_bronze_locks_invokes_both_refreshers(monkeypatch):
    seen: list[str] = []

    monkeypatch.setattr("det.runtime.lease.refresh_lease", lambda *_a, **_k: seen.append("lease"))
    monkeypatch.setattr(
        "det.runtime.lease.refresh_dataset_lock",
        lambda *_a, **_k: seen.append("dataset"),
    )

    refresh_bronze_locks(
        pipeline_lease=Lease(
            token="t",
            pipeline="p",
            interval_start="2026-08-06T00:00:00+00:00",
            interval_end="2026-08-07T00:00:00+00:00",
            ttl_sec=60,
            lock_id="p/x",
        ),
        dataset_lock=DatasetLockHandle(
            token="d",
            dataset_id="example_api.events_v1",
            mode="shared",
            ttl_sec=60,
            command="load",
            owner="w",
        ),
    )
    assert seen == ["lease", "dataset"]


def test_refresh_extends_dataset_lock_before_expiry(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    store = LakeDatasetLockStore(lake)
    handle = store.acquire_shared(
        dataset_id="example_api.events_v1",
        command="load",
        ttl_sec=2,
        owner="worker",
    )
    time.sleep(1)
    refresh_dataset_lock(handle, store=store)
    time.sleep(1.5)
    assert_dataset_lock_held(handle, store=store)
    store.release(handle)


def test_lake_refresh_logs_on_cas_conflict(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    start, end = resolve_interval("2026-08-15", None)
    store = LakeLeaseStore(lake)
    lease = store.acquire(
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="load",
        ttl_sec=120,
        owner="a",
    )
    assert lease.path is not None
    assert lease.version is not None

    body = read_lock(lease.path) or {}
    body["owner"] = "other"
    lease.path.write_bytes(json.dumps(body, indent=2, sort_keys=True).encode("utf-8"))

    with capture_logs() as logs:
        store.refresh(lease)
    assert any(
        e.get("event") == "lake lease refresh CAS conflict" for e in logs
    )


def test_dataset_lock_refresh_logs_on_cas_conflict(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    store = LakeDatasetLockStore(lake)
    handle = store.acquire_shared(
        dataset_id="example_api.events_v1",
        command="load",
        ttl_sec=120,
        owner="a",
    )
    path = handle.path
    assert path is not None
    body = json.loads(path.read_text(encoding="utf-8"))
    body["shared"][0]["owner"] = "other"
    path.write_bytes(json.dumps(body, indent=2, sort_keys=True).encode("utf-8"))

    with capture_logs() as logs:
        refresh_dataset_lock(handle, store=store)
    assert any(
        e.get("event") == "dataset lock refresh CAS conflict" for e in logs
    )
