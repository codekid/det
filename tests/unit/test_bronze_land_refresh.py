from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from det.runtime.bronze_land import BronzeLandParams, land_bronze_partition
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import Lease, LeaseFencedError, refresh_bronze_locks
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
        ttl_sec=4,
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


def test_land_bronze_fences_lease_before_validation_stamp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Stale worker must not publish raw validation after the final bronze chunk."""
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(json.dumps(_SCHEMA), encoding="utf-8")
    raw_dir = tmp_path / "raw" / "run"
    raw_dir.mkdir(parents=True)
    manifest = {
        "source": "test.source",
        "interval_start": "2026-08-06T00:00:00+00:00",
        "interval_end": "2026-08-07T00:00:00+00:00",
        "extract_run_datetime": "2026-08-06T15:00:00+00:00",
        "wire_version": 1,
    }
    (raw_dir / "meta").mkdir()
    (raw_dir / "meta" / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    config = PipelineConfig(
        name="example_api.events",
        source=SourceConfig(type="example_api.events"),
        schema_path=str(schema_path),
        ingestion=IngestionConfig(library="det", chunk_rows=1000),
        destination=DestinationConfig(type="filesystem", path=str(tmp_path / "lake")),
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
    )

    class _Source:
        name = "test.source"

        def records_from_raw(self, *, config, raw_dir, manifest):  # noqa: ANN001
            yield SourceRow(data={"event_id": 1}, filename="a.json")

    class _Backend:
        def write(self, records, **kwargs):  # noqa: ANN001
            partition = kwargs["partition_dir"]
            partition.mkdir(parents=True, exist_ok=True)
            list(records)
            return partition

    fence_calls = 0
    stamped: list[bool] = []

    def fence_after_write(lease, *, store=None):  # noqa: ANN001
        nonlocal fence_calls
        fence_calls += 1
        if fence_calls > 1:
            raise LeaseFencedError("stale worker after final chunk")

    monkeypatch.setattr("det.runtime.bronze_land.assert_lease_held", fence_after_write)
    monkeypatch.setattr(
        "det.runtime.bronze_land.stamp_validation_success",
        lambda *args, **kwargs: stamped.append(True),
    )

    with pytest.raises(LeaseFencedError, match="stale worker after final chunk"):
        land_bronze_partition(
            source=_Source(),
            backend=_Backend(),
            project_root=tmp_path,
            params=BronzeLandParams(
                raw_dir=raw_dir,
                manifest=manifest,
                effective_config={},
                bronze_config=config,
                schema=_SCHEMA,
                schema_path=str(schema_path),
                schema_resolved=schema_path,
                extract_ts="2026-08-06T15:00:00+00:00",
                start_iso="2026-08-06T00:00:00+00:00",
                end_iso="2026-08-07T00:00:00+00:00",
            ),
            pipeline_lease=Lease(
                token="lease-token",
                pipeline="example_api.events",
                interval_start="2026-08-06T00:00:00+00:00",
                interval_end="2026-08-07T00:00:00+00:00",
                ttl_sec=60,
                lock_id="example_api.events/x",
            ),
            dataset_lock=None,
        )

    assert fence_calls == 2
    assert stamped == []
