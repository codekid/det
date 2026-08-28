from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import LeaseFencedError, LeaseHeldError
from det.runtime.lease.dataset_lock import (
    LakeDatasetLockStore,
    assert_dataset_lock_held,
    dataset_lock_path,
    dataset_shared_lock,
    force_release_dataset_lock,
    read_dataset_lock,
)
from det.runtime.lease.store import ResolvedLeaseOptions


class _StopAfterPurge(Exception):
    """Test-only: stop migrate after purge ordering is recorded."""


@pytest.fixture(autouse=True)
def _reset_memory():
    clear_memory_lakes()
    yield
    clear_memory_lakes()


def _lake(tmp_path: Path, name: str = "ds-lock"):
    return open_lake(str(tmp_path / name), tmp_path)


def _opts(enabled: bool = True) -> ResolvedLeaseOptions:
    return ResolvedLeaseOptions(enabled=enabled)


def test_parallel_shared_locks(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    store = LakeDatasetLockStore(lake)
    handles: list[object] = []

    def take(i: int) -> None:
        handles.append(
            store.acquire_shared(
                dataset_id=dataset_id,
                command="load",
                ttl_sec=120,
                owner=f"w{i}",
            )
        )

    threads = [threading.Thread(target=take, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    body = read_dataset_lock(dataset_lock_path(lake, dataset_id))
    assert body is not None
    assert len(body.get("shared") or []) == 3
    for h in handles:
        store.release(h)  # type: ignore[arg-type]
    assert read_dataset_lock(dataset_lock_path(lake, dataset_id)) is None


def test_exclusive_blocks_shared_and_waits_for_drain(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    store = LakeDatasetLockStore(lake)
    shared = store.acquire_shared(
        dataset_id=dataset_id,
        command="load",
        ttl_sec=120,
        owner="load",
    )
    acquired: dict[str, object] = {}

    def take_exclusive() -> None:
        acquired["handle"] = store.acquire_exclusive(
            dataset_id=dataset_id,
            command="migrate",
            ttl_sec=120,
            owner="migrate",
            wait=True,
            wait_sec=5,
            poll_interval=0.05,
        )

    t = threading.Thread(target=take_exclusive)
    t.start()
    time.sleep(0.15)
    assert "handle" not in acquired
    store.release(shared)
    t.join(timeout=2)
    assert "handle" in acquired
    ex = read_dataset_lock(dataset_lock_path(lake, dataset_id))
    assert ex is not None
    assert ex.get("exclusive") is not None
    store.release(acquired["handle"])  # type: ignore[arg-type]


def test_exclusive_waits_for_other_exclusive_release(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    store = LakeDatasetLockStore(lake)
    first = store.acquire_exclusive(
        dataset_id=dataset_id,
        command="migrate",
        ttl_sec=120,
        owner="first",
        wait=False,
    )
    acquired: dict[str, object] = {}

    def second() -> None:
        acquired["handle"] = store.acquire_exclusive(
            dataset_id=dataset_id,
            command="migrate",
            ttl_sec=120,
            owner="second",
            wait=True,
            wait_sec=5,
            poll_interval=0.05,
        )

    t = threading.Thread(target=second)
    t.start()
    time.sleep(0.15)
    assert "handle" not in acquired
    store.release(first)
    t.join(timeout=2)
    assert "handle" in acquired
    store.release(acquired["handle"])  # type: ignore[arg-type]


def test_exclusive_fails_fast_when_other_exclusive_held(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    store = LakeDatasetLockStore(lake)
    first = store.acquire_exclusive(
        dataset_id=dataset_id,
        command="migrate",
        ttl_sec=120,
        owner="first",
        wait=False,
    )
    with pytest.raises(LeaseHeldError):
        store.acquire_exclusive(
            dataset_id=dataset_id,
            command="migrate",
            ttl_sec=120,
            owner="second",
            wait=False,
        )
    store.release(first)


def test_shared_blocked_while_exclusive_held(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    store = LakeDatasetLockStore(lake)
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


def test_ensure_held_fences_expired_shared_token(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    path = dataset_lock_path(lake, dataset_id)
    store = LakeDatasetLockStore(lake)
    handle = store.acquire_shared(
        dataset_id=dataset_id,
        command="load",
        ttl_sec=120,
        owner="stale",
    )
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    body = read_dataset_lock(path) or {}
    for row in body.get("shared") or []:
        row["expires_at"] = past
    handle.version = path.replace_if_match(
        handle.version or path.object_version() or "",
        __import__("json").dumps(body, indent=2, sort_keys=True).encode("utf-8"),
    )
    with pytest.raises(LeaseFencedError):
        assert_dataset_lock_held(handle)


def test_dataset_lock_noop_when_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DET_LOCK", "0")
    lake = _lake(tmp_path)
    with dataset_shared_lock(
        lake,
        "example_api.events_v1",
        command="load",
        options=_opts(enabled=False),
    ) as handle:
        assert handle is None
        assert_dataset_lock_held(handle)


def test_recreate_exclusive_before_purge_order(
    tmp_path: Path, project_root: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("pyiceberg")
    from contextlib import contextmanager

    import yaml

    from det.runtime.migrate import BronzeMigrator

    schema_src = project_root / "schemas/example_api/events/events.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {
                    "type": "example_api.events",
                    "overrides": {"fixture_records": [{"id": "e1"}]},
                },
                "schema": "schemas/example_api/events/events.schema.yaml",
                "destination": {
                    "type": "iceberg",
                    "path": str(tmp_path / "lake"),
                    "partition": "extract_run",
                },
                "ingestion": {"library": "thin"},
            }
        ),
        encoding="utf-8",
    )
    from det.runtime.runner import PipelineRunner

    runner = PipelineRunner(tmp_path)
    runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    from det.runtime.lease.dataset_lock import dataset_exclusive_lock as real_exclusive_lock

    order: list[str] = []

    @contextmanager
    def track_exclusive(*args, **kwargs):  # noqa: ANN002, ANN003
        order.append("exclusive_enter")
        with real_exclusive_lock(*args, **kwargs) as handle:
            yield handle

    def track_purge(**kwargs):  # noqa: ANN003
        order.append("purge")
        raise _StopAfterPurge()

    monkeypatch.setattr(
        "det.runtime.migrate.dataset_exclusive_lock",
        track_exclusive,
    )
    monkeypatch.setattr(
        "det.ingestion.iceberg_writer.purge_iceberg_table",
        track_purge,
    )
    with pytest.raises(_StopAfterPurge):
        BronzeMigrator(tmp_path).migrate(
            pipeline=pipe,
            to_bronze="example_api.events_v1",
            schema_path=schema_dst,
            mapper_name="identity",
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            lake_path=str(tmp_path / "lake"),
            recreate_iceberg=True,
        )
    assert order.index("exclusive_enter") < order.index("purge")


def test_migrate_dry_run_reports_dataset_exclusive(project_root: Path, tmp_path: Path):
    pytest.importorskip("pyiceberg")
    import yaml

    from det.runtime.migrate import BronzeMigrator, MigratePlan

    pipe_path = tmp_path / "iceberg.yaml"
    pipe_path.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {
                    "type": "example_api.events",
                    "overrides": {"fixture_records": [{"id": "e1"}]},
                },
                "schema": str(
                    project_root / "schemas/example_api/events/events.schema.yaml"
                ),
                "destination": {
                    "type": "iceberg",
                    "path": str(tmp_path / "lake"),
                    "partition": "extract_run",
                },
            }
        ),
        encoding="utf-8",
    )
    plan = BronzeMigrator(tmp_path).migrate(
        pipeline=pipe_path,
        to_bronze="example_api.events_v1",
        schema_path=project_root / "schemas/example_api/events/events.schema.yaml",
        mapper_name="identity",
        interval_start="2026-08-06",
        lake_path=str(tmp_path / "lake"),
        dry_run=True,
        recreate_iceberg=True,
    )
    assert isinstance(plan, MigratePlan)
    assert plan.will_take_dataset_exclusive is True
    assert plan.recreate_warning is not None
    assert "exclusive bronze-dataset lock" in plan.recreate_warning


def test_acquire_shared_rejects_unreadable_lock_file(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    path = dataset_lock_path(lake, dataset_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not-json", encoding="utf-8")
    store = LakeDatasetLockStore(lake)
    with pytest.raises(LeaseHeldError, match="unreadable"):
        store.acquire_shared(
            dataset_id=dataset_id,
            command="load",
            ttl_sec=120,
            owner="w",
        )


def test_force_release_dataset_lock(tmp_path: Path):
    lake = _lake(tmp_path)
    dataset_id = "example_api.events_v1"
    store = LakeDatasetLockStore(lake)
    handle = store.acquire_exclusive(
        dataset_id=dataset_id,
        command="migrate",
        ttl_sec=120,
        owner="migrate",
        wait=False,
    )
    store.release(handle)
    handle = store.acquire_shared(
        dataset_id=dataset_id,
        command="load",
        ttl_sec=120,
        owner="load",
    )
    payload = force_release_dataset_lock(
        lake,
        dataset_id,
        options=_opts(),
    )
    assert payload is not None
    assert read_dataset_lock(dataset_lock_path(lake, dataset_id)) is None
