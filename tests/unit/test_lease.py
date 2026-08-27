from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import (
    DEFAULT_LOCK_TTL_SEC,
    LeaseFencedError,
    LeaseHeldError,
    acquire_lease,
    advisory_lock_keys,
    assert_lease_held,
    force_release_lock,
    lock_path,
    pipeline_lease,
    read_lock,
    refresh_lease,
    resolve_lock_ttl_sec,
)
from det.runtime.meta import resolve_interval


@pytest.fixture(autouse=True)
def _reset_memory():
    clear_memory_lakes()
    yield
    clear_memory_lakes()


def _lake(tmp_name: str = "t"):
    return open_lake(f"memory://{tmp_name}", Path("/tmp"))


def test_resolve_lock_ttl_order():
    assert resolve_lock_ttl_sec(None, env={}) == DEFAULT_LOCK_TTL_SEC
    assert resolve_lock_ttl_sec(None, env={"DET_LOCK_TTL_SEC": "90"}) == 90
    assert resolve_lock_ttl_sec(30, env={"DET_LOCK_TTL_SEC": "90"}) == 30
    with pytest.raises(ValueError):
        resolve_lock_ttl_sec(0)


def test_second_acquire_fails():
    """A second process (empty ContextVar) must not steal a live lease."""
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    caught: list[BaseException] = []

    def other() -> None:
        try:
            with pipeline_lease(
                lake,
                pipeline="noaa.storm_events",
                interval_start=start,
                interval_end=end,
                command="load",
            ):
                pass
        except LeaseHeldError as exc:
            caught.append(exc)

    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
        command="extract",
    ):
        t = threading.Thread(target=other)
        t.start()
        t.join()
    assert caught
    assert "lock-release" in str(caught[0])


def test_different_intervals_do_not_contend():
    lake = _lake()
    a0, a1 = resolve_interval("2026-08-15", None)
    b0, b1 = resolve_interval("2026-08-16", None)
    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=a0, interval_end=a1,
        command="extract",
    ):
        with pipeline_lease(
            lake, pipeline="noaa.storm_events", interval_start=b0, interval_end=b1,
            command="extract",
        ):
            pass


def test_nested_same_interval_is_noop():
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    with pipeline_lease(
        lake, pipeline="example_api.events", interval_start=start, interval_end=end,
        command="run",
    ):
        with pipeline_lease(
            lake, pipeline="example_api.events", interval_start=start, interval_end=end,
            command="extract",
        ):
            pass


def test_release_then_reacquire():
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    with pipeline_lease(
        lake, pipeline="example_api.events", interval_start=start, interval_end=end,
        command="extract",
    ):
        pass
    with pipeline_lease(
        lake, pipeline="example_api.events", interval_start=start, interval_end=end,
        command="extract",
    ):
        pass


def test_expired_steal(monkeypatch: pytest.MonkeyPatch):
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    path = lock_path(lake, "noaa.storm_events", start, end)
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "pipeline": "noaa.storm_events",
                "token": "old",
                "owner": "dead",
                "expires_at": past,
            }
        )
    )
    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
        command="extract",
    ) as lease:
        assert lease is not None
        held = read_lock(path)
        assert held is not None
        assert held["token"] != "old"
        assert held["owner"] != "dead"


def test_live_lease_not_stolen():
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    caught: list[BaseException] = []

    def other() -> None:
        try:
            with pipeline_lease(
                lake,
                pipeline="noaa.storm_events",
                interval_start=start,
                interval_end=end,
                command="load",
            ):
                pass
        except LeaseHeldError as exc:
            caught.append(exc)

    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
        command="extract", ttl_sec=3600,
    ):
        t = threading.Thread(target=other)
        t.start()
        t.join()
    assert caught


def test_det_lock_disabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_LOCK", "0")
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
        command="extract",
    ) as a:
        with pipeline_lease(
            lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
            command="load",
        ) as b:
            assert a is None
            assert b is None


def test_force_release_removes_live_lease():
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    path = lock_path(lake, "noaa.storm_events", start, end)
    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
        command="extract",
    ):
        payload = force_release_lock(path)
        assert payload is not None
        assert read_lock(path) is None


def test_per_acquire_ttl():
    lake = _lake()
    start, end = resolve_interval("2026-08-15", None)
    with pipeline_lease(
        lake, pipeline="noaa.storm_events", interval_start=start, interval_end=end,
        command="extract", ttl_sec=90,
    ) as lease:
        assert lease is not None
        held = read_lock(lease.path)
        assert held is not None
        assert held["ttl_sec"] == 90


def test_advisory_lock_keys_stable():
    a = advisory_lock_keys(
        "noaa.storm_events",
        "2026-08-15T00:00:00+00:00",
        "2026-08-16T00:00:00+00:00",
    )
    b = advisory_lock_keys(
        "noaa.storm_events",
        "2026-08-15T00:00:00+00:00",
        "2026-08-16T00:00:00+00:00",
    )
    assert a == b
    assert a[0] != advisory_lock_keys(
        "noaa.fatalities",
        "2026-08-15T00:00:00+00:00",
        "2026-08-16T00:00:00+00:00",
    )[0]


def test_refresh_lease_uses_bound_store_when_path_missing():
    from det.runtime.lease import Lease, refresh_lease

    calls: list[object] = []

    class _Store:
        def refresh(self, lease: object) -> None:
            calls.append(lease)

    lease = Lease(
        token="t",
        pipeline="p",
        interval_start="2026-08-15T00:00:00+00:00",
        interval_end="2026-08-16T00:00:00+00:00",
        ttl_sec=60,
        lock_id="p/x",
        path=None,
        store=_Store(),
    )
    refresh_lease(lease)
    assert calls == [lease]
    refresh_lease(lease, store=lease.store)
    assert len(calls) == 2


def test_read_lock_returns_none_on_undecodable_utf8(tmp_path: Path):
    from det.runtime.lease import lock_path, read_lock

    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    start, end = resolve_interval("2026-08-15", None)
    path = lock_path(lake, "example_api.events", start, end)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xff\xfe not utf-8")
    assert read_lock(path) is None


def test_ensure_held_ok_and_fenced_after_steal(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    start, end = resolve_interval("2026-08-15", None)
    first = acquire_lease(
        lake,
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="extract",
        owner="a",
        ttl_sec=1,
    )
    assert first is not None
    assert_lease_held(first)
    # Expire and steal as another owner (empty ContextVar via thread).
    path = first.path
    assert path is not None
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    body = read_lock(path) or {}
    body["expires_at"] = past
    first.version = path.replace_if_match(
        first.version or path.object_version() or "",
        json.dumps(body, indent=2, sort_keys=True).encode("utf-8"),
    )
    stolen: dict[str, object] = {}

    def steal() -> None:
        stolen["lease"] = acquire_lease(
            lake,
            pipeline="example_api.events",
            interval_start=start,
            interval_end=end,
            command="load",
            owner="b",
            ttl_sec=120,
        )

    t = threading.Thread(target=steal)
    t.start()
    t.join()
    second = stolen["lease"]
    assert second is not None
    refresh_lease(first)  # soft no-op
    with pytest.raises(LeaseFencedError):
        assert_lease_held(first)
    assert_lease_held(second)  # type: ignore[arg-type]
    from det.runtime.lease import release_lease

    release_lease(second)  # type: ignore[arg-type]


def test_ensure_held_fenced_after_ttl_expiry_same_token(tmp_path: Path):
    """Expired lease with unchanged token must not renew via fence."""
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    start, end = resolve_interval("2026-08-15", None)
    lease = acquire_lease(
        lake,
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="extract",
        owner="stale",
        ttl_sec=120,
    )
    assert lease is not None
    path = lease.path
    assert path is not None
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    body = read_lock(path) or {}
    body["expires_at"] = past
    lease.version = path.replace_if_match(
        lease.version or path.object_version() or "",
        json.dumps(body, indent=2, sort_keys=True).encode("utf-8"),
    )
    token_before = lease.token
    with pytest.raises(LeaseFencedError):
        assert_lease_held(lease)
    held = read_lock(path)
    assert held is not None
    assert held.get("token") == token_before
    assert held.get("expires_at") == past
    from det.runtime.lease import release_lease

    release_lease(lease)


def test_assert_lease_held_noop_when_disabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_LOCK", "0")
    lake = _lake("fence-off")
    start, end = resolve_interval("2026-08-15", None)
    with pipeline_lease(
        lake,
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="extract",
    ) as lease:
        assert lease is None
        assert_lease_held(lease)


def test_extract_fence_preserves_raw_dir(
    tmp_path: Path, project_root: Path, monkeypatch: pytest.MonkeyPatch
):
    import yaml

    from det.runtime.manifest import is_committed_raw_dir
    from det.runtime.runner import PipelineRunner

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
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
            }
        ),
        encoding="utf-8",
    )
    runner = PipelineRunner(tmp_path)

    def boom(lease, *, store=None):  # noqa: ANN001
        raise LeaseFencedError("injected fence")

    monkeypatch.setattr("det.runtime.runner.assert_lease_held", boom)
    with pytest.raises(LeaseFencedError, match="injected fence"):
        runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    raw_root = tmp_path / "lake" / "raw"
    data_dirs = list(raw_root.rglob("data"))
    assert len(data_dirs) == 1
    raw_dir = data_dirs[0].parent
    assert list(raw_dir.rglob("*"))  # extract bytes retained
    assert not is_committed_raw_dir(raw_dir)
    assert list(raw_root.rglob("manifest.json")) == []


def test_runner_fence_blocks_write_after_steal(
    tmp_path: Path, project_root: Path, monkeypatch: pytest.MonkeyPatch
):
    import yaml

    from det.runtime.runner import PipelineRunner

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
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
            }
        ),
        encoding="utf-8",
    )
    runner = PipelineRunner(tmp_path)
    writes: list[str] = []

    def tracking_write(self, records, **kwargs):  # noqa: ANN001
        writes.append("write")
        from det.ingestion.jsonl import write_jsonl_partition

        return write_jsonl_partition(
            records, kwargs["partition_dir"], chunk_rows=kwargs.get("chunk_rows", 1000)
        )

    monkeypatch.setattr(
        "det.ingestion.det_backend.DetBackend.write",
        tracking_write,
    )

    extracted = runner.extract(
        pipe, interval_start="2026-08-06", interval_end="2026-08-07"
    )

    def boom(lease, *, store=None):  # noqa: ANN001
        raise LeaseFencedError("injected fence")

    monkeypatch.setattr("det.runtime.runner.assert_lease_held", boom)
    with pytest.raises(LeaseFencedError, match="injected fence"):
        runner.load(
            pipe,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            extract_run_datetime=extracted.extract_run_datetime,
        )
    assert writes == []


def test_local_exclusive_create(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    target = lake / "locks" / "p" / "x.json"
    target.create_exclusive(b"one")
    with pytest.raises(FileExistsError):
        target.create_exclusive(b"two")
    assert target.read_bytes() == b"one"


def test_runner_second_extract_blocked(
    tmp_path: Path, project_root: Path, monkeypatch: pytest.MonkeyPatch
):
    import threading
    import time

    import yaml

    from det.runtime.runner import PipelineRunner

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
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
            }
        ),
        encoding="utf-8",
    )
    runner = PipelineRunner(tmp_path)
    held = {"inside": False}

    def blocking_extract(self, **kwargs):
        held["inside"] = True
        time.sleep(0.25)
        return []

    monkeypatch.setattr(
        "det.sources.example_api.events.ExampleApiSource.extract_to_raw",
        blocking_extract,
    )
    err: list[BaseException] = []

    def other():
        while not held["inside"]:
            time.sleep(0.01)
        try:
            runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
        except Exception as exc:  # noqa: BLE001
            err.append(exc)

    t = threading.Thread(target=other)
    t.start()
    runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")
    t.join(timeout=5)
    assert any(isinstance(e, LeaseHeldError) for e in err)


def test_cli_lock_release_requires_force():
    import typer

    from det.cli import lock_release

    with pytest.raises(typer.BadParameter, match="--force"):
        lock_release(
            ctx=None,
            pipeline="noaa.storm_events",
            interval_start="2026-08-15",
            interval_end=None,
            force=False,
            lake_path=None,
            project_root=None,
        )
