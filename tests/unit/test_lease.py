from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import (
    DEFAULT_LOCK_TTL_SEC,
    LeaseHeldError,
    advisory_lock_keys,
    force_release_lock,
    lock_path,
    pipeline_lease,
    read_lock,
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
