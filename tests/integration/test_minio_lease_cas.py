"""MinIO CAS soak for lake leases. Skipped unless AWS_ENDPOINT_URL is set."""

from __future__ import annotations

import json
import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.lake import ENV_LAKE_MODE, open_lake
from det.runtime.lease import (
    LeaseHeldError,
    acquire_lease,
    lock_path,
    pipeline_lease,
    read_lock,
    refresh_lease,
    release_lease,
)
from det.runtime.meta import resolve_interval

_ENDPOINT = (os.environ.get("AWS_ENDPOINT_URL") or "").strip()
_BUCKET = (os.environ.get("DET_MINIO_BUCKET") or "det-ci").strip()
_LAKE_URI = f"s3://{_BUCKET}/det-lease-cas"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.minio,
    pytest.mark.skipif(not _ENDPOINT, reason="AWS_ENDPOINT_URL not set"),
]


def _ensure_bucket() -> None:
    pytest.importorskip("s3fs")
    import fsspec

    from det.runtime.object_store import fsspec_s3_kwargs

    fs = fsspec.filesystem("s3", **fsspec_s3_kwargs())
    if not fs.exists(_BUCKET):
        fs.mkdir(_BUCKET)


def test_minio_exclusive_create_and_cas_steal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("s3fs")
    _ensure_bucket()
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("AWS_ENDPOINT_URL", _ENDPOINT)
    lake = open_lake(f"{_LAKE_URI}/{tmp_path.name}", tmp_path)
    start, end = resolve_interval("2026-08-15", None)

    winners: list[str] = []
    errors: list[BaseException] = []

    def worker(name: str) -> None:
        try:
            lease = acquire_lease(
                lake,
                pipeline="example_api.events",
                interval_start=start,
                interval_end=end,
                command="extract",
                owner=name,
                ttl_sec=120,
            )
            assert lease is not None
            winners.append(name)
        except LeaseHeldError as exc:
            errors.append(exc)

    # Seed an expired lock so both race on steal.
    path = lock_path(lake, "example_api.events", start, end)
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    version = path.create_exclusive(
        json.dumps(
            {
                "pipeline": "example_api.events",
                "token": "old",
                "owner": "dead",
                "expires_at": past,
                "ttl_sec": 1,
                "interval_start": start,
                "interval_end": end,
                "command": "extract",
            }
        ).encode("utf-8")
    )
    assert version

    t1 = threading.Thread(target=worker, args=("w1",))
    t2 = threading.Thread(target=worker, args=("w2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert len(winners) == 1
    assert len(errors) == 1
    held = read_lock(path)
    assert held is not None
    assert held["token"] != "old"
    # Cleanup
    path.unlink(missing_ok=True)


def test_minio_refresh_release_after_foreign_steal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("s3fs")
    _ensure_bucket()
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    monkeypatch.setenv("AWS_ENDPOINT_URL", _ENDPOINT)
    lake = open_lake(f"{_LAKE_URI}/{tmp_path.name}-rr", tmp_path)
    start, end = resolve_interval("2026-08-16", None)

    with pipeline_lease(
        lake,
        pipeline="example_api.events",
        interval_start=start,
        interval_end=end,
        command="extract",
        owner="first",
        ttl_sec=1,
    ) as first:
        assert first is not None
        path = first.path
        assert path is not None
        # Expire and steal as second holder.
        past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
        body = read_lock(path) or {}
        body["expires_at"] = past
        path.replace_if_match(
            first.version or path.object_version() or "",
            json.dumps(body, indent=2, sort_keys=True).encode("utf-8"),
        )
        second = acquire_lease(
            lake,
            pipeline="example_api.events",
            interval_start=start,
            interval_end=end,
            command="load",
            owner="second",
            ttl_sec=120,
        )
        assert second is not None
        # First holder's refresh/release must not clobber second.
        refresh_lease(first)
        release_lease(first)
        held = read_lock(path)
        assert held is not None
        assert held["token"] == second.token
        assert held["owner"] == "second"
        release_lease(second)
