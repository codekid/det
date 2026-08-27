from __future__ import annotations

import gzip
from pathlib import Path

import pytest
import requests
import yaml

from det.errors import DetPluginError
from det.runtime.lake import (
    DEFAULT_LAKE_REL,
    ENV_LAKE_MODE,
    LakeRef,
    clear_memory_lakes,
    is_lake_uri,
    lake_mode_from_env,
    open_lake,
    pick_lake_spec,
    reset_lake_mode_warning_for_tests,
    validate_lake_mode,
)
from det.runtime.manifest import is_committed_raw_dir, read_manifest, write_manifest
from det.runtime.runner import PipelineRunner
from det.sources.example_api.events import ExampleApiSource
from det.sources.http import http_get_file


@pytest.fixture(autouse=True)
def _reset_memory(monkeypatch: pytest.MonkeyPatch):
    clear_memory_lakes()
    reset_lake_mode_warning_for_tests()
    monkeypatch.delenv(ENV_LAKE_MODE, raising=False)
    yield
    clear_memory_lakes()
    reset_lake_mode_warning_for_tests()


def test_pick_lake_spec_order():
    env = {"DET_LAKE_PATH": "s3://from-env"}
    assert (
        pick_lake_spec(
            cli_lake_path="s3://from-cli",
            destination_path="./yaml-lake",
            env=env,
        )
        == "s3://from-cli"
    )
    assert (
        pick_lake_spec(
            destination_path="./yaml-lake",
            env=env,
        )
        == "./yaml-lake"
    )
    assert pick_lake_spec(env=env) == "s3://from-env"
    assert pick_lake_spec(env={}) == DEFAULT_LAKE_REL
    assert pick_lake_spec(cli_lake_path="  ", destination_path=None, env={}) == DEFAULT_LAKE_REL
    assert pick_lake_spec(destination_path=None, env={}) == DEFAULT_LAKE_REL


def test_omitted_destination_path_is_unset():
    from det.runtime.config import DestinationConfig

    dest = DestinationConfig(type="filesystem")
    assert dest.path is None


def test_open_lake_local_does_not_use_uri_path(tmp_path: Path):
    root = open_lake("./data/lake", tmp_path)
    assert root.is_local
    assert root.to_path() == (tmp_path / "data" / "lake").resolve()
    assert not is_lake_uri("./data/lake")
    assert is_lake_uri("s3://bucket/prefix")
    assert is_lake_uri("gs://bucket/prefix")
    assert is_lake_uri("memory://t")


def test_open_s3_without_extra_hints_install(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")

    def fail(extra: str):
        raise ImportError(
            f"Object lake {extra} requires the optional extra: pip install 'det[{extra}]'"
        )

    monkeypatch.setattr("det.runtime.lake._import_fsspec", fail)
    with pytest.raises(ImportError, match=r"det\[s3\]"):
        open_lake("s3://bucket/prefix", Path("/tmp"))


def test_open_gcs_without_extra_hints_install(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")

    def fail(extra: str):
        raise ImportError(
            f"Object lake {extra} requires the optional extra: pip install 'det[{extra}]'"
        )

    monkeypatch.setattr("det.runtime.lake._import_fsspec", fail)
    with pytest.raises(ImportError, match=r"det\[gcs\]"):
        open_lake("gs://bucket/prefix", Path("/tmp"))


def test_memory_manifest_commit_and_visibility(tmp_path: Path):
    raw = open_lake("memory://commit", tmp_path) / "raw" / "run"
    (raw / "data").mkdir(parents=True, exist_ok=True)
    (raw / "data" / "page.json").write_text("{}", encoding="utf-8")
    assert not is_committed_raw_dir(raw)
    write_manifest(raw, {"ok": True})
    assert is_committed_raw_dir(raw)
    assert read_manifest(raw)["ok"] is True
    tmp = raw / "meta" / "manifest.json.tmp"
    assert not tmp.exists()


def test_memory_failed_extract_deletes_prefix(project_root: Path, tmp_path: Path):
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
                "destination": {"type": "filesystem", "path": "memory://boom"},
            }
        ),
        encoding="utf-8",
    )

    def boom(self, *, config, interval, data_dir):
        (data_dir / "partial.bin").write_bytes(b"truncated")
        raise RuntimeError("download failed")

    from unittest.mock import patch

    runner = PipelineRunner(tmp_path)
    with (
        patch.object(ExampleApiSource, "extract_to_raw", boom),
        pytest.raises(DetPluginError, match="download failed"),
    ):
        runner.extract(pipe, interval_start="2026-08-06", interval_end="2026-08-07")

    lake = open_lake("memory://boom", tmp_path)
    raw = lake / "raw"
    assert not any(p.name == "manifest.json" for p in raw.rglob("manifest.json"))
    assert not any(p.name == "partial.bin" for p in raw.rglob("partial.bin"))


def test_memory_listing_stays_under_dataset(tmp_path: Path):
    lake = open_lake("memory://list", tmp_path)
    (lake / "raw" / "noaa" / "storm_events_v1" / "keep.txt").write_text("a", encoding="utf-8")
    (lake / "raw" / "noaa" / "other_v1" / "skip.txt").write_text("b", encoding="utf-8")
    dataset = lake / "raw" / "noaa" / "storm_events_v1"
    names = {p.name for p in dataset.iterdir()}
    assert names == {"keep.txt"}
    assert "skip.txt" not in names
    # Listing the dataset prefix must not surface sibling datasets.
    walked = [p.name for p in dataset.rglob("*")]
    assert walked == ["keep.txt"]


def test_http_get_file_memory_upload_and_retry_deletes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    payload = gzip.compress(b"a,b\n1,2\n")

    class FakeResponse:
        def __init__(self, status: int, content: bytes = b"", headers=None):
            self.status_code = status
            self.headers = headers or {}
            self._content = content

        def raise_for_status(self):
            if self.status_code >= 400:
                err = requests.HTTPError(f"HTTP {self.status_code}")
                err.response = self
                raise err

        def iter_content(self, chunk_size: int = 1):
            yield self._content

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    leftover = [
        FakeResponse(503),
        FakeResponse(200, payload, {"Content-Length": str(len(payload))}),
    ]

    def fake_get(self, url, **kwargs):
        return leftover.pop(0)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr("det.sources.http.time.sleep", lambda s: None)

    dest = open_lake("memory://http", tmp_path) / "file.csv.gz"
    n = http_get_file(
        "https://example/file.csv.gz",
        dest,
        timeout=1,
        max_attempts=3,
        encoding="gzip",
    )
    assert n == len(payload)
    assert dest.is_file()
    assert dest.read_bytes() == payload
    assert leftover == []


def test_path_constructor_never_used_for_s3(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Regression: pathlib.Path mangles s3:// into a relative local path."""
    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")

    def boom(*args, **kwargs):
        raise AssertionError("Path() must not wrap object-store lake URIs")

    import det.runtime.lake as lake_mod

    monkeypatch.setattr(lake_mod, "_import_fsspec", boom)
    with pytest.raises(AssertionError, match="must not wrap"):
        open_lake("s3://bucket/prefix", tmp_path)

    monkeypatch.setenv(ENV_LAKE_MODE, "local")
    # Local Path wrapping still works.
    local = open_lake("./data/lake", tmp_path)
    assert isinstance(local, LakeRef)
    assert local.is_local


def test_lake_mode_from_env_defaults_and_parse(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(ENV_LAKE_MODE, raising=False)
    assert lake_mode_from_env() == "local"
    assert lake_mode_from_env({}) == "local"
    assert lake_mode_from_env({ENV_LAKE_MODE: ""}) == "local"
    assert lake_mode_from_env({ENV_LAKE_MODE: "LOCAL"}) == "local"
    assert lake_mode_from_env({ENV_LAKE_MODE: "cloud"}) == "cloud"
    with pytest.raises(ValueError, match="must be 'local' or 'cloud'"):
        lake_mode_from_env({ENV_LAKE_MODE: "hybrid"})


def test_validate_lake_mode_local_rejects_object_uris():
    validate_lake_mode("./data/lake", "local")
    validate_lake_mode("memory://t", "local")
    with pytest.raises(ValueError, match="DET_LAKE_MODE=local forbids"):
        validate_lake_mode("s3://bucket/prefix", "local")
    with pytest.raises(ValueError, match="DET_LAKE_MODE=local forbids"):
        validate_lake_mode("gs://bucket/prefix", "local")


def test_validate_lake_mode_cloud_rejects_local_and_memory():
    validate_lake_mode("s3://bucket/prefix", "cloud")
    validate_lake_mode("gs://bucket/prefix", "cloud")
    validate_lake_mode("gcs://bucket/prefix", "cloud")
    with pytest.raises(ValueError, match="DET_LAKE_MODE=cloud requires"):
        validate_lake_mode("./data/lake", "cloud")
    with pytest.raises(ValueError, match="DET_LAKE_MODE=cloud forbids memory"):
        validate_lake_mode("memory://t", "cloud")


def test_open_lake_enforces_mode_before_import(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    called = {"n": 0}

    def boom(extra: str):
        called["n"] += 1
        raise AssertionError("import should not run when mode rejects")

    monkeypatch.setattr("det.runtime.lake._import_fsspec", boom)
    # Default local rejects s3 before fsspec import.
    with pytest.raises(ValueError, match="DET_LAKE_MODE=local forbids"):
        open_lake("s3://bucket/prefix", tmp_path)
    assert called["n"] == 0

    monkeypatch.setenv(ENV_LAKE_MODE, "cloud")
    with pytest.raises(ValueError, match="DET_LAKE_MODE=cloud requires"):
        open_lake("./data/lake", tmp_path)
    assert called["n"] == 0


def test_local_replace_if_match_bumps_version_same_size(tmp_path: Path):
    from det.runtime.lake import ObjectVersionConflict

    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    target = lake / "locks" / "p" / "x.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    v0 = target.create_exclusive(b"same-size-body")
    v1 = target.replace_if_match(v0, b"same-size-BODY")
    assert v1 != v0
    assert target.read_bytes() == b"same-size-BODY"
    with pytest.raises(ObjectVersionConflict):
        target.replace_if_match(v0, b"stale-writer")
    assert target.delete_if_match(v0) is False
    assert target.delete_if_match(v1) is True
    assert not target.exists()


def test_local_cas_serializes_concurrent_replace(tmp_path: Path):
    import threading

    from det.runtime.lake import ObjectVersionConflict

    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    target = lake / "locks" / "p" / "race.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    version = target.create_exclusive(b"seed")
    results: list[str | BaseException] = []

    def worker(payload: bytes) -> None:
        try:
            results.append(target.replace_if_match(version, payload))
        except BaseException as exc:  # noqa: BLE001
            results.append(exc)

    t1 = threading.Thread(target=worker, args=(b"one",))
    t2 = threading.Thread(target=worker, args=(b"two",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    wins = [r for r in results if isinstance(r, str)]
    losses = [r for r in results if isinstance(r, ObjectVersionConflict)]
    assert len(wins) == 1
    assert len(losses) == 1
    assert target.read_bytes() in {b"one", b"two"}


def test_is_precondition_failed_requires_structured_signal():
    from det.runtime.lake import _is_precondition_failed, _raise_s3_cas

    class PreconditionFailed(Exception):
        pass

    assert _is_precondition_failed(PreconditionFailed("x"))

    wrapped = OSError(22, "pre-condition failed")
    wrapped.__cause__ = Exception("noise")
    assert not _is_precondition_failed(wrapped)
    assert not _is_precondition_failed(OSError("If-Match header missing"))

    client = Exception("ClientError")
    client.response = {  # type: ignore[attr-defined]
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }
    outer = OSError(22, "Invalid argument")
    outer.__cause__ = client
    assert _is_precondition_failed(outer)

    with pytest.raises(FileExistsError):
        _raise_s3_cas(outer, "b/k", create=True)
