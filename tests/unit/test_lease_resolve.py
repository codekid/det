"""Lease option resolution and Postgres fail-closed."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.runtime.config import LeaseConfig, load_pipeline_config
from det.runtime.lake import clear_memory_lakes, open_lake
from det.runtime.lease import (
    ResolvedLeaseOptions,
    open_lease_store,
    resolve_lease_options,
)
from det.runtime.settings import DetSettings


@pytest.fixture(autouse=True)
def _reset_memory():
    clear_memory_lakes()
    yield
    clear_memory_lakes()


def test_resolve_pipeline_backend_before_settings_default(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path, env={"DET_LAKE_MODE": "local"})
    assert settings.lock_backend == "lake"
    opts = resolve_lease_options(
        settings=settings,
        pipeline=type("P", (), {"lease": LeaseConfig(backend="postgres")})(),
        env={},
    )
    assert opts.backend == "postgres"


def test_resolve_env_backend_wins(tmp_path: Path) -> None:
    settings = DetSettings.from_env(
        project_root=tmp_path,
        env={"DET_LAKE_MODE": "local", "DET_LOCK_BACKEND": "postgres"},
    )
    opts = resolve_lease_options(
        settings=settings,
        pipeline=type("P", (), {"lease": LeaseConfig(backend="lake")})(),
        env={"DET_LOCK_BACKEND": "lake"},
    )
    assert opts.backend == "lake"


def test_overlap_requires_postgres() -> None:
    with pytest.raises(ValueError, match="overlap"):
        resolve_lease_options(env={"DET_LOCK_MODE": "overlap", "DET_LOCK_BACKEND": "lake"})


def test_lease_yaml_overlap_with_lake_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="overlap"):
        LeaseConfig(backend="lake", mode="overlap")


def test_postgres_store_fail_closed_without_dsn(tmp_path: Path) -> None:
    lake = open_lake("memory://pgfail", Path("/tmp"))
    options = ResolvedLeaseOptions(backend="postgres", pg_dsn_env="DET_LOCK_PG_DSN_MISSING")
    store = open_lease_store(lake, options, resolve_secret=lambda _n: None)
    with pytest.raises(RuntimeError, match="DET_LOCK_PG_DSN_MISSING"):
        store.acquire(
            pipeline="example_api.events",
            interval_start="2026-08-15T00:00:00+00:00",
            interval_end="2026-08-16T00:00:00+00:00",
            command="extract",
            ttl_sec=60,
            owner="test",
        )


def test_pipeline_yaml_lease_roundtrip(tmp_path: Path, project_root: Path) -> None:
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
                "source": {"type": "example_api.events"},
                "schema": "schemas/example_api/events/events.schema.yaml",
                "lease": {"backend": "postgres", "mode": "exact"},
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
            }
        ),
        encoding="utf-8",
    )
    config = load_pipeline_config(pipe)
    assert config.lease is not None
    assert config.lease.backend == "postgres"
    opts = resolve_lease_options(pipeline=config, env={})
    assert opts.backend == "postgres"
    assert opts.mode == "exact"
