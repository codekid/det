"""DetSettings: from_env, overrides, secrets callable, runner wiring."""

from __future__ import annotations

from pathlib import Path

import pytest

import det
from det.destinations.models import lake_root
from det.runtime.config import DestinationConfig
from det.runtime.lake import pick_lake_spec
from det.runtime.runner import PipelineRunner
from det.runtime.secrets import resolve_secret
from det.runtime.settings import DetSettings, use_settings


def test_from_env_reads_lake_locks_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "lake"))
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.setenv("DET_LOCK", "0")
    monkeypatch.setenv("DET_LOCK_TTL_SEC", "90")
    monkeypatch.setenv("DET_LOCK_OWNER", "test-owner")
    monkeypatch.setenv("DET_SECRETS_BACKEND", "env")
    monkeypatch.setenv("DET_SECRETS_TTL_SEC", "10")

    settings = DetSettings.from_env(project_root=tmp_path)
    assert settings.project_root == tmp_path.resolve()
    assert settings.lake_path == str(tmp_path / "lake")
    assert settings.lake_mode == "local"
    assert settings.locks_enabled is False
    assert settings.lock_ttl_sec == 90
    assert settings.lock_owner == "test-owner"
    assert settings.secrets_backend == "env"
    assert settings.secrets_ttl_sec == 10


def test_with_overrides_lake_and_lock(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_override="/tmp/cli-lake",
        lock_ttl_sec=30,
        lock_owner="cli",
    )
    assert settings.lake_override == "/tmp/cli-lake"
    assert settings.lock_ttl_sec == 30
    assert settings.lock_owner == "cli"
    assert settings.effective_lock_ttl(None) == 30
    assert settings.effective_lock_ttl(12) == 12


def test_pick_lake_prefers_override_then_destination_then_settings(
    tmp_path: Path,
) -> None:
    assert (
        pick_lake_spec(
            cli_lake_path="/cli",
            destination_path="/dest",
            settings_lake_path="/settings",
        )
        == "/cli"
    )
    assert (
        pick_lake_spec(
            destination_path="/dest",
            settings_lake_path="/settings",
        )
        == "/dest"
    )
    assert pick_lake_spec(settings_lake_path="/settings") == "/settings"


def test_lake_root_uses_settings(tmp_path: Path) -> None:
    lake_dir = tmp_path / "from-settings"
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path=str(lake_dir)
    )
    dest = DestinationConfig(type="filesystem")
    ref = lake_root(dest, tmp_path, settings=settings)
    assert Path(str(ref)).resolve() == lake_dir.resolve()


def test_custom_resolve_secret_callable(tmp_path: Path) -> None:
    calls: list[str] = []

    def lookup(name: str) -> str | None:
        calls.append(name)
        if name == "MY_TOKEN":
            return "tok-value"
        return None

    settings = DetSettings.from_env(project_root=tmp_path, resolve_secret=lookup)
    with use_settings(settings):
        assert resolve_secret("MY_TOKEN", keys=("token", "value")) == "tok-value"
        # Cached — second call should not re-invoke lookup when TTL > 0
        assert resolve_secret("MY_TOKEN", keys=("value",)) == "tok-value"
    assert calls == ["MY_TOKEN"]


def test_runner_accepts_settings(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        locks_enabled=False
    )
    runner = PipelineRunner(settings=settings)
    assert runner.project_root == tmp_path.resolve()
    assert runner.settings is settings


def test_runner_rejects_both_settings_and_project_root(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path)
    with pytest.raises(ValueError, match="not both"):
        PipelineRunner(tmp_path, settings=settings)


def test_settings_secret_caches_are_isolated(tmp_path: Path) -> None:
    """Two DetSettings instances do not share a secret cache (#34)."""
    calls_a: list[str] = []
    calls_b: list[str] = []

    def lookup_a(name: str) -> str | None:
        calls_a.append(name)
        return "a-tok" if name == "TOK" else None

    def lookup_b(name: str) -> str | None:
        calls_b.append(name)
        return "b-tok" if name == "TOK" else None

    settings_a = DetSettings.from_env(project_root=tmp_path, resolve_secret=lookup_a)
    settings_b = DetSettings.from_env(project_root=tmp_path, resolve_secret=lookup_b)

    with use_settings(settings_a):
        assert resolve_secret("TOK", keys=("value",)) == "a-tok"
        assert resolve_secret("TOK", keys=("value",)) == "a-tok"
    with use_settings(settings_b):
        assert resolve_secret("TOK", keys=("value",)) == "b-tok"
        assert resolve_secret("TOK", keys=("value",)) == "b-tok"

    assert calls_a == ["TOK"]
    assert calls_b == ["TOK"]


def test_det_exports_settings() -> None:
    assert "DetSettings" in det.__all__
    assert det.DetSettings is DetSettings
