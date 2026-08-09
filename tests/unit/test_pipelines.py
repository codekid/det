from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.runtime.pipelines import (
    PipelineRefError,
    list_pipeline_ids,
    resolve_pipeline_ref,
    resolve_project_root,
)


def _write_pipeline(root: Path, canonical: str = "noaa.storm_events") -> Path:
    provider, source = canonical.split(".", 1)
    path = root / "configs" / "pipelines" / provider / f"{source}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": canonical,
                "source": {"type": canonical},
                "schema": f"schemas/{provider}/{source}/{source}.schema.yaml",
            }
        ),
        encoding="utf-8",
    )
    return path


def test_resolve_canonical_id(tmp_path: Path):
    path = _write_pipeline(tmp_path)
    resolved = resolve_pipeline_ref("noaa.storm_events", project_root=tmp_path)
    assert resolved.path == path.resolve()
    assert resolved.canonical_id == "noaa.storm_events"
    assert resolved.relative_path == "configs/pipelines/noaa/storm_events.yaml"


def test_resolve_slash_and_yaml_path(tmp_path: Path):
    path = _write_pipeline(tmp_path)
    by_slash = resolve_pipeline_ref("noaa/storm_events", project_root=tmp_path)
    by_path = resolve_pipeline_ref(
        "configs/pipelines/noaa/storm_events.yaml", project_root=tmp_path
    )
    assert by_slash.path == path.resolve()
    assert by_path.path == path.resolve()


def test_resolve_unknown_lists_known(tmp_path: Path):
    _write_pipeline(tmp_path)
    with pytest.raises(PipelineRefError, match="noaa.storm_events"):
        resolve_pipeline_ref("missing.feed", project_root=tmp_path)


def test_list_pipeline_ids(tmp_path: Path):
    _write_pipeline(tmp_path, "noaa.storm_events")
    _write_pipeline(tmp_path, "example_api.events")
    assert list_pipeline_ids(tmp_path) == ["example_api.events", "noaa.storm_events"]


def test_resolve_project_root_prefers_explicit_then_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    explicit = tmp_path / "explicit"
    explicit.mkdir()
    env_root = tmp_path / "env"
    env_root.mkdir()
    monkeypatch.setenv("DET_PROJECT_ROOT", str(env_root))
    assert resolve_project_root(explicit) == explicit.resolve()
    assert resolve_project_root(None) == env_root.resolve()
    monkeypatch.delenv("DET_PROJECT_ROOT")
    assert resolve_project_root(None) == Path.cwd().resolve()


def test_cli_accepts_canonical_id(tmp_path: Path, project_root: Path):
    import structlog
    from typer.testing import CliRunner

    from det.cli import app
    from det.logging import configure_logging

    runner = CliRunner()
    try:
        result = runner.invoke(
            app,
            [
                "list-pipelines",
                "--project-root",
                str(project_root),
            ],
        )
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")
    assert result.exit_code == 0
    assert "noaa.storm_events" in result.stdout
    assert "example_api.events" in result.stdout
