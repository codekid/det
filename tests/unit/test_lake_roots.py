"""LakeRoots: unified (layout 1) vs split (layout 2) resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from det.destinations.models import bronze_dataset_dir, raw_dataset_dir
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
    ValidationConfig,
)
from det.runtime.lake import (
    DEFAULT_LAKE_REL,
    LakeRoots,
    clear_memory_lakes,
    is_split_lake_configured,
    reset_lake_mode_warning_for_tests,
    resolve_lake_roots,
    validate_lake_roots,
)
from det.runtime.settings import DetSettings


@pytest.fixture(autouse=True)
def _reset_memory(monkeypatch: pytest.MonkeyPatch):
    clear_memory_lakes()
    reset_lake_mode_warning_for_tests()
    monkeypatch.delenv("DET_LAKE_MODE", raising=False)
    for key in (
        "DET_LAKE_PATH",
        "DET_LAKE_PATH_RAW",
        "DET_LAKE_PATH_BRONZE",
        "DET_LAKE_PATH_OPS",
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    clear_memory_lakes()
    reset_lake_mode_warning_for_tests()


def _pipeline(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        name="example_api.events",
        source=SourceConfig(type="example_api.events"),
        schema_path="schemas/example_api/events/events.schema.yaml",
        validation=ValidationConfig(),
        ingestion=IngestionConfig(),
        destination=DestinationConfig(type="filesystem"),
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
        wire_version=1,
    )


def test_resolve_unified_default(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path)
    roots = resolve_lake_roots(settings, project_root=tmp_path)
    assert roots.layout == 1
    assert not roots.is_split
    assert roots.raw == roots.bronze == roots.ops
    assert roots.unified_spec == DEFAULT_LAKE_REL


def test_resolve_split_from_settings_overrides(tmp_path: Path) -> None:
    raw = tmp_path / "acme-raw"
    bronze = tmp_path / "acme-bronze"
    ops = tmp_path / "acme-ops"
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=str(raw),
        lake_path_bronze=str(bronze),
        lake_path_ops=str(ops),
    )
    roots = resolve_lake_roots(settings, project_root=tmp_path)
    assert roots.layout == 2
    assert roots.is_split
    assert Path(str(roots.raw)).resolve() == raw.resolve()
    assert Path(str(roots.bronze)).resolve() == bronze.resolve()
    assert Path(str(roots.ops)).resolve() == ops.resolve()


def test_resolve_split_incomplete_raises(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=str(tmp_path / "raw-only"),
    )
    with pytest.raises(ValueError, match="missing DET_LAKE_PATH_BRONZE"):
        resolve_lake_roots(settings, project_root=tmp_path)


def test_is_split_lake_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert not is_split_lake_configured(DetSettings.from_env(project_root=tmp_path))
    monkeypatch.setenv("DET_LAKE_PATH_RAW", str(tmp_path / "r"))
    settings = DetSettings.from_env(project_root=tmp_path)
    assert is_split_lake_configured(settings)


def test_dataset_dirs_flattened_in_layout_2(tmp_path: Path) -> None:
    raw = tmp_path / "layer-raw"
    bronze = tmp_path / "layer-bronze"
    ops = tmp_path / "layer-ops"
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=str(raw),
        lake_path_bronze=str(bronze),
        lake_path_ops=str(ops),
    )
    cfg = _pipeline(tmp_path)
    from det.runtime.settings import use_settings

    with use_settings(settings):
        raw_dir = raw_dataset_dir(cfg, tmp_path, settings=settings)
        bronze_dir = bronze_dataset_dir(cfg, tmp_path, settings=settings)
    assert Path(str(raw_dir)).resolve() == (raw / "example_api" / "events_v1").resolve()
    assert Path(str(bronze_dir)).resolve() == (
        bronze / "example_api" / "events_v1"
    ).resolve()
    # No medallion prefix in layout 2.
    assert "raw" not in Path(str(raw_dir)).parts[-3:]
    assert "bronze" not in Path(str(bronze_dir)).parts[-3:]


def test_dataset_dirs_prefixed_in_layout_1(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path=str(lake)
    )
    cfg = _pipeline(tmp_path)
    raw_dir = raw_dataset_dir(cfg, tmp_path, settings=settings)
    bronze_dir = bronze_dataset_dir(cfg, tmp_path, settings=settings)
    assert Path(str(raw_dir)).resolve() == (
        lake / "raw" / "example_api" / "events_v1"
    ).resolve()
    assert Path(str(bronze_dir)).resolve() == (
        lake / "bronze" / "example_api" / "events_v1"
    ).resolve()


def test_destination_path_ignored_in_split_mode(tmp_path: Path) -> None:
    raw = tmp_path / "r"
    bronze = tmp_path / "b"
    ops = tmp_path / "o"
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=str(raw),
        lake_path_bronze=str(bronze),
        lake_path_ops=str(ops),
    )
    roots = resolve_lake_roots(
        settings,
        project_root=tmp_path,
        destination_path=str(tmp_path / "should-ignore"),
    )
    assert Path(str(roots.raw)).resolve() == raw.resolve()


def test_cli_override_wins_over_settings(tmp_path: Path) -> None:
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path_raw=str(tmp_path / "settings-raw"),
        lake_path_bronze=str(tmp_path / "settings-bronze"),
        lake_path_ops=str(tmp_path / "settings-ops"),
    )
    cli_raw = tmp_path / "cli-raw"
    roots = resolve_lake_roots(
        settings,
        project_root=tmp_path,
        cli_lake_path_raw=str(cli_raw),
        cli_lake_path_bronze=str(tmp_path / "settings-bronze"),
        cli_lake_path_ops=str(tmp_path / "settings-ops"),
    )
    assert Path(str(roots.raw)).resolve() == cli_raw.resolve()


class _Uri:
    def __init__(self, spec: str) -> None:
        self._spec = spec

    def __str__(self) -> str:
        return self._spec


def test_validate_split_rejects_mixed_object_schemes() -> None:
    roots = LakeRoots(
        raw=_Uri("s3://acme-raw"),  # type: ignore[arg-type]
        bronze=_Uri("gs://acme-bronze"),  # type: ignore[arg-type]
        ops=_Uri("s3://acme-ops"),  # type: ignore[arg-type]
        layout=2,
    )
    with pytest.raises(ValueError, match="URI kind"):
        validate_lake_roots(roots, mode="cloud")


def test_validate_split_allows_matching_s3_schemes() -> None:
    roots = LakeRoots(
        raw=_Uri("s3://acme-raw"),  # type: ignore[arg-type]
        bronze=_Uri("s3://acme-bronze"),  # type: ignore[arg-type]
        ops=_Uri("s3://acme-ops"),  # type: ignore[arg-type]
        layout=2,
    )
    validate_lake_roots(roots, mode="cloud")
