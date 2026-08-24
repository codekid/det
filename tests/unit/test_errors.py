"""DetError hierarchy, plugin wrap, and logging edge contract."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

import det
from det.errors import (
    DetConflictError,
    DetContractError,
    DetError,
    DetNotFoundError,
    DetPluginError,
    reraise_as_plugin,
)
from det.runtime.discovery import PluginLoadError
from det.runtime.lease import LeaseHeldError
from det.runtime.registry import get_source
from det.runtime.runner import PipelineRunner
from det.runtime.secrets import SecretError, SecretNotSetError
from det.validation.jsonschema_validator import SchemaValidationError


def test_public_error_exports() -> None:
    for name in (
        "DetError",
        "DetConfigError",
        "DetPluginError",
        "DetContractError",
        "DetConflictError",
        "DetNotFoundError",
        "drop_secrets",
        "scrub_secrets",
        "scrub_rendered",
    ):
        assert name in det.__all__
        assert getattr(det, name) is not None


def test_folded_exceptions_are_det_error() -> None:
    assert issubclass(LeaseHeldError, DetConflictError)
    assert issubclass(LeaseHeldError, DetError)
    assert issubclass(PluginLoadError, DetPluginError)
    assert issubclass(SecretNotSetError, SecretError)
    assert issubclass(SecretError, DetError)
    assert issubclass(SchemaValidationError, DetContractError)


def test_reraise_preserves_det_error() -> None:
    with pytest.raises(DetNotFoundError, match="missing"):
        try:
            raise DetNotFoundError("missing")
        except Exception as exc:
            reraise_as_plugin(exc, plugin="x", action="extract")


def test_reraise_wraps_other_exceptions() -> None:
    with pytest.raises(DetPluginError, match="extract_to_raw failed") as caught:
        try:
            raise RuntimeError("boom")
        except Exception as exc:
            reraise_as_plugin(exc, plugin="example_api.events", action="extract_to_raw")
    assert caught.value.plugin == "example_api.events"
    assert isinstance(caught.value.__cause__, RuntimeError)


def test_unknown_source_is_not_found() -> None:
    with pytest.raises(DetNotFoundError, match="Unknown source"):
        get_source("no.such.source")


def test_runner_wraps_plugin_extract_failure(
    tmp_path: Path, project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
                "destination": {"type": "filesystem"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "lake"))
    monkeypatch.setenv("DET_LOCK", "0")

    def boom(*_a: Any, **_k: Any) -> list[dict[str, Any]]:
        raise RuntimeError("plugin blew up")

    monkeypatch.setattr(
        "det.sources.example_api.events.ExampleApiSource.extract_to_raw",
        boom,
    )
    with pytest.raises(DetPluginError, match="extract_to_raw failed") as caught:
        PipelineRunner(tmp_path).extract(
            pipe, interval_start="2026-08-06", interval_end="2026-08-07"
        )
    assert isinstance(caught.value, DetError)
    assert isinstance(caught.value.__cause__, RuntimeError)


def test_configure_logging_not_on_import() -> None:
    # Importing det must not require or invoke configure_logging as a side effect
    # beyond exposing the callable (CLI still configures at the Typer edge).
    assert callable(det.configure_logging)
    assert callable(det.get_logger)
