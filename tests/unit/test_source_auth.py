"""A declared credential must resolve or fail the run — never a silent public fetch."""

from __future__ import annotations

from pathlib import Path

import pytest

from det.runtime.secrets import SecretNotSetError
from det.sources.base import Interval, merge_source_config
from det.sources.example_api.events import ExampleApiSource
from det.sources.http_json import source_bearer_token
from det.sources.openlibrary.subjects import OpenLibrarySubjectsSource

_AUTH_NAMES = ("EXAMPLE_API_TOKEN", "DET_EXAMPLE_API", "EXAMPLE_API")


def _unset_example_api(monkeypatch) -> None:
    for name in _AUTH_NAMES:
        monkeypatch.delenv(name, raising=False)


def _interval() -> Interval:
    return Interval(start="2026-08-06T00:00:00+00:00", end="2026-08-07T00:00:00+00:00")


def test_declared_auth_source_fails_when_the_secret_is_unset(monkeypatch):
    _unset_example_api(monkeypatch)
    source = ExampleApiSource()
    config = merge_source_config(source.defaults(), {"fixture_records": None})
    with pytest.raises(SecretNotSetError, match="EXAMPLE_API_TOKEN"):
        source_bearer_token(config, source_name=source.name)


def test_legacy_auth_env_name_still_resolves(monkeypatch):
    _unset_example_api(monkeypatch)
    monkeypatch.setenv("EXAMPLE_API_TOKEN", "legacy-token")
    source = ExampleApiSource()
    config = merge_source_config(source.defaults(), {})
    assert source_bearer_token(config, source_name=source.name) == "legacy-token"


def test_provider_named_secret_resolves_without_auth_env(monkeypatch):
    _unset_example_api(monkeypatch)
    monkeypatch.setenv("DET_EXAMPLE_API", '{"token": "provider-token"}')
    source = ExampleApiSource()
    config = merge_source_config(source.defaults(), {})
    assert source_bearer_token(config, source_name=source.name) == "provider-token"


def test_public_source_performs_no_lookup(monkeypatch):
    monkeypatch.delenv("DET_OPENLIBRARY", raising=False)
    monkeypatch.delenv("OPENLIBRARY", raising=False)
    source = OpenLibrarySubjectsSource()
    config = merge_source_config(source.defaults(), {})
    assert source_bearer_token(config, source_name=source.name) is None


def test_fixture_extract_needs_no_secret(monkeypatch, tmp_path: Path):
    _unset_example_api(monkeypatch)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    source = ExampleApiSource()
    config = merge_source_config(
        source.defaults(), {"fixture_records": [{"id": "e1"}]}
    )
    artifacts = source.extract_to_raw(
        config=config,
        interval=_interval(),
        data_dir=data_dir,
    )
    assert artifacts and artifacts[0]["origin"] == "fixture_records"
