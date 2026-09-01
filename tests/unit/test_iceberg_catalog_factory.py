"""Unit tests for Iceberg catalog env resolution and factory props."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from det.ingestion.iceberg_catalog_factory import (
    ENV_CATALOG,
    ENV_GLUE_ID,
    ENV_REST_CREDENTIAL,
    ENV_REST_URI,
    ENV_REST_WAREHOUSE,
    catalog_kind_from_env,
    glue_catalog_props,
    maybe_bind_location,
    resolve_iceberg_catalog,
    rest_catalog_props,
)
from det.ingestion.iceberg_writer import ensure_iceberg_table
from det.runtime.lake import open_lake


def test_catalog_kind_default_hadoop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_CATALOG, raising=False)
    monkeypatch.delenv(ENV_REST_URI, raising=False)
    assert catalog_kind_from_env({}) == "hadoop"
    assert catalog_kind_from_env() == "hadoop"


def test_catalog_kind_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CATALOG, "REST")
    assert catalog_kind_from_env() == "rest"
    monkeypatch.setenv(ENV_CATALOG, "glue")
    assert catalog_kind_from_env() == "glue"


def test_catalog_kind_soft_default_from_rest_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_CATALOG, raising=False)
    monkeypatch.setenv(ENV_REST_URI, "https://catalog.example/iceberg")
    assert catalog_kind_from_env() == "rest"


def test_catalog_kind_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_CATALOG, "polar")
    with pytest.raises(ValueError, match="hadoop|rest|glue"):
        catalog_kind_from_env()


def test_object_store_root_uri() -> None:
    from det.ingestion.iceberg_catalog_factory import object_store_root_uri

    assert object_store_root_uri("s3://det-ci/lake/bronze/x") == "s3://det-ci/"
    assert object_store_root_uri("gs://b/a") == "gs://b/"
    assert object_store_root_uri("gcs://b/a") == "gs://b/"
    assert object_store_root_uri("file:///tmp/lake/bronze/x") is None


def test_ensure_iceberg_namespace_idempotent_and_sets_location() -> None:
    from det.ingestion.iceberg_catalog_factory import ensure_iceberg_namespace

    calls: list[tuple[str, dict[str, str]]] = []

    class _Cat:
        def create_namespace(self, namespace: str, properties: Any = None) -> None:
            props = dict(properties or {})
            calls.append((namespace, props))
            if len(calls) > 1:
                from pyiceberg.exceptions import NamespaceAlreadyExistsError

                raise NamespaceAlreadyExistsError("already")

    cat = _Cat()
    ensure_iceberg_namespace(
        cat, "bronze_example_api", table_location="s3://det-ci/lake/bronze/x"
    )
    ensure_iceberg_namespace(
        cat, "bronze_example_api", table_location="s3://det-ci/lake/bronze/x"
    )
    assert calls == [
        ("bronze_example_api", {"location": "s3://det-ci/"}),
        ("bronze_example_api", {"location": "s3://det-ci/"}),
    ]


def test_rest_catalog_props_require_uri(tmp_path: Path) -> None:
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    with pytest.raises(ValueError, match=ENV_REST_URI):
        rest_catalog_props(lake, env={ENV_CATALOG: "rest"})


def test_rest_catalog_props_builds(tmp_path: Path) -> None:
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    props = rest_catalog_props(
        lake,
        env={
            ENV_REST_URI: "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            ENV_REST_WAREHOUSE: "bl://projects/p/catalogs/c",
            ENV_REST_CREDENTIAL: "cid:secret",
        },
    )
    assert props["type"] == "rest"
    assert props["uri"].startswith("https://biglake.googleapis.com/")
    assert props["warehouse"] == "bl://projects/p/catalogs/c"
    assert props["credential"] == "cid:secret"


def test_rest_catalog_props_default_warehouse_to_lake(tmp_path: Path) -> None:
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    props = rest_catalog_props(
        lake,
        env={ENV_REST_URI: "http://localhost:8181/catalog"},
    )
    assert props["warehouse"].startswith("file://")
    assert "credential" not in props
    assert "rest.sigv4-enabled" not in props


def test_rest_catalog_props_glue_enables_sigv4() -> None:
    class _Fake:
        is_local = False

        def __str__(self) -> str:
            return "s3://bucket/det-lake"

    props = rest_catalog_props(
        _Fake(),  # type: ignore[arg-type]
        env={
            ENV_REST_URI: "https://glue.us-west-2.amazonaws.com/iceberg",
            ENV_REST_WAREHOUSE: "s3://bucket/det-lake",
            "AWS_REGION": "us-west-2",
        },
    )
    assert props["rest.sigv4-enabled"] == "true"
    assert props["rest.signing-name"] == "glue"
    assert props["rest.signing-region"] == "us-west-2"


def test_resolve_rest_glue_passes_sigv4_to_load_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv(
        ENV_REST_URI, "https://glue.us-east-1.amazonaws.com/iceberg"
    )
    monkeypatch.setenv(ENV_REST_WAREHOUSE, "s3://bucket/lake")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    lake.mkdir(parents=True, exist_ok=True)
    sentinel = object()
    seen: dict[str, str] = {}

    def _fake_load(name: str, **props: str) -> object:
        assert name == "det"
        seen.update(props)
        return sentinel

    monkeypatch.setattr("pyiceberg.catalog.load_catalog", _fake_load)
    assert resolve_iceberg_catalog(lake) is sentinel
    assert seen["rest.sigv4-enabled"] == "true"
    assert seen["rest.signing-name"] == "glue"
    assert seen["rest.signing-region"] == "us-east-1"


def test_glue_catalog_props_require_s3(tmp_path: Path) -> None:
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    with pytest.raises(ValueError, match="s3://"):
        glue_catalog_props(lake, env={ENV_CATALOG: "glue"})


def test_glue_catalog_props_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    class _Fake:
        is_local = False

        def __str__(self) -> str:
            return "s3://bucket/det-lake"

    fake = _Fake()
    props = glue_catalog_props(
        fake,  # type: ignore[arg-type]
        env={
            ENV_CATALOG: "glue",
            ENV_GLUE_ID: "123456789012",
            "AWS_REGION": "us-west-2",
            "AWS_ACCESS_KEY_ID": "AKIA",
            "AWS_SECRET_ACCESS_KEY": "secret",
        },
    )
    assert props["type"] == "glue"
    assert props["warehouse"] == "s3://bucket/det-lake"
    assert props["glue.id"] == "123456789012"
    assert props["client.region"] == "us-west-2"
    assert props["s3.access-key-id"] == "AKIA"


def test_maybe_bind_location_skips_without_method() -> None:
    class Stub:
        def load_table(self, ident: Any) -> Any:
            return None

    stub = Stub()
    maybe_bind_location(stub, ("bronze_noaa", "storm_events_v1"), "file:///tmp/t")


def test_maybe_bind_location_calls_when_present() -> None:
    catalog = MagicMock()
    maybe_bind_location(catalog, ("ns", "t"), "s3://b/t")
    catalog.bind_location.assert_called_once_with(("ns", "t"), "s3://b/t")


def test_resolve_hadoop_returns_lake_hadoop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    monkeypatch.delenv(ENV_CATALOG, raising=False)
    monkeypatch.delenv(ENV_REST_URI, raising=False)
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    lake.mkdir(parents=True, exist_ok=True)
    catalog = resolve_iceberg_catalog(lake)
    from det.ingestion.iceberg_catalog import LakeHadoopCatalog

    assert isinstance(catalog, LakeHadoopCatalog)


def test_resolve_rest_calls_load_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv(ENV_REST_URI, "http://localhost:8181/")
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    lake.mkdir(parents=True, exist_ok=True)
    sentinel = object()

    def _fake_load(name: str, **props: str) -> object:
        assert name == "det"
        assert props["type"] == "rest"
        assert props["uri"] == "http://localhost:8181/"
        return sentinel

    monkeypatch.setattr(
        "pyiceberg.catalog.load_catalog",
        _fake_load,
    )
    assert resolve_iceberg_catalog(lake) is sentinel


def test_ensure_iceberg_table_without_bind_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pyiceberg")
    from pyiceberg.exceptions import NoSuchTableError

    created: dict[str, Any] = {}

    class Restish:
        def load_table(self, identifier: Any) -> Any:
            raise NoSuchTableError("missing")

        def create_namespace(self, ns: str) -> None:
            created["ns"] = ns

        def create_table(self, identifier: Any, **kwargs: Any) -> Any:
            created["ident"] = identifier
            created["location"] = kwargs.get("location")
            return MagicMock(name="table")

    table = ensure_iceberg_table(
        catalog=Restish(),
        identifier=("bronze_noaa", "storm_events_v1"),
        location="file:///tmp/bronze/noaa/storm_events_v1",
        columns=[("__row_hash", "STRING"), ("id", "INTEGER")],
        partition="none",
    )
    assert table is not None
    assert created["ns"] == "bronze_noaa"
    assert created["ident"] == ("bronze_noaa", "storm_events_v1")
    assert created["location"].endswith("storm_events_v1")
