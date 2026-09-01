"""Unit tests for iceberg-register plan/apply."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml

from det.ingestion.iceberg_catalog_factory import ENV_CATALOG, ENV_REST_URI
from det.runtime.iceberg_register import (
    apply_iceberg_register,
    build_iceberg_register_plan,
    iceberg_register_write_argv,
)


def _write_pipeline(tmp_path: Path) -> Path:
    schema = tmp_path / "schemas/example_api/events/events.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    pipe = tmp_path / "configs/pipelines/example_api/events.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {"type": "example_api.events"},
                "schema": "schemas/example_api/events/events.schema.yaml",
                "destination": {"type": "iceberg"},
            }
        ),
        encoding="utf-8",
    )
    return pipe


def _plant_bronze(tmp_path: Path, *, with_ops: bool = False) -> Path:
    lake = tmp_path / "lake"
    table = lake / "bronze" / "example_api" / "events_v1"
    meta = table / "metadata"
    meta.mkdir(parents=True)
    meta_file = meta / "00000-abc.metadata.json"
    meta_file.write_text("{}", encoding="utf-8")
    (meta / "version-hint.text").write_text("00000-abc", encoding="utf-8")
    if with_ops:
        ops = lake / "ops" / "run_receipts" / "metadata"
        ops.mkdir(parents=True)
        ops_meta = ops / "00000-ops.metadata.json"
        ops_meta.write_text("{}", encoding="utf-8")
        (ops / "version-hint.text").write_text("00000-ops", encoding="utf-8")
    return lake


def test_plan_refuses_hadoop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_pipeline(tmp_path)
    lake = _plant_bronze(tmp_path)
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    monkeypatch.delenv(ENV_CATALOG, raising=False)
    monkeypatch.delenv(ENV_REST_URI, raising=False)
    with pytest.raises(ValueError, match="hadoop"):
        build_iceberg_register_plan(project_root=tmp_path)


def test_plan_rest_missing_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_pipeline(tmp_path)
    lake = _plant_bronze(tmp_path)
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.delenv(ENV_REST_URI, raising=False)
    with pytest.raises(ValueError, match=ENV_REST_URI):
        build_iceberg_register_plan(project_root=tmp_path)


def test_plan_glue_requires_s3(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_pipeline(tmp_path)
    lake = _plant_bronze(tmp_path)
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    monkeypatch.setenv(ENV_CATALOG, "glue")
    with pytest.raises(ValueError, match="s3://"):
        build_iceberg_register_plan(project_root=tmp_path)


def test_plan_builds_bronze_and_ops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_pipeline(tmp_path)
    lake = _plant_bronze(tmp_path, with_ops=True)
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv(ENV_REST_URI, "http://localhost:8181/api/catalog")
    monkeypatch.setenv("DET_ICEBERG_REST_WAREHOUSE", "det_lake")
    plan = build_iceberg_register_plan(project_root=tmp_path)
    assert plan.catalog_kind == "rest"
    assert plan.rest_uri_host == "localhost:8181"
    assert plan.warehouse == "det_lake"
    names = {(t.namespace, t.table, t.kind) for t in plan.tables}
    assert ("bronze_example_api", "events_v1", "bronze") in names
    assert ("ops", "run_receipts", "ops") in names


def test_plan_pipeline_skips_ops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pipe = _write_pipeline(tmp_path)
    lake = _plant_bronze(tmp_path, with_ops=True)
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv(ENV_REST_URI, "http://localhost:8181/api/catalog")
    plan = build_iceberg_register_plan(
        project_root=tmp_path,
        pipeline=pipe,
        include_ops=False,
    )
    assert len(plan.tables) == 1
    assert plan.tables[0].kind == "bronze"


def test_argv() -> None:
    assert iceberg_register_write_argv(lake_path="s3://b/l", skip_ops=True) == [
        "iceberg-register",
        "--apply",
        "--lake-path",
        "s3://b/l",
        "--skip-ops",
    ]


def test_apply_register_then_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_pipeline(tmp_path)
    lake = _plant_bronze(tmp_path)
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    monkeypatch.setenv(ENV_CATALOG, "rest")
    monkeypatch.setenv(ENV_REST_URI, "http://localhost:8181/api/catalog")
    plan = build_iceberg_register_plan(project_root=tmp_path, include_ops=False)

    from pyiceberg.exceptions import NoSuchTableError

    catalog = MagicMock()
    catalog.load_table.side_effect = [NoSuchTableError("missing"), object()]
    registered: list[Any] = []

    def _register(ident: Any, meta: str) -> Any:
        registered.append((ident, meta))
        return object()

    catalog.register_table.side_effect = _register
    monkeypatch.setattr(
        "det.runtime.iceberg_register.resolve_iceberg_catalog",
        lambda *_a, **_k: catalog,
    )

    first = apply_iceberg_register(plan, project_root=tmp_path)
    assert first["count"] == 1
    assert first["applied"][0]["status"] == "registered"
    assert registered

    second = apply_iceberg_register(plan, project_root=tmp_path)
    assert second["applied"][0]["status"] == "exists"
