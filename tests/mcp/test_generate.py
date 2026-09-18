from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.mcp.context import PathSandboxError
from det.mcp.generate import (
    infer_schema_from_records,
    mapper_from_diff_dry_run,
    schema_from_sample_dry_run,
)
from det.mcp.server import create_server
from det.runtime.meta import to_partition_value


def test_infer_schema_from_mixed_rows():
    result = infer_schema_from_records(
        [
            {"id": 1, "name": "a", "score": None},
            {"id": 2, "name": "b", "score": 1.5},
            {"id": 3, "name": "c"},  # score missing → not required
        ],
        title="demo",
    )
    schema = result.schema
    assert schema["$title"] == "demo"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"id", "name"}
    assert schema["properties"]["id"]["type"] == "integer"
    assert schema["properties"]["name"]["type"] == "string"
    score_type = schema["properties"]["score"]["type"]
    assert "number" in (score_type if isinstance(score_type, list) else [score_type])
    assert "null" in (score_type if isinstance(score_type, list) else [score_type])
    assert result.warnings == []


def test_infer_schema_deep_nested_object():
    result = infer_schema_from_records(
        [
            {
                "id": "ord_1",
                "customer": {
                    "email": "a@b.com",
                    "address": {"city": "Austin", "geo": {"lat": 30.2, "lon": -97.7}},
                },
            }
        ]
    )
    props = result.schema["properties"]
    geo = props["customer"]["properties"]["address"]["properties"]["geo"]
    assert geo["properties"]["lat"]["type"] == "number"
    assert geo["additionalProperties"] is False
    assert props["customer"]["additionalProperties"] is False


def test_infer_schema_array_of_objects():
    result = infer_schema_from_records(
        [
            {
                "id": "ord_1",
                "line_items": [
                    {"sku": "A", "qty": 1},
                    {"sku": "B", "qty": 2},
                ],
            }
        ]
    )
    items = result.schema["properties"]["line_items"]["items"]
    assert items["properties"]["sku"]["type"] == "string"
    assert items["properties"]["qty"]["type"] == "integer"
    assert items["additionalProperties"] is False


def test_infer_schema_strips_meta_keys():
    result = infer_schema_from_records(
        [
            {
                "id": 1,
                "__extract_run_datetime": "2026-01-01T00:00:00+00:00",
                "nested": {"x": 1, "__bronze_loaded_at": "nope"},
            }
        ]
    )
    props = result.schema["properties"]
    assert "__extract_run_datetime" not in props
    assert "id" in props
    assert "__bronze_loaded_at" not in props["nested"]["properties"]
    assert props["nested"]["properties"]["x"]["type"] == "integer"


def test_infer_schema_widens_int_string_with_warning():
    result = infer_schema_from_records(
        [
            {"id": 1, "qty": 1},
            {"id": 2, "qty": "2"},
        ]
    )
    assert result.schema["properties"]["qty"]["type"] == "string"
    assert any("qty" in w and "string" in w for w in result.warnings)


def test_normalize_scalar_strips_stale_structural_keywords():
    """Widen-to-string must not keep properties/items/additionalProperties."""
    from det.mcp.generate import _normalize_schema_node

    warnings: list[str] = []
    out = _normalize_schema_node(
        {
            "type": ["object", "string"],
            "properties": {"x": {"type": "integer"}},
            "required": ["x"],
            "additionalProperties": True,
            "items": {"type": "integer"},
        },
        path="payload",
        warnings=warnings,
    )
    assert out["type"] == "string"
    assert "properties" not in out
    assert "required" not in out
    assert "items" not in out
    assert "additionalProperties" not in out
    assert any("payload" in w for w in warnings)


def test_fold_anyof_object_scalar_widens_to_string():
    """anyOf object|string should widen, not leave complex anyOf intact."""
    from det.mcp.generate import _normalize_schema_node

    warnings: list[str] = []
    out = _normalize_schema_node(
        {
            "anyOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "properties": {"a": {"type": "integer"}},
                    "required": ["a"],
                },
            ]
        },
        path="payload",
        warnings=warnings,
    )
    assert out.get("type") == "string"
    assert "anyOf" not in out
    assert "properties" not in out
    assert any("structural+scalar" in w or "string" in w for w in warnings)


def test_fold_anyof_two_objects_left_intact():
    """Structure-only anyOf (two object shapes) stays for human review."""
    from det.mcp.generate import _normalize_schema_node

    warnings: list[str] = []
    node = {
        "anyOf": [
            {
                "type": "object",
                "properties": {"a": {"type": "integer"}},
            },
            {
                "type": "object",
                "properties": {"b": {"type": "string"}},
            },
        ]
    }
    out = _normalize_schema_node(node, path="payload", warnings=warnings)
    assert "anyOf" in out
    assert any("left anyOf intact" in w for w in warnings)


def test_schema_from_sample_inline_dry_run(tmp_path: Path):
    out = schema_from_sample_dry_run(
        records=[{"id": 1, "event_name": "x"}, {"id": 2, "event_name": "y"}],
        schema_out="schemas/demo/demo.schema.yaml",
        root=tmp_path,
    )
    assert out["dry_run"] is True
    assert out["rows_sampled"] == 2
    assert out["would_write"] == "schemas/demo/demo.schema.yaml"
    assert "id" in out["schema"]["properties"]
    assert out["warnings"] == []
    assert not (tmp_path / "schemas" / "demo" / "demo.schema.yaml").exists()
    assert "type: object" in out["yaml"]


def test_schema_from_sample_dry_run_includes_widen_warnings(tmp_path: Path):
    out = schema_from_sample_dry_run(
        records=[{"qty": 1}, {"qty": "2"}],
        schema_out="schemas/demo/mixed.schema.yaml",
        root=tmp_path,
    )
    assert out["warnings"]
    assert any("qty" in w for w in out["warnings"])
    assert "warnings" in out["note"].lower() or "widening" in out["note"].lower()


def _write_pipeline_and_raw(root: Path) -> Path:
    provider, source = "example_api", "events"
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True)
    schema_rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
    schema_path = root / schema_rel
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "integer"},
                    "event_name": {"type": "string"},
                },
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    (pipe_dir / f"{source}.yaml").write_text(
        yaml.safe_dump(
            {
                "name": f"{provider}.{source}",
                "source": {"type": f"{provider}.{source}"},
                "schema": schema_rel,
                "ingestion": {"library": "thin"},
                "destination": {"type": "filesystem", "path": str(root / "lake")},
            }
        ),
        encoding="utf-8",
    )
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    run = "2026-08-06T10:00:00+00:00"
    run_dir = (
        root
        / "lake"
        / "raw"
        / provider
        / source
        / f"__interval_start_datetime={to_partition_value(start)}"
        / f"__interval_end_datetime={to_partition_value(end)}"
        / f"__extract_run_datetime={to_partition_value(run)}"
    )
    page = run_dir / "data" / "pages" / "0001.json"
    page.parent.mkdir(parents=True)
    page.write_text(
        json.dumps({"data": {"events": [{"id": 1, "eventName": "alpha"}]}}),
        encoding="utf-8",
    )
    (run_dir / "meta").mkdir()
    (run_dir / "meta" / "manifest.json").write_text(
        json.dumps(
            {
                "source": f"{provider}.{source}",
                "artifacts": [
                    {
                        "path": page.relative_to(run_dir).as_posix(),
                        "format": "json_page",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def test_schema_from_sample_pipeline_dry_run(tmp_path: Path):
    run_dir = _write_pipeline_and_raw(tmp_path)
    out = schema_from_sample_dry_run(
        "example_api.events",
        run_path=str(run_dir.relative_to(tmp_path)),
        limit=10,
        root=tmp_path,
    )
    assert out["dry_run"] is True
    assert out["rows_sampled"] == 1
    assert "event_name" in out["schema"]["properties"]
    assert not (
        tmp_path / "schemas" / "example_api" / "events" / "events_inferred.schema.yaml"
    ).exists()


def test_mapper_from_diff_rename(tmp_path: Path):
    from_path = tmp_path / "schemas" / "old.yaml"
    to_path = tmp_path / "schemas" / "new.yaml"
    from_path.parent.mkdir(parents=True)
    from_path.write_text(
        yaml.safe_dump(
            {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "severity": {"type": "string"},
                },
            }
        ),
        encoding="utf-8",
    )
    to_path.write_text(
        yaml.safe_dump(
            {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "level": {"type": "string"},
                },
            }
        ),
        encoding="utf-8",
    )
    out = mapper_from_diff_dry_run(
        "schemas/old.yaml",
        "schemas/new.yaml",
        "example_api_v1_to_v2",
        root=tmp_path,
    )
    assert out["dry_run"] is True
    ops_by = {o["op"]: o for o in out["ops"]}
    assert "rename" in ops_by
    assert ops_by["rename"]["from"] == "severity"
    assert ops_by["rename"]["to"] == "level"
    assert "def example_api_v1_to_v2" in out["code"]
    assert "severity" in out["code"] and "level" in out["code"]
    assert "@mapper" in out["register_hint"]
    assert "register_mapper" not in out["register_hint"]
    assert "@mapper" in out["code"]
    assert not (tmp_path / "src").exists()


def test_mapper_from_diff_sandbox(tmp_path: Path):
    with pytest.raises(PathSandboxError):
        mapper_from_diff_dry_run(
            str(tmp_path.parent / "escape.yaml"),
            "schemas/x.yaml",
            "m",
            root=tmp_path,
        )


def test_create_server_registers_generate_tools():
    server = create_server()
    names = sorted(server._tool_manager._tools)
    assert "schema_from_sample_dry_run" in names
    assert "mapper_from_diff_dry_run" in names
