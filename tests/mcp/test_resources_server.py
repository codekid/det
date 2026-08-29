from __future__ import annotations

from pathlib import Path

import yaml

from det.mcp.resources import pipeline_yaml, readme_pointer, schema_yaml_nested
from det.mcp.server import create_server


def test_resources_read_pipeline_and_schema(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    (tmp_path / "configs" / "pipelines").mkdir(parents=True)
    (tmp_path / "schemas" / "demo").mkdir(parents=True)
    (tmp_path / "configs" / "pipelines" / "demo.yaml").write_text(
        "name: demo\n", encoding="utf-8"
    )
    (tmp_path / "schemas" / "demo" / "demo.schema.yaml").write_text(
        yaml.safe_dump({"type": "object"}),
        encoding="utf-8",
    )
    assert "name: demo" in pipeline_yaml("demo")
    assert "type: object" in schema_yaml_nested("demo", "demo.schema.yaml")
    assert "det://" in readme_pointer()


def test_create_server_registers_tools():
    server = create_server()
    # FastMCP keeps tools on _tool_manager
    names = sorted(server._tool_manager._tools)
    for expected in (
        "list_pipelines",
        "describe_pipeline",
        "prune_dry_run",
        "dbt_dry_run",
        "scaffold_dbt_dry_run",
        "scaffold_ops_dry_run",
        "init_pipeline_dry_run",
        "diff_partitions",
        "sample_raw",
        "validate_sample",
        "sample_bronze",
        "diagnose_pipeline",
        "schema_from_sample_dry_run",
        "mapper_from_diff_dry_run",
        "airflow_health",
        "list_airflow_dags",
        "list_airflow_dag_runs",
        "describe_airflow_det_env",
        "preview_backfill_conf",
        "migrate_dry_run",
        "list_runs",
        "summarize_runs",
        "check",
        "list_models",
        "describe_model",
        "query_analytics",
        "cube_meta",
        "cube_load",
        "list_approvals",
        "describe_approval",
    ):
        assert expected in names


def test_create_server_registers_skill_prompts():
    server = create_server()
    names = server._prompt_manager._prompts
    for expected in (
        "det_ops",
        "det_new_source",
        "det_migrate",
        "det_dbt",
        "det_airflow",
    ):
        assert expected in names


def _schema_types(prop: dict) -> set[str]:
    types: set[str] = set()
    raw = prop.get("type")
    if isinstance(raw, str):
        types.add(raw)
    elif isinstance(raw, list):
        types.update(raw)
    for alt in prop.get("anyOf") or prop.get("oneOf") or []:
        alt_type = alt.get("type")
        if isinstance(alt_type, str):
            types.add(alt_type)
        elif isinstance(alt_type, list):
            types.update(alt_type)
    return types


def _param_description(prop: dict) -> str:
    desc = prop.get("description")
    if isinstance(desc, str) and desc.strip():
        return desc
    for alt in prop.get("anyOf") or prop.get("oneOf") or []:
        alt_desc = alt.get("description")
        if (
            isinstance(alt_desc, str)
            and alt_desc.strip()
            and alt.get("type") in {"string", "integer"}
        ):
            return alt_desc
    return ""


def test_tool_string_int_params_have_descriptions():
    server = create_server()
    missing: list[str] = []
    for name, tool in server._tool_manager._tools.items():
        props = (tool.parameters or {}).get("properties") or {}
        for pname, prop in props.items():
            types = _schema_types(prop)
            if not types.intersection({"string", "integer"}):
                continue
            if not _param_description(prop).strip():
                missing.append(f"{name}.{pname}")
    assert missing == []
