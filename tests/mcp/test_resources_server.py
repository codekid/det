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
        "init_pipeline_dry_run",
    ):
        assert expected in names
