"""Project-local sources/ discovery and init-source scaffold."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.runtime.discovery import (
    PluginLoadError,
    discovered_source_ids,
    project_source_map,
)
from det.runtime.registry import clear_registries, get_source, list_sources
from det.runtime.runner import PipelineRunner
from det.scaffold.init_source import init_source


def _write_project_plugin(root: Path, plugin_id: str = "acme.widgets") -> Path:
    provider, source = plugin_id.split(".", 1)
    path = root / "sources" / provider / f"{source}.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    class_name = "AcmeWidgetsSource"
    path.write_text(
        f'''\
from pathlib import Path
from typing import Any
from collections.abc import Iterator
from det.runtime.lake import LakeRef
from det.sources.base import Interval, SourceRow
from det.sources.http_json import dig, nest_under_path, write_json_page
import json

class {class_name}:
    name = "{plugin_id}"

    def defaults(self) -> dict[str, Any]:
        return {{
            "record_path": "data.records",
            "auth_env": None,
            "fixture_records": [{{"id": 1, "payload": "x"}}],
        }}

    def extract_to_raw(self, *, config, interval, data_dir):
        pages_dir = data_dir / "pages"
        pages_dir.mkdir(parents=True, exist_ok=True)
        record_path = config.get("record_path") or "data.records"
        return [
            write_json_page(
                pages_dir=pages_dir,
                data_dir=data_dir,
                page_num=1,
                body=nest_under_path(list(config["fixture_records"]), record_path=record_path),
                origin="fixture_records",
            )
        ]

    def records_from_raw(self, *, config, raw_dir, manifest) -> Iterator[SourceRow]:
        record_path = config.get("record_path") or "data.records"
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = dig(payload, record_path) or []
            for row in rows:
                if isinstance(row, dict):
                    yield SourceRow(data=dict(row), filename=Path(art["path"]).name)
''',
        encoding="utf-8",
    )
    return path


def test_project_source_discovered(tmp_path: Path) -> None:
    _write_project_plugin(tmp_path)
    clear_registries()
    assert "acme.widgets" in discovered_source_ids(project_root=tmp_path)
    assert project_source_map(tmp_path)["acme.widgets"].name == "widgets.py"
    plugin = get_source("acme.widgets", project_root=tmp_path)
    assert plugin.name == "acme.widgets"


def test_project_source_collides_with_in_tree(tmp_path: Path) -> None:
    _write_project_plugin(tmp_path, "noaa.storm_events")
    clear_registries()
    with pytest.raises(PluginLoadError, match="collides with in-tree"):
        discovered_source_ids(project_root=tmp_path)


def test_init_source_writes_plugin_and_pipeline(tmp_path: Path) -> None:
    clear_registries()
    result = init_source(
        name="acme.widgets",
        project_root=tmp_path,
        skip_dbt=True,
        destination_type="filesystem",
    )
    assert result.plugin_path.is_file()
    assert "acme.widgets" in list_sources(project_root=tmp_path)
    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    assert pipe.is_file()
    doc = yaml.safe_load(pipe.read_text(encoding="utf-8"))
    assert doc["source"]["type"] == "acme.widgets"
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    assert schema.is_file()


def test_init_source_refuses_in_tree_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="collides with in-tree"):
        init_source(name="example_api.events", project_root=tmp_path, skip_pipeline=True)


def test_runner_uses_project_source(tmp_path: Path) -> None:
    clear_registries()
    init_source(
        name="acme.widgets",
        project_root=tmp_path,
        skip_dbt=True,
        destination_type="filesystem",
    )
    runner = PipelineRunner(tmp_path)
    result = runner.run(
        "configs/pipelines/acme/widgets.yaml",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == 1
