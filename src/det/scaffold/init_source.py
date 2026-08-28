"""Scaffold a project-local source plugin (+ optional pipeline YAML / schema)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent

from det.logging import get_logger
from det.runtime.discovery import in_tree_source_map, project_sources_dir
from det.runtime.ids import parse_canonical_id, validate_canonical_id
from det.scaffold.dbt import ScaffoldAction
from det.scaffold.init_pipeline import InitPipelineResult, init_pipeline

logger = get_logger(__name__)


@dataclass
class InitSourceResult:
    name: str
    plugin_path: Path
    actions: list[ScaffoldAction] = field(default_factory=list)
    pipeline: InitPipelineResult | None = None


_PLUGIN_TEMPLATE = '''\
"""Project-local DET source plugin: {name}.

Edit extract_to_raw / records_from_raw for your API. Defaults use fixture_records
so `det run` works offline until you wire HTTP.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from det.runtime.lake import LakeRef
from det.sources.base import Interval, SourceRow
from det.sources.http_json import dig, nest_under_path, write_json_page


class {class_name}:
    """Scaffolded source — replace fixture_records with a real fetch when ready."""

    name = "{name}"

    def defaults(self) -> dict[str, Any]:
        return {{
            "record_path": "data.records",
            # Public by default (no credential lookup). Set auth_env to an env name
            # when the API needs a bearer token.
            "auth_env": None,
            "fixture_records": [{{"id": 1, "payload": "hello from {name}"}}],
        }}

    def extract_to_raw(
        self,
        *,
        config: dict[str, Any],
        interval: Interval,
        data_dir: Path | LakeRef,
    ) -> list[dict[str, Any]]:
        pages_dir = data_dir / "pages"
        pages_dir.mkdir(parents=True, exist_ok=True)
        record_path = config.get("record_path") or "data.records"
        fixtures = config.get("fixture_records")
        if fixtures is None:
            raise ValueError(
                f"{{self.name}}: set source.overrides.fixture_records "
                "or implement HTTP fetch"
            )
        return [
            write_json_page(
                pages_dir=pages_dir,
                data_dir=data_dir,
                page_num=1,
                body=nest_under_path(list(fixtures), record_path=record_path),
                origin="fixture_records",
            )
        ]

    def records_from_raw(
        self,
        *,
        config: dict[str, Any],
        raw_dir: Path | LakeRef,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        record_path = config.get("record_path") or "data.records"
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = dig(payload, record_path)
            if rows is None and isinstance(payload, list):
                rows = payload
            if not isinstance(rows, list):
                raise ValueError(f"No record list at {{record_path!r}} in {{path}}")
            for row in rows:
                if isinstance(row, dict):
                    yield SourceRow(data=dict(row), filename=Path(art["path"]).name)
'''


def _class_name(provider: str, source: str) -> str:
    def part(text: str) -> str:
        return "".join(p.title() for p in text.replace("-", "_").split("_") if p)

    return f"{part(provider)}{part(source)}Source"


def init_source(
    *,
    name: str,
    project_root: Path,
    force: bool = False,
    dry_run: bool = False,
    skip_pipeline: bool = False,
    skip_dbt: bool = False,
    destination_type: str = "iceberg",
    lake_path: str | None = None,
    connection: str | None = None,
) -> InitSourceResult:
    """
    Write ``sources/<provider>/<source>.py`` and optionally pipeline YAML + schema.

    After the plugin file exists, discovery picks it up under ``project_root``.
    """
    name = validate_canonical_id(name)
    root = project_root.resolve()
    provider, source = parse_canonical_id(name)
    plugin_path = project_sources_dir(root) / provider / f"{source}.py"
    actions: list[ScaffoldAction] = []

    tree = in_tree_source_map()
    if name in tree:
        raise ValueError(
            f"source id {name!r} collides with in-tree plugin {tree[name]}; "
            "choose another provider.source name"
        )

    body = dedent(
        _PLUGIN_TEMPLATE.format(name=name, class_name=_class_name(provider, source))
    )
    exists = plugin_path.exists()
    if exists and not force:
        actions.append(
            ScaffoldAction(path=plugin_path, action="skip", detail="plugin exists")
        )
    elif dry_run:
        actions.append(
            ScaffoldAction(
                path=plugin_path,
                action="would_write",
                detail="overwrite" if exists else "create",
            )
        )
    else:
        sources_root = project_sources_dir(root)
        sources_root.mkdir(parents=True, exist_ok=True)
        gitkeep = sources_root / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.write_text("", encoding="utf-8")
        plugin_path.parent.mkdir(parents=True, exist_ok=True)
        plugin_path.write_text(body, encoding="utf-8")
        actions.append(
            ScaffoldAction(
                path=plugin_path,
                action="write",
                detail="overwrite" if exists else "create",
            )
        )
        logger.info("init-source wrote plugin", path=str(plugin_path), name=name)
        # Drop a previously imported project module so force-overwrite reloads.
        import sys

        from det.runtime.discovery import _project_module_name
        from det.runtime.registry import _SOURCE_REGISTRY, _root_key

        sys.modules.pop(_project_module_name(name), None)
        _SOURCE_REGISTRY.pop((name, _root_key(root)), None)

    pipeline_result = None
    if not skip_pipeline:
        if dry_run and not plugin_path.exists():
            actions.append(
                ScaffoldAction(
                    path=root / "configs" / "pipelines",
                    action="would_write",
                    detail="init-pipeline (after plugin)",
                )
            )
        else:
            pipeline_result = init_pipeline(
                name=name,
                source_type=name,
                project_root=root,
                force=force,
                dry_run=dry_run,
                skip_dbt=skip_dbt,
                destination_type=destination_type,
                lake_path=lake_path,
                connection=connection,
            )
            actions.extend(pipeline_result.actions)

    return InitSourceResult(
        name=name,
        plugin_path=plugin_path,
        actions=actions,
        pipeline=pipeline_result,
    )
