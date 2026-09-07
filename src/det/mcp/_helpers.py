"""Shared helpers for DET MCP tool entrypoints (tools, dry_run, ops_tools)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import generate as gen
from det.mcp import inspect as insp
from det.mcp.context import project_root
from det.mcp.reload import refresh_det_runtime
from det.runtime.lake import LakeRef
from det.runtime.lake import relpath as lake_relpath

DEFAULT_LIST_LIMIT = insp.DEFAULT_LIST_LIMIT
DEFAULT_SAMPLE_LIMIT = insp.DEFAULT_SAMPLE_LIMIT
MAX_SAMPLE_LIMIT = insp.MAX_SAMPLE_LIMIT


def prepare_tool() -> None:
    """Evict stale det.* modules so long-lived MCP sees disk edits."""
    import importlib

    import det.mcp.generate as generate_mod
    import det.mcp.inspect as inspect_mod

    refresh_det_runtime()
    # Re-bind inspect/generate so their imports of registry/plugins/runtime are fresh.
    global insp, gen
    insp = importlib.reload(inspect_mod)
    gen = importlib.reload(generate_mod)


def root(root_path: Path | None = None) -> Path:
    return root_path.resolve() if root_path is not None else project_root()


def approval_plan(command: str, argv: list[str]) -> dict[str, Any]:
    from det.runtime.approval import make_plan

    return make_plan(command, argv).to_dict()


def pipeline_path(pipeline: str, project: Path) -> Path:
    """Resolve a pipeline name (``noaa.storm_events``), path, or nested stem."""
    from det.runtime.pipelines import resolve_pipeline_ref

    return resolve_pipeline_ref(pipeline, project_root=project).path


def canonical_id(pipeline: str, project: Path) -> str:
    """Resolved pipeline identity for approval plans.

    Approval digests must be built from the same identity the CLI uses, so both
    surfaces go through ``resolve_pipeline_ref`` rather than reading ``name:``
    from the config (which is not validated against the file's location).
    """
    from det.runtime.pipelines import resolve_pipeline_ref

    return resolve_pipeline_ref(pipeline, project_root=project).canonical_id


def require_catchup_scope(*, pipeline: str | None, all_pipelines: bool) -> None:
    """Require exactly one of ``pipeline`` or ``all_pipelines=True`` (CLI parity)."""
    if all_pipelines == (pipeline is not None):
        raise ValueError("exactly one of pipeline / all_pipelines=True is required")


def load_pipeline(pipeline: str, project: Path):
    from det.runtime.config import load_pipeline_config
    from det.runtime.pipelines import resolve_pipeline_ref

    resolved = resolve_pipeline_ref(pipeline, project_root=project)
    return load_pipeline_config(resolved.path), resolved.path


def rel(path: Path | LakeRef, project: Path) -> str:
    return lake_relpath(path, project)
