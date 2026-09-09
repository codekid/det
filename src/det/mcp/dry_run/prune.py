"""MCP prune dry-run."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import _helpers as h


def prune_dry_run(
    pipeline: str,
    *,
    interval_start: str,
    interval_end: str | None = None,
    keep: int = 1,
    root: Path | None = None,
) -> dict[str, Any]:
    h.prepare_tool()
    from det.runtime.prune import BronzePruner

    base = h.root(root)
    config, _ = h.load_pipeline(pipeline, base)
    plan = BronzePruner(base).plan(
        config,
        interval_start=interval_start,
        interval_end=interval_end,
        keep=keep,
    )
    from det.runtime.approval import prune_write_argv

    pipe_id = h.canonical_id(pipeline, base)
    return {
        "pipeline": pipe_id,
        "keep": keep,
        "approval_plan": h.approval_plan(
            "prune",
            prune_write_argv(
                pipe_id,
                interval_start,
                interval_end=interval_end,
                keep=keep,
                **h.approval_lake_kwargs(project_root=base),
            ),
        ),
        "remove_count": plan.remove_count,
        "to_remove": [
            {
                "interval_start": r.interval_start,
                "interval_end": r.interval_end,
                "extract_run_datetime": r.extract_run_datetime,
                "path": h.rel(r.path, base) if r.path is not None else None,
            }
            for r in plan.to_remove
        ],
        "to_keep": [
            {
                "interval_start": r.interval_start,
                "interval_end": r.interval_end,
                "extract_run_datetime": r.extract_run_datetime,
                "path": h.rel(r.path, base) if r.path is not None else None,
            }
            for r in plan.to_keep
        ],
    }
