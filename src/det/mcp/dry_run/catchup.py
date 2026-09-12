"""MCP silver catch-up dry-runs and bronze/silver diff."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import _helpers as h


def _effective_lookback(
    *,
    interval_start: str | None,
    interval_end: str | None,
    extract_lookback: str | None,
    census: bool,
) -> tuple[str | None, bool]:
    """Return (effective_lookback, census_for_argv)."""
    from det.runtime.silver_catchup import resolve_catchup_candidate_scope

    effective = resolve_catchup_candidate_scope(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
    )
    census_argv = bool(census) and effective is None and interval_start is None
    return effective, census_argv


def diff_bronze_silver(
    pipeline: str | None = None,
    *,
    all_pipelines: bool = False,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_lookback: str | None = None,
    census: bool = False,
    limit: int = h.DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Latest bronze extract-run per interval vs silver coverage (read-only)."""
    h.prepare_tool()
    from det.runtime.silver_catchup import diff_bronze_silver as _diff
    from det.runtime.silver_catchup import diff_bronze_silver_fleet

    h.require_catchup_scope(pipeline=pipeline, all_pipelines=all_pipelines)
    effective, _census = _effective_lookback(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
    )
    base = h.root(root)
    if all_pipelines:
        return diff_bronze_silver_fleet(
            project_root=base,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=effective,
            limit=limit,
        )
    return _diff(
        pipeline,  # type: ignore[arg-type]
        project_root=base,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=effective,
        limit=limit,
    )


def silver_catchup_dry_run(
    pipeline: str | None = None,
    *,
    all_pipelines: bool = False,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_lookback: str | None = None,
    census: bool = False,
    limit: int = h.DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview immutable ops/silver_catchup/<id>.json + approval_plan (never writes)."""
    h.prepare_tool()
    from det.runtime.approval import (
        silver_catchup_apply_cli_hint,
        silver_catchup_plan_write_argv,
    )
    from det.runtime.silver_catchup import plan_catchup_manifest

    h.require_catchup_scope(pipeline=pipeline, all_pipelines=all_pipelines)
    effective, census_argv = _effective_lookback(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
    )
    base = h.root(root)
    pipe_id = h.canonical_id(pipeline, base) if pipeline is not None else None
    planned = plan_catchup_manifest(
        project_root=base,
        pipeline=pipe_id,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=effective,
        limit=limit,
    )
    mid = str(planned["manifest_id"])
    digest = str(planned["content_digest"])
    write_argv = silver_catchup_plan_write_argv(
        pipeline=pipe_id,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=effective,
        census=census_argv,
        limit=limit,
        manifest_id=mid,
        content_digest=digest,
        **h.approval_lake_kwargs(project_root=base),
    )
    apply_cli = silver_catchup_apply_cli_hint(write_argv)
    return {
        **planned,
        "ladder_rung": "apply",
        "next_rung": "apply",
        "approval_plan": h.approval_plan("silver-catchup-plan", write_argv),
        "next_steps": (
            "Show approval_plan, then STOP — do not apply in this turn. "
            "After operator confirm: det approve --plan <approval_plan> "
            f"--approved-by <id>. Later turn only: {apply_cli} "
            "(or det silver-catchup-plan --apply …). After apply succeeds, "
            "start the build rung with a separate "
            f"dbt_dry_run(catchup=True, catchup_manifest={mid})."
        ),
    }


def _mode_a_heal_preview(
    pipeline: str,
    *,
    extract_lookback: str | None = None,
    limit: int = h.DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Build Mode A heal preview payload (no MCP prepare_tool)."""
    from det.runtime.approval import (
        silver_catchup_apply_cli_hint,
        silver_catchup_plan_write_argv,
    )
    from det.runtime.silver_catchup import plan_catchup_manifest

    if not (pipeline and str(pipeline).strip()):
        raise ValueError("Mode A heal requires pipeline")
    effective, _census = _effective_lookback(
        interval_start=None,
        interval_end=None,
        extract_lookback=extract_lookback,
        census=False,
    )
    base = h.root(root)
    pipe_id = h.canonical_id(pipeline, base)
    planned = plan_catchup_manifest(
        project_root=base,
        pipeline=pipe_id,
        all_pipelines=False,
        interval_start=None,
        interval_end=None,
        extract_lookback=effective,
        limit=limit,
    )
    raw_diff = planned.get("diff")
    diff: dict[str, Any] = raw_diff if isinstance(raw_diff, dict) else {}
    catchup_count = int(diff.get("catchup_count") or 0)
    runs = list(diff.get("catchup_runs") or [])
    mid = str(planned["manifest_id"])
    digest = str(planned["content_digest"])
    lookback = planned.get("extract_lookback") or effective or "48h"
    base_out: dict[str, Any] = {
        "dry_run": True,
        "route": "mode_a",
        "boring": True,
        "pipeline": pipe_id,
        "extract_lookback": lookback,
        "catchup_count": catchup_count,
        "catchup_runs": runs[: min(10, limit)],
        "candidate_mode": planned.get("candidate_mode"),
        "nothing_to_do": catchup_count == 0,
    }
    if catchup_count == 0:
        return {
            **base_out,
            "ladder_rung": "apply",
            "next_rung": None,
            "next_steps": (
                "Mode A heal: nothing to do (catchup_count=0). "
                "Verify with det silver-catchup verify -p "
                f"{pipe_id}, or use Advanced census if you need a full audit."
            ),
        }
    write_argv = silver_catchup_plan_write_argv(
        pipeline=pipe_id,
        all_pipelines=False,
        interval_start=None,
        interval_end=None,
        extract_lookback=effective,
        census=False,
        limit=limit,
        manifest_id=mid,
        content_digest=digest,
        **h.approval_lake_kwargs(project_root=base),
    )
    apply_cli = silver_catchup_apply_cli_hint(write_argv)
    return {
        **base_out,
        "manifest_id": mid,
        "content_digest": digest,
        "manifest_relpath": planned.get("manifest_relpath"),
        "ladder_rung": "apply",
        "next_rung": "apply",
        "approval_plan": h.approval_plan("silver-catchup-plan", write_argv),
        "next_steps": (
            "Mode A heal (boring route). Show approval_plan, then STOP — "
            "do not apply in this turn. After operator confirm: det approve "
            f"--plan <approval_plan> --approved-by <id>. Later turn only: "
            f"{apply_cli}."
        ),
        "after_apply": (
            f"After apply succeeds: det silver-catchup heal --continue "
            f"--manifest-id {mid} (or MCP dbt_dry_run catchup=True, "
            f"catchup_manifest={mid}) → STOP → separate approve → build."
        ),
    }


def silver_catchup_heal_dry_run(
    pipeline: str,
    *,
    extract_lookback: str | None = None,
    limit: int = h.DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Mode A boring-route preview: holes + apply approval_plan (never writes).

    Rejects census / fleet / interval windows — those stay on
    ``silver_catchup_dry_run`` (Advanced). Default lookback is 48h when omitted.
    """
    h.prepare_tool()
    return _mode_a_heal_preview(
        pipeline,
        extract_lookback=extract_lookback,
        limit=limit,
        root=root,
    )


def silver_catchup_cleanup_dry_run(
    *,
    manifest_id: str | None = None,
    older_than: str | None = None,
) -> dict[str, Any]:
    """Preview BQ ``_det_catchup_runs_*`` drops + approval_plan (never writes)."""
    h.prepare_tool()
    from det.runtime.approval import silver_catchup_cleanup_write_argv
    from det.runtime.silver_catchup import plan_bq_catchup_cleanup

    mid = str(manifest_id).strip() if manifest_id else ""
    older = str(older_than).strip() if older_than else ""
    planned = plan_bq_catchup_cleanup(
        manifest_id=mid or None,
        older_than=older or None,
    )
    before = planned.get("created_before")
    if mid:
        write_argv = silver_catchup_cleanup_write_argv(manifest_id=mid)
        apply_hint = f"--manifest-id {mid}"
    else:
        before_s = str(before).strip() if before is not None else ""
        if not before_s:
            raise ValueError(
                "silver_catchup_cleanup_dry_run requires planned created_before "
                "when not using manifest_id (immutable cutoff for approval)"
            )
        write_argv = silver_catchup_cleanup_write_argv(created_before=before_s)
        apply_hint = f"--created-before {before_s}"
    return {
        **planned,
        "dry_run": True,
        "ladder_rung": "cleanup",
        "next_rung": "cleanup",
        "approval_plan": h.approval_plan("silver-catchup-cleanup", write_argv),
        "next_steps": (
            "Show approval_plan, then STOP — do not cleanup --apply in this turn. "
            "After operator confirm: det approve --plan <approval_plan> "
            "--approved-by <id>. Later turn only: det silver-catchup cleanup "
            f"--apply {apply_hint} --approval <id>. Heal does not auto-drop "
            "these tables."
        ),
    }
