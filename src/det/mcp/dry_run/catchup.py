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
    from det.runtime.approval import silver_catchup_plan_write_argv
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
    return {
        **planned,
        "approval_plan": h.approval_plan(
            "silver-catchup-plan",
            silver_catchup_plan_write_argv(
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
            ),
        ),
        "next_steps": (
            "Show approval_plan, then STOP — do not apply in this turn. "
            "After operator confirm: det approve --plan <approval_plan> "
            "--approved-by <id>. Later turn only: det silver-catchup apply "
            f"--manifest-id {mid} --content-digest {digest} --approval <id> "
            "(or det silver-catchup-plan --apply …). Apply and build are "
            "separate approvals: after apply succeeds, MCP "
            f"dbt_dry_run(catchup=True, catchup_manifest={mid}) → show that "
            "approval_plan, STOP again; separate approve; later turn: "
            f"det silver-catchup build --manifest-id {mid} --approval <dbt_id> "
            f"(or det dbt --catchup --catchup-manifest {mid} --approval <dbt_id>)."
        ),
    }


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
        "approval_plan": h.approval_plan("silver-catchup-cleanup", write_argv),
        "next_steps": (
            "Show approval_plan, then STOP — do not cleanup --apply in this turn. "
            "After operator confirm: det approve --plan <approval_plan> "
            "--approved-by <id>. Later turn only: det silver-catchup cleanup "
            f"--apply {apply_hint} --approval <id>. Heal does not auto-drop "
            "these tables."
        ),
    }
