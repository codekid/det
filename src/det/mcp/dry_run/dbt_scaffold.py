"""MCP dbt / scaffold / init-pipeline dry-runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import _helpers as h


def dbt_dry_run(
    pipeline: str | None = None,
    *,
    command: str = "build",
    select: list[str] | None = None,
    catchup: bool = False,
    catchup_manifest: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    h.prepare_tool()
    from det.runtime.dbt_runner import analytics_exclude, run_dbt

    base = h.root(root)
    pipeline_arg: Path | str | None = None
    if pipeline is not None:
        pipeline_arg = h.pipeline_path(pipeline, base)
    if catchup and not (catchup_manifest and str(catchup_manifest).strip()):
        raise ValueError("catchup requires catchup_manifest (scm_… id)")
    if catchup_manifest and str(catchup_manifest).strip() and not catchup:
        raise ValueError("catchup_manifest requires catchup=true")
    result = run_dbt(
        project_root=base,
        command=command,  # type: ignore[arg-type]
        select=select,
        exclude=analytics_exclude(select),
        pipeline=pipeline_arg,
        catchup=catchup,
        catchup_manifest=catchup_manifest,
        dry_run=True,
    )
    from det.runtime.approval import dbt_write_argv

    mid = str(catchup_manifest).strip() if catchup_manifest else ""
    out: dict[str, Any] = {
        "dry_run": True,
        "command": result.command,
        "select": list(result.select),
        "project_dir": h.rel(result.project_dir, base),
        "lake_path": result.lake_path,
        "bronze_source": result.bronze_source,
        "catchup": catchup,
        "catchup_manifest": catchup_manifest,
        "approval_plan": h.approval_plan(
            "dbt",
            dbt_write_argv(
                h.canonical_id(pipeline, base) if pipeline is not None else None,
                command=command,
                select=select,
                catchup=catchup,
                catchup_manifest=catchup_manifest,
                **h.approval_lake_kwargs(project_root=base),
            ),
        ),
    }
    if catchup:
        cmd = str(result.command).strip() or "build"
        dbt_hint = (
            f"det dbt --command {cmd} --catchup --catchup-manifest {mid} "
            "--approval <id>"
        )
        if cmd == "build":
            write_hint = (
                f"det silver-catchup build --manifest-id {mid} --approval <id> "
                f"(or {dbt_hint})"
            )
        else:
            write_hint = dbt_hint
        out["ladder_rung"] = "build"
        out["next_rung"] = "build"
        out["next_steps"] = (
            "Show approval_plan, then STOP — do not build/dbt write in this turn. "
            "After operator confirm: det approve --plan <approval_plan> "
            f"--approved-by <id>. Later turn only: {write_hint}."
        )
    return out


def scaffold_dbt_dry_run(
    pipeline: str,
    *,
    force: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    h.prepare_tool()
    from det.scaffold.dbt import scaffold_dbt

    base = h.root(root)
    config, _ = h.load_pipeline(pipeline, base)
    from det.runtime.approval import scaffold_dbt_write_argv

    result = scaffold_dbt(config, project_root=base, force=force, dry_run=True)
    return {
        "dry_run": True,
        "dataset": result.dataset,
        "approval_plan": h.approval_plan(
            "scaffold-dbt",
            scaffold_dbt_write_argv(h.canonical_id(pipeline, base), force=force),
        ),
        "actions": [
            {
                "action": a.action,
                "path": h.rel(a.path, base),
                "detail": a.detail,
            }
            for a in result.actions
        ],
    }


def scaffold_ops_dry_run(
    *,
    force: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview scaffold-ops file actions without writing."""
    h.prepare_tool()
    from det.runtime.approval import scaffold_ops_write_argv
    from det.scaffold.ops import scaffold_ops

    base = h.root(root)
    result = scaffold_ops(project_root=base, force=force, dry_run=True)
    return {
        "dry_run": True,
        "dataset": result.dataset,
        "approval_plan": h.approval_plan(
            "scaffold-ops",
            scaffold_ops_write_argv(force=force),
        ),
        "actions": [
            {
                "action": a.action,
                "path": h.rel(a.path, base),
                "detail": a.detail,
            }
            for a in result.actions
        ],
    }


def init_pipeline_dry_run(
    name: str,
    source_type: str,
    *,
    destination_type: str = "iceberg",
    connection: str | None = None,
    lake_path: str | None = None,
    skip_dbt: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    h.prepare_tool()
    from det.scaffold.init_pipeline import init_pipeline

    base = h.root(root)
    result = init_pipeline(
        name=name,
        source_type=source_type,
        project_root=base,
        dry_run=True,
        skip_dbt=skip_dbt,
        destination_type=destination_type,
        lake_path=lake_path,
        connection=connection,
    )
    from det.runtime.approval import init_pipeline_write_argv

    return {
        "dry_run": True,
        "name": result.name,
        "pipeline_path": h.rel(result.pipeline_path, base),
        "schema_path": h.rel(result.schema_path, base),
        "approval_plan": h.approval_plan(
            "init-pipeline",
            init_pipeline_write_argv(
                name,
                source_type,
                destination_type=destination_type,
                connection=connection,
                lake_path=lake_path,
                skip_dbt=skip_dbt,
            ),
        ),
        "actions": [
            {
                "action": a.action,
                "path": h.rel(a.path, base),
                "detail": a.detail,
            }
            for a in result.actions
        ],
    }
