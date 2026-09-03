"""CLI: bronze↔silver catch-up diff and manifest plan/apply."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _APPROVAL_HELP,
    _LAKE_PATH_BRONZE_HELP,
    _LAKE_PATH_HELP,
    _LAKE_PATH_OPS_HELP,
    _LAKE_PATH_RAW_HELP,
    _PIPELINE_HELP,
    _PROJECT_ROOT_HELP,
    _REQUIRE_APPROVAL_HELP,
    _claimed_approval_work,
    _consume_approval,
    _gate_approval,
    _project_root,
    _resolve_interval,
    _resolve_pipeline,
    _settings,
)


@app.command("silver-catchup-diff")
def silver_catchup_diff_cmd(
    pipeline: str | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help=f"Single pipeline. {_PIPELINE_HELP}",
    ),
    all_pipelines: bool = typer.Option(
        False,
        "--all-pipelines",
        help="Diff every pipeline under configs/pipelines/",
    ),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    limit: int = typer.Option(200, "--limit", help="Max runs listed per side (cap 200)"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(
        None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP
    ),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(
        None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Compare latest bronze extract-run per interval to silver coverage (read-only)."""
    from det.runtime.settings import use_settings
    from det.runtime.silver_catchup import diff_bronze_silver, diff_bronze_silver_fleet

    if all_pipelines == (pipeline is not None):
        raise typer.BadParameter(
            "exactly one of --pipeline / --all-pipelines is required",
            param_hint="--pipeline/--all-pipelines",
        )

    root = _project_root(project_root)
    settings = _settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
    start = end = None
    if interval_start is not None:
        start, end = _resolve_interval(interval_start, interval_end)

    with use_settings(settings):
        if all_pipelines:
            payload = diff_bronze_silver_fleet(
                project_root=root,
                interval_start=start,
                interval_end=end,
                limit=limit,
            )
        else:
            # Exactly-one-of --pipeline / --all-pipelines is enforced above.
            if pipeline is None:
                raise typer.BadParameter(
                    "--pipeline is required unless --all-pipelines is set",
                    param_hint="--pipeline",
                )
            resolved = _resolve_pipeline(pipeline, root)
            payload = diff_bronze_silver(
                resolved.canonical_id,
                project_root=root,
                interval_start=start,
                interval_end=end,
                limit=limit,
            )

    if as_json:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return

    catchup = payload.get("catchup_runs") or []
    if "results" in payload:
        ok_n = sum(int(r.get("ok_count") or 0) for r in payload["results"])
        stale_n = sum(int(r.get("stale_siblings_count") or 0) for r in payload["results"])
    else:
        ok_n = int(payload.get("ok_count") or 0)
        stale_n = int(payload.get("stale_siblings_count") or 0)
    typer.echo(
        f"catchup_count={payload.get('catchup_count', len(catchup))} "
        f"ok_count={ok_n} stale_siblings={stale_n}"
    )
    for row in catchup:
        typer.echo(
            f"  catchup pipeline={row.get('pipeline')} "
            f"interval={row.get('interval_start')}..{row.get('interval_end')} "
            f"run={row.get('extract_run_datetime')}"
        )
    note = payload.get("note")
    if note:
        typer.echo(f"note: {note}", err=True)


@app.command("silver-catchup-plan")
def silver_catchup_plan_cmd(
    ctx: typer.Context,
    pipeline: str | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help=f"Single pipeline. {_PIPELINE_HELP}",
    ),
    all_pipelines: bool = typer.Option(
        False,
        "--all-pipelines",
        help="Plan catch-up for every pipeline under configs/pipelines/",
    ),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    limit: int = typer.Option(200, "--limit", help="Max runs considered per pipeline"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview manifest only"),
    apply: bool = typer.Option(False, "--apply", help="Write ops/silver_catchup/manifest.json"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(
        None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP
    ),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(
        None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(
        False, "--require-approval", help=_REQUIRE_APPROVAL_HELP
    ),
) -> None:
    """Build or apply the silver catch-up manifest under ops/silver_catchup/."""
    from det.runtime.approval import silver_catchup_plan_write_argv
    from det.runtime.settings import use_settings
    from det.runtime.silver_catchup import (
        manifest_relpath_for_root,
        plan_catchup_manifest,
        write_catchup_manifest,
    )

    if dry_run == apply:
        raise typer.BadParameter(
            "exactly one of --dry-run or --apply is required",
            param_hint="--dry-run/--apply",
        )
    if all_pipelines == (pipeline is not None):
        raise typer.BadParameter(
            "exactly one of --pipeline / --all-pipelines is required",
            param_hint="--pipeline/--all-pipelines",
        )

    root = _project_root(project_root)
    settings = _settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
    start = end = None
    if interval_start is not None:
        start, end = _resolve_interval(interval_start, interval_end)

    pipe_id: str | None = None
    if pipeline is not None:
        pipe_id = _resolve_pipeline(pipeline, root).canonical_id

    claimed = False
    if apply:
        claimed = _gate_approval(
            root,
            "silver-catchup-plan",
            silver_catchup_plan_write_argv(
                pipeline=pipe_id,
                all_pipelines=all_pipelines,
                interval_start=start,
                interval_end=end,
                limit=limit,
                lake_path=lake_path,
                lake_path_raw=lake_path_raw,
                lake_path_bronze=lake_path_bronze,
                lake_path_ops=lake_path_ops,
            ),
            approval,
            require_approval,
            ctx=ctx,
        )

    with use_settings(settings):
        planned = plan_catchup_manifest(
            project_root=root,
            pipeline=pipe_id,
            all_pipelines=all_pipelines,
            interval_start=start,
            interval_end=end,
            limit=limit,
        )
        if dry_run:
            if as_json:
                typer.echo(json.dumps(planned, indent=2, default=str))
            else:
                runs = planned["manifest"].get("runs") or []
                typer.echo(
                    f"DRY-RUN silver-catchup-plan runs={len(runs)} "
                    f"path={planned['manifest_relpath']}"
                )
                for row in runs:
                    typer.echo(
                        f"  {row.get('pipeline')} "
                        f"{row.get('interval_start')}..{row.get('interval_end')} "
                        f"run={row.get('extract_run_datetime')}"
                    )
            return

        with _claimed_approval_work(claimed, approval):
            path = write_catchup_manifest(
                planned["manifest"],
                project_root=root,
                settings=settings,
            )
        _consume_approval(root, approval)

    if as_json:
        typer.echo(
            json.dumps(
                {
                    **planned,
                    "dry_run": False,
                    "written": manifest_relpath_for_root(root, path),
                },
                indent=2,
                default=str,
            )
        )
    else:
        typer.echo(
            f"OK silver-catchup-plan wrote={manifest_relpath_for_root(root, path)} "
            f"runs={len(planned['manifest'].get('runs') or [])}"
        )
