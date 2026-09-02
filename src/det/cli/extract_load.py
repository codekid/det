from __future__ import annotations

import sys
from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _APPROVAL_HELP,
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

_LAKE_PATH_HELP = "Unified lake root (layout 1). Ignored when split roots are set."
_LAKE_PATH_RAW_HELP = "Raw layer root URI (layout 2; requires bronze + ops)."
_LAKE_PATH_BRONZE_HELP = "Bronze layer root URI (layout 2; requires raw + ops)."
_LAKE_PATH_OPS_HELP = "Ops layer root URI for runs/locks (layout 2; requires raw + bronze)."


@app.command("extract")
def extract_raw(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    set_: list[str] = typer.Option([], "--set"),
    lock_ttl_sec: int | None = typer.Option(
        None,
        "--lock-ttl-sec",
        help="Lake lease TTL in seconds (default: DET_LOCK_TTL_SEC or 7200)",
    ),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Source → raw data/ + format check + meta/manifest.json."""
    from det.runtime.approval import extract_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    # Resolve before gating so the approval digest is built from the canonical id
    # and ISO interval, not the raw ref form the caller happened to type.
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    claimed = _gate_approval(
        root,
        "extract",
        extract_write_argv(
            resolved.canonical_id,
            start_iso,
            end_iso,
            lake_path=lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
            set_=set_,
        ),
        approval,
        require_approval,
        ctx=ctx,
    )
    try:
        with _claimed_approval_work(claimed, approval):
            result = PipelineRunner(
                settings=_settings(
                    root,
                    lake_path=lake_path,
                    lake_path_raw=lake_path_raw,
                    lake_path_bronze=lake_path_bronze,
                    lake_path_ops=lake_path_ops,
                    lock_ttl_sec=lock_ttl_sec,
                )
            ).extract(
                resolved.path,
                interval_start=start_iso,
                interval_end=end_iso,
                overrides=set_,
            )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(
        f"OK extract pipeline={result.pipeline} artifacts={result.artifacts} raw={result.raw_dir}"
    )


@app.command("load")
def load_bronze(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_run_datetime: str | None = typer.Option(
        None,
        "--extract-run-datetime",
        help="Raw run to load. Defaults to the latest run for the interval.",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    set_: list[str] = typer.Option([], "--set"),
    lock_ttl_sec: int | None = typer.Option(
        None,
        "--lock-ttl-sec",
        help="Lake lease TTL in seconds (default: DET_LOCK_TTL_SEC or 7200)",
    ),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Raw data/ → snake_case naming → JSON Schema → bronze."""
    from det.runtime.approval import load_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    claimed = _gate_approval(
        root,
        "load",
        load_write_argv(
            resolved.canonical_id,
            start_iso,
            end_iso,
            extract_run_datetime,
            lake_path=lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
            set_=set_,
        ),
        approval,
        require_approval,
        ctx=ctx,
    )
    try:
        with _claimed_approval_work(claimed, approval):
            result = PipelineRunner(
                settings=_settings(
                    root,
                    lake_path=lake_path,
                    lake_path_raw=lake_path_raw,
                    lake_path_bronze=lake_path_bronze,
                    lake_path_ops=lake_path_ops,
                    lock_ttl_sec=lock_ttl_sec,
                )
            ).load(
                resolved.path,
                interval_start=start_iso,
                interval_end=end_iso,
                overrides=set_,
                extract_run_datetime=extract_run_datetime,
            )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(
        f"OK load pipeline={result.pipeline} rows={result.rows} partition={result.partition_dir}"
    )


@app.command("run")
def run_pipeline(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    set_: list[str] = typer.Option([], "--set"),
    lock_ttl_sec: int | None = typer.Option(
        None,
        "--lock-ttl-sec",
        help="Lake lease TTL in seconds (default: DET_LOCK_TTL_SEC or 7200)",
    ),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """extract then load with one shared run-start stamp."""
    from det.runtime.approval import run_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    claimed = _gate_approval(
        root,
        "run",
        run_write_argv(
            resolved.canonical_id,
            start_iso,
            end_iso,
            lake_path=lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
            set_=set_,
        ),
        approval,
        require_approval,
        ctx=ctx,
    )
    print("det: run starting…", file=sys.stderr, flush=True)
    try:
        with _claimed_approval_work(claimed, approval):
            result = PipelineRunner(
                settings=_settings(
                    root,
                    lake_path=lake_path,
                    lake_path_raw=lake_path_raw,
                    lake_path_bronze=lake_path_bronze,
                    lake_path_ops=lake_path_ops,
                    lock_ttl_sec=lock_ttl_sec,
                )
            ).run(
                resolved.path,
                interval_start=start_iso,
                interval_end=end_iso,
                overrides=set_,
            )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(f"OK pipeline={result.pipeline} rows={result.rows} partition={result.partition_dir}")
