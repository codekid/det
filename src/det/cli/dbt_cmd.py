from __future__ import annotations

from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _APPROVAL_HELP,
    _PIPELINE_HELP,
    _PROJECT_ROOT_HELP,
    _REQUIRE_APPROVAL_HELP,
    _analytics_exclude,
    _claimed_approval_work,
    _consume_approval,
    _gate_approval,
    _project_root,
    _resolve_pipeline,
)


@app.command("dbt")
def dbt_cmd(
    ctx: typer.Context,
    pipeline: str | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help=f"If set, select stg_<provider>__<source>+ for this pipeline. {_PIPELINE_HELP}",
    ),
    select: list[str] = typer.Option(
        [],
        "--select",
        "-s",
        help="dbt --select args (overrides pipeline default selection)",
    ),
    command: str = typer.Option(
        "build",
        "--command",
        "-c",
        help="dbt subcommand: build | run | test",
    ),
    full_refresh: bool = typer.Option(False, "--full-refresh"),
    project_dir: Path | None = typer.Option(
        None,
        "--project-dir",
        help="dbt project directory (default: <project-root>/dbt)",
    ),
    target: str | None = typer.Option(
        None,
        "--target",
        help="dbt profile target (default: ops when --select is ops-only)",
    ),
    lake_path: str | None = typer.Option(
        None,
        "--lake-path",
        help="Lake root URI or path (default: DET_LAKE_PATH or ./data/lake)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the dbt argv and env without running",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Run dbt (build/run/test) for local testing. Requires the optional [dbt] extra."""
    from det.runtime.approval import dbt_write_argv
    from det.runtime.dbt_runner import DbtNotInstalledError, run_dbt

    if command not in {"build", "run", "test"}:
        raise typer.BadParameter(
            "must be one of: build, run, test",
            param_hint="--command",
        )

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root) if pipeline is not None else None
    claimed = False
    if not dry_run:
        claimed = _gate_approval(
            root,
            "dbt",
            dbt_write_argv(
                resolved.canonical_id if resolved else None,
                command=command,
                select=select or None,
                full_refresh=full_refresh,
                target=target,
                lake_path=lake_path,
                set_=set_ or None,
            ),
            approval,
            require_approval,
            ctx=ctx,
        )
    pipe = None
    if resolved is not None:
        pipe = resolved.path

    try:
        with _claimed_approval_work(claimed, approval):
            if resolved is not None:
                from det.runtime.config import load_pipeline_config
                from det.scaffold.view_warn import emit_view_size_warnings

                cfg = load_pipeline_config(resolved.path, overrides=set_ or None)
                for w in emit_view_size_warnings(
                    cfg,
                    project_root=root,
                    lake_path=lake_path,
                ):
                    typer.echo(f"WARNING: {w.message}", err=True)

            result = run_dbt(
                project_root=root,
                command=command,  # type: ignore[arg-type]
                project_dir=project_dir,
                select=select or None,
                exclude=_analytics_exclude(select or None),
                target=target,
                full_refresh=full_refresh,
                lake_path=lake_path,
                pipeline=pipe,
                pipeline_overrides=set_ or None,
                dry_run=dry_run,
            )
            if not dry_run and result.returncode != 0:
                raise typer.Exit(code=result.returncode)
            if not dry_run:
                _consume_approval(root, approval)
    except DbtNotInstalledError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    except FileNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    sel = " ".join(result.select) if result.select else "(all)"
    typer.echo(f"{'DRY-RUN' if dry_run else 'RUN'} dbt={' '.join(result.command)}")
    typer.echo(f"  select={sel}")
    if result.lake_path:
        typer.echo(f"  DET_LAKE_PATH={result.lake_path}")
    if result.bronze_source:
        typer.echo(f"  DET_BRONZE_SOURCE={result.bronze_source}")
    if dry_run:
        return
    typer.echo(f"OK dbt finished exit={result.returncode}")

