from __future__ import annotations

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
    _resolve_pipeline,
)


@app.command("init-source")
def init_source_cmd(
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Canonical provider.source id (writes sources/<provider>/<source>.py)",
    ),
    destination_type: str = typer.Option(
        "iceberg",
        "--destination-type",
        help="iceberg (default lake) | filesystem (JSONL) | duckdb | postgres",
    ),
    connection: str | None = typer.Option(
        None,
        "--connection",
        help=(
            "DuckDB file path, or for postgres the env var name holding the DSN "
            "(e.g. DET_POSTGRES_DSN). Required for duckdb/postgres"
        ),
    ),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    skip_pipeline: bool = typer.Option(
        False,
        "--skip-pipeline",
        help="Only write the plugin file (no YAML / schema / dbt)",
    ),
    skip_dbt: bool = typer.Option(False, "--skip-dbt", help="Skip scaffold-dbt"),
    force: bool = typer.Option(False, "--force"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Scaffold a project-local source plugin (+ pipeline YAML / schema by default)."""
    from det.scaffold.init_source import init_source

    if destination_type not in {"filesystem", "duckdb", "postgres", "iceberg"}:
        raise typer.BadParameter(
            "must be filesystem, duckdb, postgres, or iceberg",
            param_hint="--destination-type",
        )
    root = _project_root(project_root)
    try:
        result = init_source(
            name=name,
            project_root=root,
            force=force,
            dry_run=dry_run,
            skip_pipeline=skip_pipeline,
            skip_dbt=skip_dbt,
            destination_type=destination_type,
            lake_path=lake_path,
            connection=connection,
        )
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    mode = "DRY-RUN" if dry_run else "OK"
    typer.echo(f"{mode} init-source name={result.name} plugin={result.plugin_path}")
    for action in result.actions:
        typer.echo(f"  {action.action}: {action.path} ({action.detail})")


@app.command("init-pipeline")
def init_pipeline_cmd(
    ctx: typer.Context,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Canonical provider.source id (must match --source-type)",
    ),
    source_type: str = typer.Option(
        ...,
        "--source-type",
        help="Registered source plugin, e.g. noaa.storm_events",
    ),
    destination_type: str = typer.Option(
        "iceberg",
        "--destination-type",
        help="iceberg (default lake) | filesystem (JSONL) | duckdb | postgres",
    ),
    connection: str | None = typer.Option(
        None,
        "--connection",
        help=(
            "DuckDB file path, or for postgres the env var name holding the DSN "
            "(e.g. DET_POSTGRES_DSN). Required for duckdb/postgres"
        ),
    ),
    lake_path: str | None = typer.Option(
        None,
        "--lake-path",
        help="Rare: emit destination.path (default lake is DET_LAKE_PATH or ./data/lake)",
    ),
    skip_dbt: bool = typer.Option(False, "--skip-dbt", help="Skip scaffold-dbt"),
    force: bool = typer.Option(False, "--force"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Create pipeline YAML + minimal schema + scaffold-dbt models."""
    from det.runtime.approval import init_pipeline_write_argv
    from det.scaffold.init_pipeline import init_pipeline

    if destination_type not in {"filesystem", "duckdb", "postgres", "iceberg"}:
        raise typer.BadParameter(
            "must be filesystem, duckdb, postgres, or iceberg",
            param_hint="--destination-type",
        )
    root = _project_root(project_root)
    claimed = False
    if not dry_run:
        claimed = _gate_approval(
            root,
            "init-pipeline",
            init_pipeline_write_argv(
                name,
                source_type,
                destination_type=destination_type,
                connection=connection,
                lake_path=lake_path,
                skip_dbt=skip_dbt,
                force=force,
            ),
            approval,
            require_approval,
            ctx=ctx,
        )
    try:
        with _claimed_approval_work(claimed, approval):
            result = init_pipeline(
                name=name,
                source_type=source_type,
                project_root=root,
                force=force,
                dry_run=dry_run,
                skip_dbt=skip_dbt,
                destination_type=destination_type,
                lake_path=lake_path,
                connection=connection,
            )
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    if not dry_run:
        _consume_approval(root, approval)
    mode = "DRY-RUN" if dry_run else "OK"
    typer.echo(f"{mode} init-pipeline name={result.name}")
    for action in result.actions:
        rel = action.path
        try:
            rel = action.path.relative_to(root)
        except ValueError:
            pass
        typer.echo(f"  {action.action}: {rel}" + (f" ({action.detail})" if action.detail else ""))


@app.command("scaffold-dbt")
def scaffold_dbt_cmd(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing stg/silver SQL and refresh YAML entries",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print actions without writing files",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Generate dbt source + stg + silver from a pipeline schema (gold is hand-written)."""
    from det.runtime.approval import scaffold_dbt_write_argv
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    claimed = False
    if not dry_run:
        claimed = _gate_approval(
            root,
            "scaffold-dbt",
            scaffold_dbt_write_argv(resolved.canonical_id, force=force, set_=set_),
            approval,
            require_approval,
            ctx=ctx,
        )
    config = load_pipeline_config(resolved.path, overrides=set_)
    from det.scaffold.view_warn import collect_view_size_warnings

    with _claimed_approval_work(claimed, approval):
        result = scaffold_dbt(config, project_root=root, force=force, dry_run=dry_run, warn=False)
    if not dry_run:
        _consume_approval(root, approval)
    mode = "DRY-RUN" if dry_run else "OK"
    typer.echo(f"{mode} scaffold-dbt dataset={result.dataset}")
    for action in result.actions:
        rel = action.path
        try:
            rel = action.path.relative_to(root)
        except ValueError:
            pass
        typer.echo(f"  {action.action}: {rel}" + (f" ({action.detail})" if action.detail else ""))
    for w in collect_view_size_warnings(config, project_root=root):
        typer.echo(f"WARNING: {w.message}", err=True)

