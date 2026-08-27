from __future__ import annotations

from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _APPROVAL_HELP,
    _PIPELINE_HELP,
    _PROJECT_ROOT_HELP,
    _REQUIRE_APPROVAL_HELP,
    _consume_approval,
    _gate_approval,
    _project_root,
    _resolve_interval,
    _resolve_pipeline,
)
from det.cli.render_runs import _print_run_list, _print_run_summary


@app.command("runs")
def runs_cmd(
    pipeline: str | None = typer.Option(None, "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str | None = typer.Option(
        None,
        "--interval-start",
        "-s",
        help="Attempt-date window start (default: 7 days ago). Not the data interval.",
    ),
    interval_end: str | None = typer.Option(
        None,
        "--interval-end",
        "-e",
        help="Attempt-date window end, exclusive (default: tomorrow UTC).",
    ),
    status: str | None = typer.Option(None, "--status", help="ok or error"),
    command: str | None = typer.Option(None, "--command", help="extract or load"),
    limit: int = typer.Option(50, "--limit", help="Max receipts to print"),
    summary: bool = typer.Option(False, "--summary", help="Per pipeline+command counts"),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show interval, owner, destination, extract, and attempt details",
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """List extract/load run receipts (observability). Manifest stays the data authority."""
    import json

    from det.destinations.models import lake_root
    from det.runtime.config import load_pipeline_config
    from det.runtime.lake import open_lake, pick_lake_spec
    from det.runtime.receipts import list_receipts, summarize_receipts

    root = _project_root(project_root)
    if pipeline:
        resolved = _resolve_pipeline(pipeline, root)
        config = load_pipeline_config(resolved.path)
        lake = lake_root(config.destination, root, cli_lake_path=lake_path)
        pipe_id = config.name
    else:
        spec = pick_lake_spec(cli_lake_path=lake_path, destination_path=None)
        lake = open_lake(spec, root)
        pipe_id = None
    try:
        if summary:
            payload = summarize_receipts(
                lake,
                pipeline=pipe_id,
                since=interval_start,
                until=interval_end,
                status=status,
                command=command,
            )
        else:
            payload = list_receipts(
                lake,
                pipeline=pipe_id,
                since=interval_start,
                until=interval_end,
                status=status,
                command=command,
                limit=limit,
            )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if as_json:
        typer.echo(json.dumps(payload, indent=2, default=str))
        return
    if summary:
        if not isinstance(payload, dict):
            raise TypeError("summarize_receipts must return a dict")
        groups = payload.get("groups") or []
        if not groups:
            typer.echo(f"(no receipts in {payload.get('since')}..{payload.get('until')})")
            return
        _print_run_summary(payload, include_pipeline=pipe_id is None)
        return
    if not isinstance(payload, list):
        raise TypeError("list_receipts must return a list")
    if not payload:
        typer.echo("(no receipts)")
        return
    _print_run_list(payload, include_pipeline=pipe_id is None, verbose=verbose)


@app.command("runs-materialize")
def runs_materialize_cmd(
    interval_start: str | None = typer.Option(
        None,
        "--interval-start",
        "-s",
        help="Attempt-date window start (default: 7 days ago). Not the data interval.",
    ),
    interval_end: str | None = typer.Option(
        None,
        "--interval-end",
        "-e",
        help="Attempt-date window end, exclusive (default: tomorrow UTC).",
    ),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Project ``{lake}/runs/`` JSON into Iceberg ``ops.run_receipts`` (replace-by-day)."""
    from det.runtime.lake import open_lake, pick_lake_spec
    from det.runtime.receipts_materialize import materialize_receipts

    root = _project_root(project_root)
    spec = pick_lake_spec(cli_lake_path=lake_path, destination_path=None)
    lake = open_lake(spec, root)
    try:
        stats = materialize_receipts(
            lake,
            since=interval_start,
            until=interval_end,
        )
    except ImportError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        f"OK materialize since={stats.since.isoformat()} until={stats.until.isoformat()} "
        f"days={stats.days_touched} rows={stats.rows_written} "
        f"skipped={stats.skipped} table={stats.table_location}"
    )


@app.command("biglake-register")
def biglake_register_cmd(
    ctx: typer.Context,
    pipeline: str | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help="Register one pipeline bronze table only (default: all bronze + ops)",
    ),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project: str | None = typer.Option(None, "--project", help="GCP project (DET_GCP_PROJECT)"),
    location: str | None = typer.Option(None, "--location", help="BQ location (DET_BQ_LOCATION)"),
    connection: str | None = typer.Option(
        None,
        "--connection",
        help="BigLake connection id (DET_BQ_CONNECTION)",
    ),
    skip_ops: bool = typer.Option(False, "--skip-ops", help="Do not register ops.run_receipts"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview registration plan only"),
    apply: bool = typer.Option(False, "--apply", help="Create/update BigLake external tables"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Register DET Iceberg tables as BigLake external tables in BigQuery (gs:// lakes)."""
    from det.runtime.biglake_register import (
        apply_biglake_register,
        biglake_register_write_argv,
        build_biglake_register_plan,
        format_dry_run,
    )

    if dry_run == apply:
        raise typer.BadParameter(
            "exactly one of --dry-run or --apply is required",
            param_hint="--dry-run/--apply",
        )

    root = _project_root(project_root)
    pipe_path = None
    if pipeline is not None:
        pipe_path = _resolve_pipeline(pipeline, root).path

    argv = biglake_register_write_argv(
        lake_path=lake_path,
        pipeline=pipeline,
        project=project,
        location=location,
        connection=connection,
        skip_ops=skip_ops or pipeline is not None,
    )
    try:
        plan = build_biglake_register_plan(
            project_root=root,
            lake_path=lake_path,
            pipeline=pipe_path,
            project=project,
            location=location,
            connection=connection,
            include_ops=not skip_ops and pipeline is None,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    if dry_run:
        typer.echo(format_dry_run(plan, argv))
        return

    _gate_approval(root, "biglake-register", argv, approval, require_approval, ctx=ctx)
    try:
        result = apply_biglake_register(plan)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(f"OK biglake-register applied={result['count']}")
    for row in result["applied"]:
        typer.echo(
            f"  {row['bq_dataset']}.{row['bq_table']} metadata={row['metadata_uri']}"
        )


@app.command("lock-show")
def lock_show(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Print the lease for a pipeline interval (or 'no lock')."""
    from det.destinations.models import lake_root
    from det.runtime.config import load_pipeline_config
    from det.runtime.lease import lock_path, open_lease_store, resolve_lease_options
    from det.runtime.settings import DetSettings

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    config = load_pipeline_config(resolved.path)
    settings = DetSettings.from_env(project_root=root)
    if lake_path is not None:
        settings = settings.with_overrides(lake_override=lake_path)
    options = resolve_lease_options(settings=settings, pipeline=config)
    lake = lake_root(config.destination, root, cli_lake_path=lake_path, settings=settings)
    store = open_lease_store(lake, options, resolve_secret=settings.resolve_secret)
    payload = store.inspect(
        pipeline=config.name, interval_start=start_iso, interval_end=end_iso
    )
    if payload is None:
        if options.backend == "lake":
            path = lock_path(lake, config.name, start_iso, end_iso)
            typer.echo(f"no lock path={path}")
        else:
            typer.echo(
                f"no lock backend=postgres schema={options.pg_schema} "
                f"table={options.pg_table}"
            )
        return
    if options.backend == "lake":
        path = lock_path(lake, config.name, start_iso, end_iso)
        typer.echo(f"path={path}")
    else:
        typer.echo(f"backend=postgres schema={options.pg_schema} table={options.pg_table}")
    for key in (
        "pipeline",
        "interval_start",
        "interval_end",
        "owner",
        "command",
        "expires_at",
        "ttl_sec",
    ):
        if key in payload:
            typer.echo(f"{key}={payload[key]}")


@app.command("lock-release")
def lock_release(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    force: bool = typer.Option(False, "--force", help="Required to delete a live lease"),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Force-delete a lease. Kill the worker first or you can dual-insert."""
    from det.destinations.models import lake_root
    from det.runtime.approval import lock_release_write_argv
    from det.runtime.config import load_pipeline_config
    from det.runtime.lease import lock_path, open_lease_store, resolve_lease_options
    from det.runtime.settings import DetSettings

    if not force:
        raise typer.BadParameter("--force is required to delete a lock", param_hint="--force")

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    _gate_approval(
        root,
        "lock-release",
        lock_release_write_argv(
            resolved.canonical_id, start_iso, end_iso, lake_path=lake_path
        ),
        approval,
        require_approval,
        ctx=ctx,
    )
    config = load_pipeline_config(resolved.path)
    settings = DetSettings.from_env(project_root=root)
    if lake_path is not None:
        settings = settings.with_overrides(lake_override=lake_path)
    options = resolve_lease_options(settings=settings, pipeline=config)
    lake = lake_root(config.destination, root, cli_lake_path=lake_path, settings=settings)
    store = open_lease_store(lake, options, resolve_secret=settings.resolve_secret)
    held = store.inspect(
        pipeline=config.name, interval_start=start_iso, interval_end=end_iso
    )
    location = (
        str(lock_path(lake, config.name, start_iso, end_iso))
        if options.backend == "lake"
        else f"postgres:{options.pg_schema}.{options.pg_table}"
    )
    if held is None:
        _consume_approval(root, approval)
        typer.echo(f"no lock location={location}")
        return
    typer.echo(
        f"releasing owner={held.get('owner')} expires_at={held.get('expires_at')} "
        f"location={location}",
        err=True,
    )
    store.force_release(
        pipeline=config.name, interval_start=start_iso, interval_end=end_iso
    )
    _consume_approval(root, approval)
    typer.echo(f"OK lock-release location={location}")


@app.command("lock-init")
def lock_init(
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help="Optional pipeline for lease: YAML overlay"
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Ensure Postgres lease schema/table exist (no-op for lake backend)."""
    from pathlib import Path as _Path

    from det.runtime.config import load_pipeline_config
    from det.runtime.lake import open_lake
    from det.runtime.lease import open_lease_store, resolve_lease_options
    from det.runtime.lease.postgres_store import PostgresLeaseStore
    from det.runtime.settings import DetSettings

    root = _project_root(project_root)
    settings = DetSettings.from_env(project_root=root)
    config = None
    if pipeline is not None:
        resolved = _resolve_pipeline(pipeline, root)
        config = load_pipeline_config(resolved.path)
    options = resolve_lease_options(settings=settings, pipeline=config)
    if options.backend != "postgres":
        typer.echo("OK lock-init backend=lake (no DDL)")
        return
    # Postgres store ignores lake; open a throwaway local root for the factory.
    lake = open_lake(str(root / ".det" / "lock-init-lake"), _Path(root))
    store = open_lease_store(lake, options, resolve_secret=settings.resolve_secret)
    if not isinstance(store, PostgresLeaseStore):
        raise typer.Exit(code=1)
    store.ensure()
    typer.echo(
        f"OK lock-init backend=postgres schema={options.pg_schema} "
        f"table={options.pg_table} mode={options.mode}"
    )
