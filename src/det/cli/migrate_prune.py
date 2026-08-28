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
    _resolve_interval,
    _resolve_pipeline,
    _schema_for_digest,
    _settings,
)


@app.command("migrate")
def migrate_bronze(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    to_bronze: str = typer.Option(..., "--to-bronze"),
    schema: Path = typer.Option(..., "--schema"),
    mapper: str = typer.Option(..., "--mapper"),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    from_raw: str | None = typer.Option(
        None,
        "--from-raw",
        help="Raw dataset lake id (defaults to pipeline {name}_v{wire_version})",
    ),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    ingestion: str = typer.Option("thin", "--ingestion"),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview partitions/rows/validation without writing bronze",
    ),
    validate_limit: int | None = typer.Option(
        None,
        "--validate-limit",
        help="With --dry-run, cap rows checked per raw partition",
    ),
    wire_version: int | None = typer.Option(
        None,
        "--wire-version",
        help="Only migrate raw partitions whose manifest wire_version matches",
    ),
    recreate_iceberg: bool = typer.Option(
        False,
        "--recreate-iceberg",
        help=(
            "Purge the target Iceberg bronze table, then recreate with YAML "
            "destination.partition. Drops the full table; rewrite scope is "
            "-s/-e or --all-raw (latest raw per interval unless --all-raw-runs)."
        ),
    ),
    all_raw: bool = typer.Option(
        False,
        "--all-raw",
        help=(
            "With --recreate-iceberg: rewrite every interval under raw "
            "(no -s/-e). Latest extract per interval unless --all-raw-runs."
        ),
    ),
    all_raw_runs: bool = typer.Option(
        False,
        "--all-raw-runs",
        help=(
            "Rematerialize every committed raw extract-run sibling as its own "
            "bronze run (default: latest only, matching det load)."
        ),
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
    lock_ttl_sec: int | None = typer.Option(
        None,
        "--lock-ttl-sec",
        help="Lake lease TTL in seconds (default: DET_LOCK_TTL_SEC or 7200)",
    ),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Rebuild bronze from raw data/ for an interval."""
    from det.runtime.approval import migrate_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.migrate import BronzeMigrator, MigratePlan

    if all_raw:
        if interval_start is not None or interval_end is not None:
            raise typer.BadParameter(
                "--all-raw cannot be combined with -s/-e",
                param_hint="--all-raw",
            )
        if not recreate_iceberg:
            raise typer.BadParameter(
                "--all-raw requires --recreate-iceberg",
                param_hint="--all-raw",
            )
        start, end = None, None
    else:
        if interval_start is None:
            raise typer.BadParameter(
                "-s/--interval-start is required unless --all-raw",
                param_hint="-s",
            )
        start, end = _resolve_interval(interval_start, interval_end)
    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    claimed = False
    if not dry_run:
        claimed = _gate_approval(
            root,
            "migrate",
            migrate_write_argv(
                resolved.canonical_id,
                to_bronze,
                _schema_for_digest(schema, root),
                mapper,
                start,
                interval_end=end,
                from_raw=from_raw,
                wire_version=wire_version,
                recreate_iceberg=recreate_iceberg,
                all_raw=all_raw,
                all_raw_runs=all_raw_runs,
                lake_path=lake_path,
                ingestion=ingestion,
                set_=set_,
            ),
            approval,
            require_approval,
            ctx=ctx,
        )
    if validate_limit is not None and not dry_run:
        raise typer.BadParameter(
            "--validate-limit requires --dry-run",
            param_hint="--validate-limit",
        )
    try:
        with _claimed_approval_work(claimed, approval):
            result = BronzeMigrator(
                settings=_settings(root, lake_path=lake_path, lock_ttl_sec=lock_ttl_sec)
            ).migrate(
                pipeline=resolved.path,
                to_bronze=to_bronze,
                schema_path=schema if schema.is_absolute() else root / schema,
                mapper_name=mapper,
                interval_start=start,
                interval_end=end,
                from_raw=from_raw,
                lake_path=lake_path,
                ingestion_library=ingestion,
                overrides=set_,
                dry_run=dry_run,
                validate_limit=validate_limit,
                wire_version=wire_version,
                recreate_iceberg=recreate_iceberg,
                all_raw=all_raw,
                all_raw_runs=all_raw_runs,
            )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    if isinstance(result, MigratePlan):
        filt = (
            f" wire_version={result.wire_version_filter}"
            if result.wire_version_filter is not None
            else ""
        )
        typer.echo(
            f"DRY-RUN migrate {result.from_raw} -> {result.to_bronze} "
            f"mapper={result.mapper_name} partitions={result.partitions_planned} "
            f"rows_checked={result.rows_checked} ok={result.ok}{filt}"
        )
        if result.recreate_warning:
            typer.echo(f"WARNING: {result.recreate_warning}", err=True)
        for part in result.partitions:
            status = "ok" if part.ok else "FAIL"
            trunc = " truncated" if part.truncated else ""
            typer.echo(
                f"  [{status}] wire_v={part.wire_version} rows={part.rows}{trunc} "
                f"raw={part.raw_path} -> {part.would_write_bronze_path}"
            )
            for err in part.errors[:5]:
                typer.echo(f"    - {err}", err=True)
        if not result.ok:
            raise typer.Exit(code=1)
        return
    _consume_approval(root, approval)
    typer.echo(
        f"OK migrate {result.from_raw} -> {result.to_bronze} "
        f"partitions={result.partitions} rows={result.rows}"
    )


@app.command("prune")
def prune_bronze(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    keep: int = typer.Option(
        1,
        "--keep",
        help="Newest extract runs to keep per interval (bronze only)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview deletes only"),
    apply: bool = typer.Option(False, "--apply", help="Perform bronze deletes"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
    lock_ttl_sec: int | None = typer.Option(
        None,
        "--lock-ttl-sec",
        help="Lake lease TTL in seconds (default: DET_LOCK_TTL_SEC or 7200)",
    ),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Delete old bronze extract runs. Never touches raw/. Requires --dry-run or --apply."""
    from det.runtime.approval import prune_write_argv
    from det.runtime.config import load_pipeline_config
    from det.runtime.lease import LeaseHeldError
    from det.runtime.prune import BronzePruner

    if dry_run == apply:
        raise typer.BadParameter(
            "exactly one of --dry-run or --apply is required",
            param_hint="--dry-run/--apply",
        )
    if keep < 1:
        raise typer.BadParameter("--keep must be >= 1", param_hint="--keep")

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    claimed = False
    if apply:
        claimed = _gate_approval(
            root,
            "prune",
            prune_write_argv(
                resolved.canonical_id,
                start_iso,
                interval_end=end_iso,
                keep=keep,
                set_=set_,
            ),
            approval,
            require_approval,
            ctx=ctx,
        )
    config = load_pipeline_config(resolved.path, overrides=set_)
    pruner = BronzePruner(
        settings=_settings(root, lock_ttl_sec=lock_ttl_sec)
    )
    plan = pruner.plan(
        config,
        interval_start=start_iso,
        interval_end=end_iso,
        keep=keep,
    )
    if dry_run:
        typer.echo(
            f"DRY-RUN prune pipeline={config.name} keep={keep} would_remove={plan.remove_count}"
        )
        for ref in plan.to_remove:
            loc = str(ref.path) if ref.path is not None else "duckdb"
            typer.echo(
                f"  remove interval={ref.interval_start}..{ref.interval_end} "
                f"run={ref.extract_run_datetime} ({loc})"
            )
        return

    try:
        with _claimed_approval_work(claimed, approval):
            removed = pruner.apply(
                config,
                plan,
                interval_start=start_iso,
                interval_end=end_iso,
            )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(f"OK prune pipeline={config.name} keep={keep} removed={removed}")
