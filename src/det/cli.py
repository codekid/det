from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Emit before any heavy imports so a hang during import is visible under `uv run`.
print("det: importing…", file=sys.stderr, flush=True)

import typer  # noqa: E402

from det.logging import configure_logging, get_logger  # noqa: E402

app = typer.Typer(
    name="det",
    help="DET — Data Extract Tool",
    no_args_is_help=True,
)
logger = get_logger(__name__)

_PIPELINE_HELP = (
    "Pipeline ref: canonical id (noaa.storm_events), slash form, or YAML path under the project"
)
_PROJECT_ROOT_HELP = "Project root (default: DET_PROJECT_ROOT env, else cwd)"
_APPROVAL_HELP = (
    "Id from `det approve` (apr_…). Validated whenever passed; required when "
    "DET_REQUIRE_APPROVAL=1 or --require-approval"
)
_REQUIRE_APPROVAL_HELP = "Fail unless --approval is set (same as DET_REQUIRE_APPROVAL=1)"


@app.callback()
def main(
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
    log_format: str | None = typer.Option(
        None,
        "--log-format",
        help="json | console (default: json when stderr is not a TTY)",
    ),
) -> None:
    try:
        configure_logging(log_level, log_format=log_format)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--log-format") from exc
    logger.info("det starting", log_level=log_level)
    print("det: loading plugins…", file=sys.stderr, flush=True)
    from det.plugins import load_plugins

    load_plugins()
    logger.info("plugins loaded")


def _resolve_interval(start: str, end: str | None) -> tuple[str, str]:
    from det.runtime.meta import resolve_interval, to_interval_datetime

    for value, hint in ((start, "--interval-start"), (end, "--interval-end")):
        if value is None:
            continue
        try:
            to_interval_datetime(value)
        except Exception as exc:
            raise typer.BadParameter(
                f"{value!r} is not a date (YYYY-MM-DD) or ISO datetime",
                param_hint=hint,
            ) from exc
    try:
        return resolve_interval(start, end)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--interval-end") from exc


def _project_root(explicit: Path | None) -> Path:
    from det.runtime.pipelines import resolve_project_root

    return resolve_project_root(explicit)


def _resolve_pipeline(ref: str, root: Path):
    """Resolve pipeline ref; log and echo the resolved path for auditability."""
    from det.runtime.pipelines import PipelineRefError, resolve_pipeline_ref

    try:
        resolved = resolve_pipeline_ref(ref, project_root=root)
    except PipelineRefError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    logger.info(
        "resolved pipeline",
        ref=resolved.ref,
        canonical_id=resolved.canonical_id,
        path=resolved.relative_path,
        project_root=str(resolved.project_root),
    )
    typer.echo(
        f"pipeline={resolved.canonical_id} path={resolved.relative_path}",
        err=True,
    )
    return resolved


def _analytics_exclude(select: list[str] | None) -> list[str] | None:
    from det.runtime.dbt_runner import analytics_exclude

    return analytics_exclude(select)


def _gate_approval(
    root: Path,
    command: str,
    argv: list[str],
    approval: str | None,
    require_approval: bool,
) -> None:
    from det.runtime.approval import ApprovalError, check_approval, require_approvals_enabled

    try:
        check_approval(
            root,
            command,
            argv,
            approval,
            require=require_approval or require_approvals_enabled(),
        )
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _consume_approval(root: Path, approval: str | None) -> None:
    if not approval:
        return
    from det.runtime.approval import ApprovalError, consume_approval

    try:
        consume_approval(root, approval)
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("extract")
def extract_raw(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
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
    """Source → raw data/ + format check + meta/manifest.json."""
    from det.runtime.approval import extract_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    _gate_approval(
        root,
        "extract",
        extract_write_argv(pipeline, interval_start, interval_end),
        approval,
        require_approval,
    )
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    try:
        result = PipelineRunner(root).extract(
            resolved.path,
            interval_start=start_iso,
            interval_end=end_iso,
            overrides=set_,
            lock_ttl_sec=lock_ttl_sec,
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
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_run_datetime: str | None = typer.Option(
        None,
        "--extract-run-datetime",
        help="Raw run to load. Defaults to the latest run for the interval.",
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
    """Raw data/ → snake_case naming → JSON Schema → bronze."""
    from det.runtime.approval import load_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    _gate_approval(
        root,
        "load",
        load_write_argv(pipeline, interval_start, interval_end, extract_run_datetime),
        approval,
        require_approval,
    )
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    try:
        result = PipelineRunner(root).load(
            resolved.path,
            interval_start=start_iso,
            interval_end=end_iso,
            overrides=set_,
            extract_run_datetime=extract_run_datetime,
            lock_ttl_sec=lock_ttl_sec,
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
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
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
    """extract then load with one shared run-start stamp."""
    from det.runtime.approval import run_write_argv
    from det.runtime.lease import LeaseHeldError
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    _gate_approval(
        root,
        "run",
        run_write_argv(pipeline, interval_start, interval_end),
        approval,
        require_approval,
    )
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    print("det: run starting…", file=sys.stderr, flush=True)
    try:
        result = PipelineRunner(root).run(
            resolved.path,
            interval_start=start_iso,
            interval_end=end_iso,
            overrides=set_,
            lock_ttl_sec=lock_ttl_sec,
        )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(f"OK pipeline={result.pipeline} rows={result.rows} partition={result.partition_dir}")


@app.command("migrate")
def migrate_bronze(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    to_bronze: str = typer.Option(..., "--to-bronze"),
    schema: Path = typer.Option(..., "--schema"),
    mapper: str = typer.Option(..., "--mapper"),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
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

    start, end = _resolve_interval(interval_start, interval_end)
    root = _project_root(project_root)
    if not dry_run:
        _gate_approval(
            root,
            "migrate",
            migrate_write_argv(
                pipeline,
                to_bronze,
                str(schema),
                mapper,
                interval_start,
                interval_end=interval_end,
                from_raw=from_raw,
                wire_version=wire_version,
            ),
            approval,
            require_approval,
        )
    resolved = _resolve_pipeline(pipeline, root)
    if validate_limit is not None and not dry_run:
        raise typer.BadParameter(
            "--validate-limit requires --dry-run",
            param_hint="--validate-limit",
        )
    try:
        result = BronzeMigrator(root).migrate(
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
            lock_ttl_sec=lock_ttl_sec,
        )
    except LeaseHeldError as exc:
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


@app.command("dbt")
def dbt_cmd(
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
    if not dry_run:
        _gate_approval(
            root,
            "dbt",
            dbt_write_argv(pipeline, command=command, select=select or None),
            approval,
            require_approval,
        )
    pipe = None
    if pipeline is not None:
        pipe = _resolve_pipeline(pipeline, root).path
        from det.runtime.config import load_pipeline_config
        from det.scaffold.view_warn import emit_view_size_warnings

        cfg = load_pipeline_config(pipe, overrides=set_ or None)
        for w in emit_view_size_warnings(
            cfg,
            project_root=root,
            lake_path=lake_path,
        ):
            typer.echo(f"WARNING: {w.message}", err=True)

    try:
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
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)
    _consume_approval(root, approval)
    typer.echo(f"OK dbt finished exit={result.returncode}")


@app.command("init-pipeline")
def init_pipeline_cmd(
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
    if not dry_run:
        _gate_approval(
            root,
            "init-pipeline",
            init_pipeline_write_argv(
                name,
                source_type,
                destination_type=destination_type,
                connection=connection,
                lake_path=lake_path,
                skip_dbt=skip_dbt,
            ),
            approval,
            require_approval,
        )
    try:
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
    if not dry_run:
        _gate_approval(
            root,
            "scaffold-dbt",
            scaffold_dbt_write_argv(pipeline, force=force),
            approval,
            require_approval,
        )
    resolved = _resolve_pipeline(pipeline, root)
    config = load_pipeline_config(resolved.path, overrides=set_)
    from det.scaffold.view_warn import collect_view_size_warnings

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


@app.command("prune")
def prune_bronze(
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
    if apply:
        _gate_approval(
            root,
            "prune",
            prune_write_argv(
                pipeline,
                interval_start,
                interval_end=interval_end,
                keep=keep,
            ),
            approval,
            require_approval,
        )
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    config = load_pipeline_config(resolved.path, overrides=set_)
    pruner = BronzePruner(root)
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
        removed = pruner.apply(
            config,
            plan,
            interval_start=start_iso,
            interval_end=end_iso,
            lock_ttl_sec=lock_ttl_sec,
        )
    except LeaseHeldError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval)
    typer.echo(f"OK prune pipeline={config.name} keep={keep} removed={removed}")


def _format_duration(value: object) -> str:
    try:
        milliseconds = max(0, int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    if milliseconds < 1000:
        return f"{milliseconds}ms"
    seconds = milliseconds / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(milliseconds // 1000, 60)
    return f"{minutes}m {remainder:02d}s"


def _format_started(value: object) -> str:
    text = str(value or "")
    if not text:
        return "-"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone().strftime("%b %d %H:%M:%S")
    except ValueError:
        return text[:19]


def _format_window_date(value: object) -> str:
    text = str(value or "")
    try:
        return datetime.fromisoformat(text).strftime("%b %d, %Y")
    except ValueError:
        return text or "-"


def _truncate(value: object, width: int) -> str:
    text = str(value or "-")
    if len(text) <= width:
        return text
    return text[: max(1, width - 1)] + "…"


def _table_widths(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> tuple[int, ...]:
    return tuple(
        max(len(header), *(len(row[index]) for row in rows)) for index, header in enumerate(headers)
    )


def _table_row(
    values: tuple[str, ...],
    widths: tuple[int, ...],
    *,
    status_color: bool = False,
) -> str:
    cells = [value.ljust(widths[index]) for index, value in enumerate(values)]
    if status_color and sys.stdout.isatty():
        color = typer.colors.GREEN if values[0] == "OK" else typer.colors.RED
        cells[0] = typer.style(cells[0], fg=color, bold=True)
    return "  ".join(cells).rstrip()


def _print_run_list(
    receipts: list[dict[str, object]],
    *,
    include_pipeline: bool,
    verbose: bool,
) -> None:
    headers = ("STATUS", "COMMAND")
    if include_pipeline:
        headers += ("PIPELINE",)
    headers += ("DURATION", "STARTED")

    rows: list[tuple[str, ...]] = []
    for receipt in receipts:
        values = (
            str(receipt.get("status") or "-").upper(),
            str(receipt.get("command") or "-"),
        )
        if include_pipeline:
            values += (_truncate(receipt.get("pipeline"), 30),)
        values += (
            _format_duration(receipt.get("duration_ms")),
            _format_started(receipt.get("started_at")),
        )
        rows.append(values)

    widths = _table_widths(headers, rows)
    typer.echo(_table_row(headers, widths))
    for receipt, row in zip(receipts, rows, strict=True):
        typer.echo(_table_row(row, widths, status_color=True))
        if receipt.get("status") == "error":
            code = str(receipt.get("error_code") or "unknown")
            message = str(receipt.get("error_message") or "")
            detail = f"{code}: {message}" if message else code
            typer.echo(f"        └─ {_truncate(detail, 110)}")
        if verbose:
            typer.echo(
                "        "
                f"Owner: {receipt.get('owner') or '-'}  "
                f"Destination: {receipt.get('destination') or '-'}"
            )
            typer.echo(
                "        "
                f"Interval: {receipt.get('interval_start') or '-'} → "
                f"{receipt.get('interval_end') or '-'}"
            )
            typer.echo(
                "        "
                f"Extract: {receipt.get('extract_run_datetime') or '-'}  "
                f"Attempt ID: {receipt.get('attempt_id') or '-'}"
            )
            if receipt.get("error_class"):
                typer.echo(f"        Error class: {receipt['error_class']}")


def _print_run_summary(
    payload: dict[str, object],
    *,
    include_pipeline: bool,
) -> None:
    typer.echo(
        f"Attempt window: {_format_window_date(payload.get('since'))} – "
        f"{_format_window_date(payload.get('until'))} (end exclusive)"
    )
    typer.echo()
    headers = ("COMMAND",)
    if include_pipeline:
        headers = ("PIPELINE",) + headers
    headers += ("ATTEMPTS", "OK", "ERRORS", "P50", "P95", "ROWS", "ERROR CODES")

    rows: list[tuple[str, ...]] = []
    groups = payload.get("groups")
    assert isinstance(groups, list)
    for group in groups:
        assert isinstance(group, dict)
        values = (str(group.get("command") or "-"),)
        if include_pipeline:
            values = (_truncate(group.get("pipeline"), 30),) + values
        error_codes = group.get("error_codes")
        codes = ""
        if isinstance(error_codes, dict):
            codes = ", ".join(f"{name}×{count}" for name, count in sorted(error_codes.items()))
        rows_value = group.get("rows")
        shown_rows = "-" if group.get("command") == "extract" else f"{int(rows_value or 0):,}"
        values += (
            f"{int(group.get('attempts') or 0):,}",
            f"{int(group.get('ok') or 0):,}",
            f"{int(group.get('error') or 0):,}",
            _format_duration(group.get("p50_ms")),
            _format_duration(group.get("p95_ms")),
            shown_rows,
            codes or "-",
        )
        rows.append(values)

    widths = _table_widths(headers, rows)
    typer.echo(_table_row(headers, widths))
    for row in rows:
        typer.echo(_table_row(row, widths))


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
        groups = payload.get("groups") or []
        if not groups:
            typer.echo(f"(no receipts in {payload.get('since')}..{payload.get('until')})")
            return
        _print_run_summary(payload, include_pipeline=pipe_id is None)
        return
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


@app.command("lock-show")
def lock_show(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Print the lake lease for a pipeline interval (or 'no lock')."""
    from det.destinations.models import lake_root
    from det.runtime.config import load_pipeline_config
    from det.runtime.lease import lock_path, read_lock

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    config = load_pipeline_config(resolved.path)
    path = lock_path(
        lake_root(config.destination, root, cli_lake_path=lake_path),
        config.name,
        start_iso,
        end_iso,
    )
    payload = read_lock(path)
    if payload is None:
        typer.echo(f"no lock path={path}")
        return
    typer.echo(f"path={path}")
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
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    force: bool = typer.Option(False, "--force", help="Required to delete a live lease"),
    lake_path: str | None = typer.Option(None, "--lake-path"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Force-delete a lake lease. Kill the worker first or you can dual-insert."""
    from det.destinations.models import lake_root
    from det.runtime.approval import lock_release_write_argv
    from det.runtime.config import load_pipeline_config
    from det.runtime.lease import force_release_lock, lock_path, read_lock

    if not force:
        raise typer.BadParameter("--force is required to delete a lock", param_hint="--force")

    root = _project_root(project_root)
    _gate_approval(
        root,
        "lock-release",
        lock_release_write_argv(pipeline, interval_start, interval_end),
        approval,
        require_approval,
    )
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    config = load_pipeline_config(resolved.path)
    path = lock_path(
        lake_root(config.destination, root, cli_lake_path=lake_path),
        config.name,
        start_iso,
        end_iso,
    )
    held = read_lock(path)
    if held is None:
        _consume_approval(root, approval)
        typer.echo(f"no lock path={path}")
        return
    typer.echo(
        f"releasing owner={held.get('owner')} expires_at={held.get('expires_at')} path={path}",
        err=True,
    )
    force_release_lock(path)
    _consume_approval(root, approval)
    typer.echo(f"OK lock-release path={path}")


@app.command("approve")
def approve_cmd(
    plan: Path | None = typer.Option(
        None,
        "--plan",
        help="JSON file from MCP approval_plan or a dry-run payload containing it",
        exists=True,
        dir_okay=False,
        readable=True,
    ),
    command: str | None = typer.Option(
        None, "--command", help="Writing verb (extract, prune, migrate, …)"
    ),
    argv_json: str | None = typer.Option(
        None,
        "--argv-json",
        help="JSON list of canonical argv after `det` (no --approval)",
    ),
    approved_by: str | None = typer.Option(
        None,
        "--approved-by",
        help="Who approved (or set DET_APPROVED_BY); required, not inferred from git",
    ),
    ttl_sec: int | None = typer.Option(
        None,
        "--ttl-sec",
        help="Override DET_APPROVAL_TTL_SEC (default 3600)",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Create a single-use approval record for a later writing CLI command."""
    import json

    from det.runtime.approval import (
        ApprovalError,
        approved_by_from_env,
        create_approval,
        make_plan,
        plan_from_mapping,
    )

    root = _project_root(project_root)
    who = (approved_by or approved_by_from_env() or "").strip()
    try:
        if plan is not None:
            if command is not None or argv_json is not None:
                raise typer.BadParameter(
                    "use --plan or --command/--argv-json, not both",
                    param_hint="--plan",
                )
            doc = json.loads(plan.read_text(encoding="utf-8"))
            if not isinstance(doc, dict):
                raise typer.BadParameter("--plan must be a JSON object", param_hint="--plan")
            stub = plan_from_mapping(doc)
        else:
            if not command or argv_json is None:
                raise typer.BadParameter(
                    "need --plan or both --command and --argv-json",
                    param_hint="--command",
                )
            parsed = json.loads(argv_json)
            if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
                raise typer.BadParameter(
                    "--argv-json must be a JSON list of strings",
                    param_hint="--argv-json",
                )
            stub = make_plan(command, parsed)
        record = create_approval(
            root,
            command=stub.command,
            argv=stub.argv,
            approved_by=who,
            ttl_sec=ttl_sec,
        )
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"invalid JSON: {exc}") from exc
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(record, indent=2))


@app.command("approval-show")
def approval_show_cmd(
    approval_id: str = typer.Argument(..., help="Approval id (apr_…)"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """Print one approval record (expired status is derived at read time)."""
    import json

    from det.runtime.approval import ApprovalError, effective_status, load_approval

    root = _project_root(project_root)
    try:
        record = dict(load_approval(root, approval_id))
        record["status"] = effective_status(record)
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(record, indent=2))


@app.command("list-approvals")
def list_approvals_cmd(
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """List unused, unexpired approvals under .det/approvals/."""
    import json

    from det.runtime.approval import list_unused_approvals

    root = _project_root(project_root)
    records = list_unused_approvals(root)
    typer.echo(json.dumps({"approvals": records}, indent=2))


@app.command("check")
def check_cmd(
    pipeline: str | None = typer.Option(
        None,
        "--pipeline",
        "-p",
        help="Check a single pipeline (default: all under configs/pipelines/)",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Exit 1 on warnings as well as errors",
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON findings"),
) -> None:
    """Validate pipeline structure (schema file, source plugin, optional dbt models)."""
    import json

    from det.runtime.check import (
        check_project,
        findings_payload,
        format_findings,
        has_errors,
        has_warnings,
    )

    root = _project_root(project_root)
    findings = check_project(root, pipeline=pipeline)
    if as_json:
        typer.echo(json.dumps(findings_payload(findings), indent=2))
    else:
        typer.echo(format_findings(findings))
    if has_errors(findings):
        raise typer.Exit(code=1)
    if strict and has_warnings(findings):
        raise typer.Exit(code=1)


@app.command("list-pipelines")
def list_pipelines_cmd(
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    """List canonical pipeline ids under configs/pipelines/."""
    from det.runtime.pipelines import list_pipeline_ids, pipelines_dir

    root = _project_root(project_root)
    ids = list_pipeline_ids(root)
    if not ids:
        typer.echo(f"(none under {pipelines_dir(root)})", err=True)
        raise typer.Exit(code=1)
    for name in ids:
        typer.echo(name)


@app.command("list-sources")
def list_sources_cmd() -> None:
    from det.runtime.registry import list_sources

    for name in list_sources():
        typer.echo(name)


@app.command("list-mappers")
def list_mappers_cmd() -> None:
    """Show migrate mappers and the source-row shape each one expects."""
    from det.runtime.registry import describe_mappers

    described = describe_mappers()
    width = max((len(name) for name, _ in described), default=0)
    for name, summary in described:
        typer.echo(f"{name.ljust(width)}  {summary}" if summary else name)


if __name__ == "__main__":
    app()
