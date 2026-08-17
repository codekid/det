from __future__ import annotations

import sys
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
    "Pipeline ref: canonical id (noaa.storm_events), slash form, "
    "or YAML path under the project"
)
_PROJECT_ROOT_HELP = (
    "Project root (default: DET_PROJECT_ROOT env, else cwd)"
)


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


@app.command("extract")
def extract_raw(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
) -> None:
    """Source → raw data/ + format check + meta/manifest.json."""
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    result = PipelineRunner(root).extract(
        resolved.path,
        interval_start=start_iso,
        interval_end=end_iso,
        overrides=set_,
    )
    typer.echo(
        f"OK extract pipeline={result.pipeline} artifacts={result.artifacts} "
        f"raw={result.raw_dir}"
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
) -> None:
    """Raw data/ → snake_case naming → JSON Schema → bronze."""
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    result = PipelineRunner(root).load(
        resolved.path,
        interval_start=start_iso,
        interval_end=end_iso,
        overrides=set_,
        extract_run_datetime=extract_run_datetime,
    )
    typer.echo(
        f"OK load pipeline={result.pipeline} rows={result.rows} "
        f"partition={result.partition_dir}"
    )


@app.command("run")
def run_pipeline(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
) -> None:
    """extract then load with one shared run-start stamp."""
    from det.runtime.runner import PipelineRunner

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    start_iso, end_iso = _resolve_interval(interval_start, interval_end)
    print("det: run starting…", file=sys.stderr, flush=True)
    result = PipelineRunner(root).run(
        resolved.path,
        interval_start=start_iso,
        interval_end=end_iso,
        overrides=set_,
    )
    typer.echo(
        f"OK pipeline={result.pipeline} rows={result.rows} "
        f"partition={result.partition_dir}"
    )


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
) -> None:
    """Rebuild bronze from raw data/ for an interval."""
    from det.runtime.migrate import BronzeMigrator, MigratePlan

    start, end = _resolve_interval(interval_start, interval_end)
    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    if validate_limit is not None and not dry_run:
        raise typer.BadParameter(
            "--validate-limit requires --dry-run",
            param_hint="--validate-limit",
        )
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
    )
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
) -> None:
    """Run dbt (build/run/test) for local testing. Requires the optional [dbt] extra."""
    from det.runtime.dbt_runner import DbtNotInstalledError, run_dbt

    if command not in {"build", "run", "test"}:
        raise typer.BadParameter(
            "must be one of: build, run, test",
            param_hint="--command",
        )

    root = _project_root(project_root)
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
        "filesystem",
        "--destination-type",
        help="filesystem | duckdb | postgres | iceberg",
    ),
    connection: str | None = typer.Option(
        None,
        "--connection",
        help="DuckDB file path or Postgres DSN (required for duckdb/postgres)",
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
) -> None:
    """Create pipeline YAML + minimal schema + scaffold-dbt models."""
    from det.scaffold.init_pipeline import init_pipeline

    if destination_type not in {"filesystem", "duckdb", "postgres", "iceberg"}:
        raise typer.BadParameter(
            "must be filesystem, duckdb, postgres, or iceberg",
            param_hint="--destination-type",
        )
    root = _project_root(project_root)
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
) -> None:
    """Generate dbt source + stg + silver from a pipeline schema (gold is hand-written)."""
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    config = load_pipeline_config(resolved.path, overrides=set_)
    from det.scaffold.view_warn import collect_view_size_warnings

    result = scaffold_dbt(
        config, project_root=root, force=force, dry_run=dry_run, warn=False
    )
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
) -> None:
    """Delete old bronze extract runs. Never touches raw/. Requires --dry-run or --apply."""
    from det.runtime.config import load_pipeline_config
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
            f"DRY-RUN prune pipeline={config.name} keep={keep} "
            f"would_remove={plan.remove_count}"
        )
        for ref in plan.to_remove:
            loc = str(ref.path) if ref.path is not None else "duckdb"
            typer.echo(
                f"  remove interval={ref.interval_start}..{ref.interval_end} "
                f"run={ref.extract_run_datetime} ({loc})"
            )
        return

    removed = pruner.apply(config, plan)
    typer.echo(
        f"OK prune pipeline={config.name} keep={keep} removed={removed}"
    )


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
        format_findings,
        has_errors,
        has_warnings,
    )

    root = _project_root(project_root)
    findings = check_project(root, pipeline=pipeline)
    if as_json:
        typer.echo(
            json.dumps(
                {
                    "ok": not has_errors(findings),
                    "error_count": sum(1 for f in findings if f.severity == "error"),
                    "warning_count": sum(
                        1 for f in findings if f.severity == "warning"
                    ),
                    "findings": [f.to_dict() for f in findings],
                },
                indent=2,
            )
        )
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
