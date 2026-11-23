"""CLI: export existing bronze to BigQuery."""

from __future__ import annotations

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
    _approval_lake_kwargs,
    _claimed_approval_work,
    _consume_approval,
    _gate_approval,
    _project_root,
    _resolve_interval,
    _resolve_pipeline,
    _settings,
)
from det.destinations.models import lake_roots_for
from det.ingestion.bq_writer import (
    bq_dataset_for_config,
    bq_project_from_settings,
    bq_table_for_config,
    write_to_bigquery,
)
from det.runtime.approval import export_bq_write_argv
from det.runtime.bronze_export import load_bronze_export_rows
from det.runtime.config import load_pipeline_config, resolve_path
from det.runtime.lease import (
    LeaseFencedError,
    LeaseHeldError,
    assert_lease_held,
    pipeline_lease,
    refresh_bronze_locks,
    resolve_lease_options,
)
from det.runtime.settings import use_settings
from det.validation.jsonschema_validator import load_json_schema


@app.command("export-bq")
def export_bq(
    ctx: typer.Context,
    pipeline: str = typer.Option(..., "--pipeline", "-p", help=_PIPELINE_HELP),
    interval_start: str = typer.Option(..., "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_run_datetime: str | None = typer.Option(
        None,
        "--extract-run-datetime",
        help="Optional single extract run to export (default: all runs in the window).",
    ),
    bq_dataset: str | None = typer.Option(
        None,
        "--bq-dataset",
        help=(
            "Override BQ dataset id (default: destination.bq_dataset or "
            "{prefix}_{provider}). Project stays DET_GCP_PROJECT."
        ),
    ),
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
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Load existing filesystem/Iceberg bronze into BigQuery (replace-by-extract-run)."""

    start, end = _resolve_interval(interval_start, interval_end)
    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root)
    settings = _settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        lock_ttl_sec=lock_ttl_sec,
    )
    # Load before claiming so config/schema errors never leave an orphan claim.
    config = load_pipeline_config(resolved.path, overrides=set_)
    json_schema = load_json_schema(resolve_path(root, config.schema_path))
    claimed = _gate_approval(
        root,
        "export-bq",
        export_bq_write_argv(
            resolved.canonical_id,
            start,
            interval_end=end,
            extract_run_datetime=extract_run_datetime,
            bq_dataset=bq_dataset,
            set_=set_,
            **_approval_lake_kwargs(settings),
        ),
        approval,
        require_approval,
        ctx=ctx,
        settings=settings,
    )
    total_rows = 0
    batches_written = 0
    project = ""
    dataset = ""
    table = ""
    try:
        with _claimed_approval_work(claimed, approval, root, settings=settings):
            with use_settings(settings):
                batches = load_bronze_export_rows(
                    config,
                    project_root=root,
                    interval_start=start,
                    interval_end=end,
                    extract_run_datetime=extract_run_datetime,
                    settings=settings,
                )
                if not batches:
                    typer.echo(
                        f"No bronze rows found for pipeline={config.name} "
                        f"interval=[{start}, {end})",
                        err=True,
                    )
                    raise typer.Exit(code=1)
                project = bq_project_from_settings(settings)
                dataset = bq_dataset_for_config(
                    config, settings, bq_dataset_override=bq_dataset
                )
                table = bq_table_for_config(config)
                ops_lake = lake_roots_for(root, settings=settings).ops
                lease_opts = resolve_lease_options(
                    settings=settings,
                    pipeline=config,
                    ttl_sec=settings.effective_lock_ttl(lock_ttl_sec),
                    owner=settings.lock_owner,
                    enabled=settings.locks_enabled,
                )
                # Per-run lease (same interval grain as load/migrate) so two
                # export-bq processes cannot DELETE+APPEND the same identity.
                for identity, rows in batches:
                    run_start, run_end, _run = identity
                    with pipeline_lease(
                        ops_lake,
                        pipeline=config.name,
                        interval_start=run_start,
                        interval_end=run_end,
                        command="export-bq",
                        ttl_sec=settings.effective_lock_ttl(lock_ttl_sec),
                        owner=settings.lock_owner,
                        enabled=settings.locks_enabled,
                        options=lease_opts,
                        resolve_secret=settings.resolve_secret,
                    ) as lease:
                        lease_store = None if lease is None else lease.store

                        def refresh_locks(
                            *,
                            _lease=lease,
                            _store=lease_store,
                        ) -> None:
                            refresh_bronze_locks(
                                pipeline_lease=_lease,
                                dataset_lock=None,
                                lease_store=_store,
                            )

                        def ensure_held(
                            *,
                            _lease=lease,
                            _store=lease_store,
                        ) -> None:
                            assert_lease_held(_lease, store=_store)

                        write_to_bigquery(
                            rows,
                            config=config,
                            json_schema=json_schema,
                            settings=settings,
                            bq_dataset_override=bq_dataset,
                            run_identity=identity,
                            on_chunk=refresh_locks,
                            ensure_held=ensure_held,
                        )
                        ensure_held()
                        refresh_locks()
                    total_rows += len(rows)
                    batches_written += 1
    except (LeaseHeldError, LeaseFencedError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    _consume_approval(root, approval, settings=settings)
    typer.echo(
        f"OK export-bq pipeline={config.name} "
        f"table={project}.{dataset}.{table} "
        f"batches={batches_written} rows={total_rows}"
    )
