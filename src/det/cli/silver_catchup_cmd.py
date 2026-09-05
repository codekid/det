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
    extract_lookback: str | None = typer.Option(
        None,
        "--extract-lookback",
        help=(
            "Mode A: only intervals touched by bronze extract runs in this "
            "lookback (e.g. 48h, 7d). Cannot combine with -s/-e."
        ),
    ),
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
    from det.runtime.silver_catchup import (
        diff_bronze_silver,
        diff_bronze_silver_fleet,
        validate_catchup_candidate_scope,
    )

    if all_pipelines == (pipeline is not None):
        raise typer.BadParameter(
            "exactly one of --pipeline / --all-pipelines is required",
            param_hint="--pipeline/--all-pipelines",
        )
    try:
        validate_catchup_candidate_scope(
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--extract-lookback") from exc

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
                extract_lookback=extract_lookback,
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
                extract_lookback=extract_lookback,
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
    mode = payload.get("candidate_mode") or "full"
    mode_bit = f" mode={mode}"
    if payload.get("extract_lookback"):
        mode_bit += f" lookback={payload['extract_lookback']}"
    typer.echo(
        f"catchup_count={payload.get('catchup_count', len(catchup))} "
        f"ok_count={ok_n} stale_siblings={stale_n}{mode_bit}"
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
    extract_lookback: str | None = typer.Option(
        None,
        "--extract-lookback",
        help=(
            "Mode A: plan from bronze extract runs in this lookback "
            "(e.g. 48h, 7d). Cannot combine with -s/-e."
        ),
    ),
    limit: int = typer.Option(200, "--limit", help="Max runs considered per pipeline"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview immutable manifest only"),
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Write ops/silver_catchup/<manifest_id>.json (immutable)",
    ),
    manifest_id: str | None = typer.Option(
        None,
        "--manifest-id",
        help="Immutable id from dry-run (scm_…); required with --content-digest on --apply",
    ),
    content_digest: str | None = typer.Option(
        None,
        "--content-digest",
        help="Coverage digest from dry-run (sha256:…); must match live plan on --apply",
    ),
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
    """Build or apply an immutable silver catch-up manifest under ops/silver_catchup/."""
    from det.errors import DetConflictError
    from det.runtime.approval import silver_catchup_plan_write_argv
    from det.runtime.settings import use_settings
    from det.runtime.silver_catchup import (
        assert_catchup_digest_matches,
        manifest_relpath_for_root,
        plan_catchup_manifest,
        validate_catchup_candidate_scope,
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
    try:
        validate_catchup_candidate_scope(
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--extract-lookback") from exc
    has_mid = bool(manifest_id and str(manifest_id).strip())
    has_digest = bool(content_digest and str(content_digest).strip())
    if has_mid != has_digest:
        raise typer.BadParameter(
            "--manifest-id and --content-digest must be passed together",
            param_hint="--manifest-id/--content-digest",
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
        from det.runtime.approval import require_approvals_enabled

        need_bound = bool(approval) or require_approval or require_approvals_enabled()
        if need_bound and not (has_mid and has_digest):
            raise typer.BadParameter(
                "--apply under approval requires --manifest-id and --content-digest "
                "from the dry-run approval_plan",
                param_hint="--manifest-id/--content-digest",
            )
        if has_mid and has_digest:
            gate_argv = silver_catchup_plan_write_argv(
                pipeline=pipe_id,
                all_pipelines=all_pipelines,
                interval_start=start,
                interval_end=end,
                extract_lookback=extract_lookback,
                limit=limit,
                manifest_id=manifest_id,
                content_digest=content_digest,
                lake_path=lake_path,
                lake_path_raw=lake_path_raw,
                lake_path_bronze=lake_path_bronze,
                lake_path_ops=lake_path_ops,
            )
        else:
            # Ungated local apply: allocate id at plan time; claim is a no-op.
            gate_argv = ["silver-catchup-plan", "--apply"]
        claimed = _gate_approval(
            root,
            "silver-catchup-plan",
            gate_argv,
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
            extract_lookback=extract_lookback,
            limit=limit,
            manifest_id=manifest_id if has_mid else None,
        )
        if dry_run:
            if as_json:
                typer.echo(json.dumps(planned, indent=2, default=str))
            else:
                runs = planned["manifest"].get("runs") or []
                mode = planned.get("candidate_mode") or "full"
                mode_bit = f" mode={mode}"
                if planned.get("extract_lookback"):
                    mode_bit += f" lookback={planned['extract_lookback']}"
                typer.echo(
                    f"DRY-RUN silver-catchup-plan runs={len(runs)} "
                    f"manifest_id={planned['manifest_id']} "
                    f"content_digest={planned['content_digest']} "
                    f"path={planned['manifest_relpath']}{mode_bit}"
                )
                for row in runs:
                    typer.echo(
                        f"  {row.get('pipeline')} "
                        f"{row.get('interval_start')}..{row.get('interval_end')} "
                        f"run={row.get('extract_run_datetime')}"
                    )
            return

        if has_digest:
            try:
                assert_catchup_digest_matches(
                    planned["manifest"], expected_digest=str(content_digest)
                )
            except ValueError as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=1) from exc

        try:
            with _claimed_approval_work(claimed, approval):
                path = write_catchup_manifest(
                    planned["manifest"],
                    project_root=root,
                    settings=settings,
                )
        except DetConflictError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
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
            f"manifest_id={planned['manifest_id']} "
            f"runs={len(planned['manifest'].get('runs') or [])}"
        )


@app.command("silver-catchup-cleanup")
def silver_catchup_cleanup_cmd(
    ctx: typer.Context,
    list_tables: bool = typer.Option(
        False,
        "--list",
        help="List BigQuery _det_catchup_runs_* external tables (read-only)",
    ),
    manifest_id: str | None = typer.Option(
        None,
        "--manifest-id",
        help="Drop one catch-up external table (scm_…)",
    ),
    older_than: str | None = typer.Option(
        None,
        "--older-than",
        help=(
            "Drop tables whose BQ created time is older than this duration "
            "(e.g. 7d, 48h). Cannot combine with --manifest-id."
        ),
    ),
    apply: bool = typer.Option(
        False,
        "--apply",
        help="Perform BigQuery external-table deletes (default is dry-run preview)",
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(
        False, "--require-approval", help=_REQUIRE_APPROVAL_HELP
    ),
) -> None:
    """List or drop BigQuery catch-up external tables (_det_catchup_runs_<scm_…>).

    Heal does not auto-drop these tables. After verify, clean up by manifest id
    or retention (--older-than). DuckDB heals never create these tables.
    """
    from det.runtime.approval import silver_catchup_cleanup_write_argv
    from det.runtime.silver_catchup import (
        apply_bq_catchup_cleanup,
        list_bq_catchup_external_tables,
        plan_bq_catchup_cleanup,
        validate_bq_catchup_cleanup_scope,
    )

    mid = str(manifest_id).strip() if manifest_id else ""
    older = str(older_than).strip() if older_than else ""
    if list_tables and apply:
        raise typer.BadParameter(
            "--list cannot combine with --apply",
            param_hint="--list/--apply",
        )
    if list_tables and mid:
        raise typer.BadParameter(
            "--list cannot combine with --manifest-id",
            param_hint="--list/--manifest-id",
        )
    if not list_tables and not mid and not older:
        raise typer.BadParameter(
            "require --list, --manifest-id, or --older-than",
            param_hint="--list/--manifest-id/--older-than",
        )
    try:
        validate_bq_catchup_cleanup_scope(
            manifest_id=mid or None,
            older_than=older or None,
            list_mode=list_tables,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    if list_tables:
        try:
            rows = list_bq_catchup_external_tables(older_than=older or None)
        except (ValueError, RuntimeError) as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
        payload = {
            "tables": rows,
            "table_count": len(rows),
            "older_than": older or None,
        }
        if as_json:
            typer.echo(json.dumps(payload, indent=2, default=str))
        else:
            typer.echo(
                f"OK silver-catchup-cleanup list count={len(rows)}"
                + (f" older_than={older}" if older else "")
            )
            for row in rows:
                created = row.get("created") or "?"
                typer.echo(f"  {row.get('relation')} created={created}")
        return

    claimed = False
    root = _project_root(None)
    if apply:
        gate_argv = silver_catchup_cleanup_write_argv(
            manifest_id=mid or None,
            older_than=older or None,
        )
        claimed = _gate_approval(
            root,
            "silver-catchup-cleanup",
            gate_argv,
            approval,
            require_approval,
            ctx=ctx,
        )
        with _claimed_approval_work(claimed, approval):
            try:
                result = apply_bq_catchup_cleanup(
                    manifest_id=mid or None,
                    older_than=older or None,
                )
            except (ValueError, RuntimeError) as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=1) from exc
        _consume_approval(root, approval)
        if as_json:
            typer.echo(json.dumps({**result, "apply": True}, indent=2, default=str))
        else:
            typer.echo(
                f"OK silver-catchup-cleanup dropped={result.get('dropped_count', 0)} "
                f"targets={result.get('target_count', 0)}"
            )
            for row in result.get("results") or []:
                typer.echo(
                    f"  {row.get('relation')} dropped={row.get('dropped')}"
                )
        return

    try:
        planned = plan_bq_catchup_cleanup(
            manifest_id=mid or None,
            older_than=older or None,
        )
    except (ValueError, RuntimeError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    if as_json:
        typer.echo(json.dumps({**planned, "dry_run": True}, indent=2, default=str))
    else:
        typer.echo(
            f"DRY-RUN silver-catchup-cleanup targets={planned.get('target_count', 0)} "
            f"mode={planned.get('mode')}"
            + (f" older_than={older}" if older else "")
            + (f" manifest_id={mid}" if mid else "")
        )
        for row in planned.get("targets") or []:
            existed = row.get("existed")
            typer.echo(
                f"  {row.get('relation')} existed={existed} "
                f"created={row.get('created') or '?'}"
            )
