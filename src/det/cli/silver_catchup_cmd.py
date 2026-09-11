"""CLI: bronze↔silver catch-up status/plan/apply/build/verify/cleanup (+ legacy aliases)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

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
    _analytics_exclude,
    _approval_lake_kwargs,
    _claimed_approval_work,
    _consume_approval,
    _gate_approval,
    _project_root,
    _resolve_interval,
    _resolve_pipeline,
    _settings,
)

catchup_app = typer.Typer(
    name="silver-catchup",
    help=(
        "Bronze↔silver catch-up. Routine default is --extract-lookback 48h; "
        "use --census for a full-lake audit. DuckDB and BigQuery-on-GCS share "
        "the same verbs."
    ),
    no_args_is_help=True,
)
app.add_typer(catchup_app, name="silver-catchup")

_LOOKBACK_HELP = (
    "Mode A lookback (e.g. 48h, 7d). Default 48h when neither --census nor "
    "-s/-e is set. Cannot combine with --census or -s/-e."
)
_CENSUS_HELP = (
    "Mode B full-lake census (no lookback). Required for a full audit; "
    "omit used to mean census — that is no longer true."
)


def _resolve_effective_lookback(
    *,
    interval_start: str | None,
    interval_end: str | None,
    extract_lookback: str | None,
    census: bool,
) -> str | None:
    from det.runtime.silver_catchup import resolve_catchup_candidate_scope

    try:
        return resolve_catchup_candidate_scope(
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
            census=census,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--extract-lookback/--census") from exc


def _census_for_argv(
    *,
    census: bool,
    effective_lookback: str | None,
    interval_start: str | None,
    interval_end: str | None,
) -> bool:
    """Whether approval argv should include ``--census`` (Mode B full only)."""
    if census:
        return True
    if effective_lookback is not None:
        return False
    if interval_start is not None or interval_end is not None:
        return False
    return False


def _print_status_payload(payload: dict[str, Any], *, as_json: bool) -> None:
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


def _run_status(
    *,
    pipeline: str | None,
    all_pipelines: bool,
    interval_start: str | None,
    interval_end: str | None,
    extract_lookback: str | None,
    census: bool,
    limit: int,
    project_root: Path | None,
    lake_path: str | None,
    lake_path_raw: str | None,
    lake_path_bronze: str | None,
    lake_path_ops: str | None,
    as_json: bool,
    fail_if_catchup: bool = False,
) -> dict[str, Any]:
    from det.runtime.settings import use_settings
    from det.runtime.silver_catchup import (
        diff_bronze_silver,
        diff_bronze_silver_fleet,
    )

    if all_pipelines == (pipeline is not None):
        raise typer.BadParameter(
            "exactly one of --pipeline / --all-pipelines is required",
            param_hint="--pipeline/--all-pipelines",
        )
    effective = _resolve_effective_lookback(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
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
                extract_lookback=effective,
                limit=limit,
            )
        else:
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
                extract_lookback=effective,
                limit=limit,
            )

    _print_status_payload(payload, as_json=as_json)
    if fail_if_catchup and int(payload.get("catchup_count") or 0) != 0:
        raise typer.Exit(code=1)
    return payload


def _run_plan(
    ctx: typer.Context,
    *,
    pipeline: str | None,
    all_pipelines: bool,
    interval_start: str | None,
    interval_end: str | None,
    extract_lookback: str | None,
    census: bool,
    limit: int,
    dry_run: bool,
    apply: bool,
    manifest_id: str | None,
    content_digest: str | None,
    project_root: Path | None,
    lake_path: str | None,
    lake_path_raw: str | None,
    lake_path_bronze: str | None,
    lake_path_ops: str | None,
    as_json: bool,
    approval: str | None,
    require_approval: bool,
) -> None:
    from det.errors import DetConflictError
    from det.runtime.approval import make_plan, silver_catchup_plan_write_argv
    from det.runtime.settings import use_settings
    from det.runtime.silver_catchup import (
        assert_catchup_digest_matches,
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
    effective = _resolve_effective_lookback(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
    )
    census_effective = _census_for_argv(
        census=census,
        effective_lookback=effective,
        interval_start=interval_start,
        interval_end=interval_end,
    )

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
                extract_lookback=effective,
                census=census_effective,
                limit=limit,
                manifest_id=manifest_id,
                content_digest=content_digest,
                **_approval_lake_kwargs(settings),
            )
        else:
            gate_argv = ["silver-catchup-plan", "--apply"]
        claimed = _gate_approval(
            root,
            "silver-catchup-plan",
            gate_argv,
            approval,
            require_approval,
            ctx=ctx,
            settings=settings,
        )

    with use_settings(settings):
        planned = plan_catchup_manifest(
            project_root=root,
            pipeline=pipe_id,
            all_pipelines=all_pipelines,
            interval_start=start,
            interval_end=end,
            extract_lookback=effective,
            limit=limit,
            manifest_id=manifest_id if has_mid else None,
        )
        if dry_run:
            mid = str(planned["manifest_id"])
            digest = str(planned["content_digest"])
            write_argv = silver_catchup_plan_write_argv(
                pipeline=pipe_id,
                all_pipelines=all_pipelines,
                interval_start=start,
                interval_end=end,
                extract_lookback=effective,
                census=census_effective,
                limit=limit,
                manifest_id=mid,
                content_digest=digest,
                **_approval_lake_kwargs(settings),
            )
            plan = make_plan("silver-catchup-plan", write_argv)
            approval_plan = {
                "command": plan.command,
                "argv": list(plan.argv),
                "plan_digest": plan.plan_digest,
            }
            payload = {
                **planned,
                "dry_run": True,
                "approval_plan": approval_plan,
                "next_steps": (
                    "Operator: det approve --plan <approval_plan> --approved-by <id>. "
                    "Later: det silver-catchup apply "
                    f"--manifest-id {mid} --content-digest {digest} --approval <id> "
                    "(or det silver-catchup-plan --apply …); then "
                    f"det silver-catchup build --manifest-id {mid} --approval <dbt_id>."
                ),
            }
            if as_json:
                typer.echo(json.dumps(payload, indent=2, default=str))
            else:
                runs = planned["manifest"].get("runs") or []
                mode = planned.get("candidate_mode") or "full"
                mode_bit = f" mode={mode}"
                if planned.get("extract_lookback"):
                    mode_bit += f" lookback={planned['extract_lookback']}"
                typer.echo(
                    f"DRY-RUN silver-catchup-plan runs={len(runs)} "
                    f"manifest_id={mid} content_digest={digest} "
                    f"path={planned['manifest_relpath']}{mode_bit}"
                )
                for row in runs:
                    typer.echo(
                        f"  {row.get('pipeline')} "
                        f"{row.get('interval_start')}..{row.get('interval_end')} "
                        f"run={row.get('extract_run_datetime')}"
                    )
                typer.echo(f"approval_plan digest={plan.plan_digest} argv={' '.join(plan.argv)}")
                typer.echo(payload["next_steps"])
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
            with _claimed_approval_work(claimed, approval, root, settings=settings):
                path = write_catchup_manifest(
                    planned["manifest"],
                    project_root=root,
                    settings=settings,
                )
        except DetConflictError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
        _consume_approval(root, approval, settings=settings)

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


def _cleanup_is_duckdb() -> bool:
    target = (os.environ.get("DET_DBT_TARGET") or "").strip().lower()
    if not target:
        return True
    return target not in {"bigquery", "bq"}


def _run_cleanup(
    ctx: typer.Context,
    *,
    list_tables: bool,
    manifest_id: str | None,
    older_than: str | None,
    created_before: str | None,
    apply: bool,
    as_json: bool,
    approval: str | None,
    require_approval: bool,
) -> None:
    from det.runtime.approval import (
        require_approvals_enabled,
        silver_catchup_cleanup_write_argv,
    )
    from det.runtime.silver_catchup import (
        apply_bq_catchup_cleanup,
        list_bq_catchup_external_tables,
        plan_bq_catchup_cleanup,
        validate_bq_catchup_cleanup_scope,
    )

    if _cleanup_is_duckdb():
        payload = {
            "cleanup_skipped": "duckdb",
            "note": (
                "BigQuery catch-up external tables are not used for DuckDB heals; nothing to clean."
            ),
        }
        if as_json:
            typer.echo(json.dumps(payload, indent=2))
        else:
            typer.echo("OK silver-catchup-cleanup skipped=duckdb")
        return

    mid = str(manifest_id).strip() if manifest_id else ""
    older = str(older_than).strip() if older_than else ""
    before = str(created_before).strip() if created_before else ""
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
    if not list_tables and not mid and not older and not before:
        raise typer.BadParameter(
            "require --list, --manifest-id, --older-than, or --created-before",
            param_hint="--list/--manifest-id/--older-than/--created-before",
        )
    try:
        validate_bq_catchup_cleanup_scope(
            manifest_id=mid or None,
            older_than=older or None,
            created_before=before or None,
            list_mode=list_tables,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    if list_tables:
        try:
            rows = list_bq_catchup_external_tables(
                older_than=older or None,
                created_before=before or None,
            )
        except (ValueError, RuntimeError) as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
        payload = {
            "tables": rows,
            "table_count": len(rows),
            "older_than": older or None,
            "created_before": before or None,
        }
        if as_json:
            typer.echo(json.dumps(payload, indent=2, default=str))
        else:
            typer.echo(
                f"OK silver-catchup-cleanup list count={len(rows)}"
                + (f" older_than={older}" if older else "")
                + (f" created_before={before}" if before else "")
            )
            for row in rows:
                created = row.get("created") or "?"
                typer.echo(f"  {row.get('relation')} created={created}")
        return

    claimed = False
    root = _project_root(None)
    if apply:
        need_bound = bool(approval) or require_approval or require_approvals_enabled()
        if need_bound and older and not before and not mid:
            raise typer.BadParameter(
                "approved --apply retention requires --created-before from "
                "dry-run (relative --older-than would drift)",
                param_hint="--created-before",
            )
        apply_before = before or None
        if not mid and not apply_before and older:
            from det.runtime.silver_catchup import resolve_bq_catchup_cleanup_cutoff

            _cutoff, apply_before, _older = resolve_bq_catchup_cleanup_cutoff(older_than=older)
        if mid:
            gate_argv = silver_catchup_cleanup_write_argv(manifest_id=mid)
        elif apply_before:
            gate_argv = silver_catchup_cleanup_write_argv(created_before=apply_before)
        else:
            raise typer.BadParameter(
                "require --manifest-id or --created-before for --apply",
                param_hint="--manifest-id/--created-before",
            )
        claimed = _gate_approval(
            root,
            "silver-catchup-cleanup",
            gate_argv,
            approval,
            require_approval,
            ctx=ctx,
        )
        with _claimed_approval_work(claimed, approval, root):
            try:
                result = apply_bq_catchup_cleanup(
                    manifest_id=mid or None,
                    created_before=apply_before,
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
                typer.echo(f"  {row.get('relation')} dropped={row.get('dropped')}")
        return

    try:
        planned = plan_bq_catchup_cleanup(
            manifest_id=mid or None,
            older_than=older or None,
            created_before=before or None,
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
            + (
                f" created_before={planned.get('created_before')}"
                if planned.get("created_before")
                else ""
            )
            + (f" older_than={planned.get('older_than')}" if planned.get("older_than") else "")
            + (f" manifest_id={mid}" if mid else "")
        )
        for row in planned.get("targets") or []:
            existed = row.get("existed")
            typer.echo(
                f"  {row.get('relation')} existed={existed} created={row.get('created') or '?'}"
            )


@catchup_app.command("status")
def silver_catchup_status_cmd(
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Single pipeline. {_PIPELINE_HELP}"
    ),
    all_pipelines: bool = typer.Option(
        False, "--all-pipelines", help="Diff every pipeline under configs/pipelines/"
    ),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_lookback: str | None = typer.Option(
        None, "--extract-lookback", "--lookback", help=_LOOKBACK_HELP
    ),
    census: bool = typer.Option(False, "--census", help=_CENSUS_HELP),
    limit: int = typer.Option(200, "--limit"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Compare latest bronze extract-run per interval to silver coverage (read-only)."""
    _run_status(
        pipeline=pipeline,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
        limit=limit,
        project_root=project_root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        as_json=as_json,
    )


@catchup_app.command("verify")
def silver_catchup_verify_cmd(
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Single pipeline. {_PIPELINE_HELP}"
    ),
    all_pipelines: bool = typer.Option(False, "--all-pipelines"),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_lookback: str | None = typer.Option(
        None, "--extract-lookback", "--lookback", help=_LOOKBACK_HELP
    ),
    census: bool = typer.Option(False, "--census", help=_CENSUS_HELP),
    limit: int = typer.Option(200, "--limit"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Re-diff with the same scope; exit 1 if catchup_count != 0."""
    _run_status(
        pipeline=pipeline,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
        limit=limit,
        project_root=project_root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        as_json=as_json,
        fail_if_catchup=True,
    )


@catchup_app.command("plan")
def silver_catchup_plan_group_cmd(
    ctx: typer.Context,
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Single pipeline. {_PIPELINE_HELP}"
    ),
    all_pipelines: bool = typer.Option(False, "--all-pipelines"),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_lookback: str | None = typer.Option(
        None, "--extract-lookback", "--lookback", help=_LOOKBACK_HELP
    ),
    census: bool = typer.Option(False, "--census", help=_CENSUS_HELP),
    limit: int = typer.Option(200, "--limit"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Preview immutable catch-up manifest + approval_plan (never writes)."""
    _run_plan(
        ctx,
        pipeline=pipeline,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
        limit=limit,
        dry_run=True,
        apply=False,
        manifest_id=None,
        content_digest=None,
        project_root=project_root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        as_json=as_json,
        approval=None,
        require_approval=False,
    )


@catchup_app.command("apply")
def silver_catchup_apply_group_cmd(
    ctx: typer.Context,
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Single pipeline. {_PIPELINE_HELP}"
    ),
    all_pipelines: bool = typer.Option(False, "--all-pipelines"),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_lookback: str | None = typer.Option(
        None, "--extract-lookback", "--lookback", help=_LOOKBACK_HELP
    ),
    census: bool = typer.Option(False, "--census", help=_CENSUS_HELP),
    limit: int = typer.Option(200, "--limit"),
    manifest_id: str | None = typer.Option(None, "--manifest-id"),
    content_digest: str | None = typer.Option(None, "--content-digest"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Write ops/silver_catchup/<id>.json. Digests match silver-catchup-plan --apply."""
    _run_plan(
        ctx,
        pipeline=pipeline,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
        limit=limit,
        dry_run=False,
        apply=True,
        manifest_id=manifest_id,
        content_digest=content_digest,
        project_root=project_root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        as_json=as_json,
        approval=approval,
        require_approval=require_approval,
    )


@catchup_app.command("build")
def silver_catchup_build_cmd(
    ctx: typer.Context,
    manifest_id: str = typer.Option(
        ...,
        "--manifest-id",
        help="Immutable catch-up manifest id (scm_…) from apply",
    ),
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Optional pipeline select for dbt. {_PIPELINE_HELP}"
    ),
    project_dir: Path | None = typer.Option(None, "--project-dir"),
    target: str | None = typer.Option(None, "--target"),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    set_: list[str] = typer.Option([], "--set"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Run catch-up dbt heal (same approval digest as ``det dbt --catchup``).

    Gates with ``dbt`` argv / ``--catchup-manifest`` — do not forward this
    command's Typer context into ``dbt_cmd`` (``--manifest-id`` would fail
    unbound-flag against the dbt bound set).
    """
    from det.runtime.approval import dbt_write_argv
    from det.runtime.dbt_runner import DbtNotInstalledError, run_dbt
    from det.runtime.settings import use_settings

    mid = str(manifest_id).strip()
    if not mid:
        raise typer.BadParameter("manifest id required", param_hint="--manifest-id")

    # Fail-closed: COMMANDLINE flags must map into the dbt digest or be neutral.
    # --manifest-id is catchup_manifest in argv (see _unbound_params_for_dbt_catchup_build).
    from det.runtime.approval import require_approvals_enabled

    need_bound = bool(approval) or require_approval or require_approvals_enabled()
    if need_bound:
        unbound = _unbound_params_for_dbt_catchup_build(ctx)
        if unbound:
            flags = ", ".join("--" + n.rstrip("_").replace("_", "-") for n in unbound)
            typer.echo(
                f"approval_unbound_flag: {flags} is not covered by the approved plan for "
                f"'dbt'. Re-approve with the flag included, or drop it.",
                err=True,
            )
            raise typer.Exit(code=1)

    root = _project_root(project_root)
    resolved = _resolve_pipeline(pipeline, root) if pipeline is not None else None
    settings = _settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
    gate_argv = dbt_write_argv(
        resolved.canonical_id if resolved else None,
        command="build",
        select=None,
        full_refresh=False,
        catchup=True,
        catchup_manifest=mid,
        target=target,
        set_=set_ or None,
        **_approval_lake_kwargs(settings),
    )
    # ctx=None: unbound already checked above with manifest_id remapped.
    claimed = _gate_approval(
        root, "dbt", gate_argv, approval, require_approval, ctx=None, settings=settings
    )
    pipe = resolved.path if resolved is not None else None
    try:
        with (
            _claimed_approval_work(claimed, approval, root, settings=settings),
            use_settings(settings),
        ):
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
                command="build",
                project_dir=project_dir,
                select=None,
                exclude=_analytics_exclude(None),
                target=target,
                full_refresh=False,
                catchup=True,
                catchup_manifest=mid,
                lake_path=lake_path,
                pipeline=pipe,
                pipeline_overrides=set_ or None,
                dry_run=False,
            )
            if result.returncode != 0:
                raise typer.Exit(code=result.returncode)
            _consume_approval(root, approval, settings=settings)
    except DbtNotInstalledError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    except FileNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"RUN dbt={' '.join(result.command)}")
    sel = " ".join(result.select) if result.select else "(all)"
    typer.echo(f"  select={sel}")
    if result.lake_path:
        typer.echo(f"  DET_LAKE_PATH={result.lake_path}")
    if result.bronze_source:
        typer.echo(f"  DET_BRONZE_SOURCE={result.bronze_source}")
    typer.echo(f"OK dbt finished exit={result.returncode}")


def _unbound_params_for_dbt_catchup_build(ctx: typer.Context) -> list[str]:
    """Unbound COMMANDLINE flags vs the ``dbt`` digest, mapping ``manifest_id``.

    ``silver-catchup build --manifest-id`` is the same binding as
    ``det dbt --catchup --catchup-manifest``; treat ``manifest_id`` as covered.
    """
    from det.cli.common import _BOUND_PARAMS, _NEUTRAL_PARAMS

    bound = _BOUND_PARAMS.get("dbt", frozenset()) | {"catchup_manifest"}
    unbound: list[str] = []
    for name in ctx.params:
        if name == "manifest_id":
            continue
        if name in bound or name in _NEUTRAL_PARAMS:
            continue
        source = ctx.get_parameter_source(name)
        if getattr(source, "name", None) == "COMMANDLINE":
            unbound.append(name)
    return sorted(unbound)


@catchup_app.command("cleanup")
def silver_catchup_cleanup_group_cmd(
    ctx: typer.Context,
    list_tables: bool = typer.Option(False, "--list"),
    manifest_id: str | None = typer.Option(None, "--manifest-id"),
    older_than: str | None = typer.Option(None, "--older-than"),
    created_before: str | None = typer.Option(None, "--created-before"),
    apply: bool = typer.Option(False, "--apply"),
    as_json: bool = typer.Option(False, "--json"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """List/drop BQ catch-up external tables; no-op skip on DuckDB."""
    _run_cleanup(
        ctx,
        list_tables=list_tables,
        manifest_id=manifest_id,
        older_than=older_than,
        created_before=created_before,
        apply=apply,
        as_json=as_json,
        approval=approval,
        require_approval=require_approval,
    )


@app.command("silver-catchup-diff")
def silver_catchup_diff_cmd(
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Single pipeline. {_PIPELINE_HELP}"
    ),
    all_pipelines: bool = typer.Option(False, "--all-pipelines"),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_lookback: str | None = typer.Option(
        None, "--extract-lookback", "--lookback", help=_LOOKBACK_HELP
    ),
    census: bool = typer.Option(False, "--census", help=_CENSUS_HELP),
    limit: int = typer.Option(200, "--limit"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON"),
) -> None:
    """Alias for ``det silver-catchup status``."""
    _run_status(
        pipeline=pipeline,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
        limit=limit,
        project_root=project_root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        as_json=as_json,
    )


@app.command("silver-catchup-plan")
def silver_catchup_plan_cmd(
    ctx: typer.Context,
    pipeline: str | None = typer.Option(
        None, "--pipeline", "-p", help=f"Single pipeline. {_PIPELINE_HELP}"
    ),
    all_pipelines: bool = typer.Option(False, "--all-pipelines"),
    interval_start: str | None = typer.Option(None, "--interval-start", "-s"),
    interval_end: str | None = typer.Option(None, "--interval-end", "-e"),
    extract_lookback: str | None = typer.Option(
        None, "--extract-lookback", "--lookback", help=_LOOKBACK_HELP
    ),
    census: bool = typer.Option(False, "--census", help=_CENSUS_HELP),
    limit: int = typer.Option(200, "--limit"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    apply: bool = typer.Option(False, "--apply"),
    manifest_id: str | None = typer.Option(None, "--manifest-id"),
    content_digest: str | None = typer.Option(None, "--content-digest"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
    as_json: bool = typer.Option(False, "--json"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Alias for ``det silver-catchup plan`` / ``apply``."""
    _run_plan(
        ctx,
        pipeline=pipeline,
        all_pipelines=all_pipelines,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
        limit=limit,
        dry_run=dry_run,
        apply=apply,
        manifest_id=manifest_id,
        content_digest=content_digest,
        project_root=project_root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
        as_json=as_json,
        approval=approval,
        require_approval=require_approval,
    )


@app.command("silver-catchup-cleanup")
def silver_catchup_cleanup_cmd(
    ctx: typer.Context,
    list_tables: bool = typer.Option(False, "--list"),
    manifest_id: str | None = typer.Option(None, "--manifest-id"),
    older_than: str | None = typer.Option(None, "--older-than"),
    created_before: str | None = typer.Option(None, "--created-before"),
    apply: bool = typer.Option(False, "--apply"),
    as_json: bool = typer.Option(False, "--json"),
    approval: str | None = typer.Option(None, "--approval", help=_APPROVAL_HELP),
    require_approval: bool = typer.Option(False, "--require-approval", help=_REQUIRE_APPROVAL_HELP),
) -> None:
    """Alias for ``det silver-catchup cleanup``."""
    _run_cleanup(
        ctx,
        list_tables=list_tables,
        manifest_id=manifest_id,
        older_than=older_than,
        created_before=created_before,
        apply=apply,
        as_json=as_json,
        approval=approval,
        require_approval=require_approval,
    )
