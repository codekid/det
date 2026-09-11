from __future__ import annotations

from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _LAKE_PATH_BRONZE_HELP,
    _LAKE_PATH_HELP,
    _LAKE_PATH_OPS_HELP,
    _LAKE_PATH_RAW_HELP,
    _PROJECT_ROOT_HELP,
    _project_root,
    _settings,
)
from det.runtime.approval import ENV_APPROVED_BY


def _approval_settings(
    root: Path,
    *,
    lake_path: str | None,
    lake_path_raw: str | None,
    lake_path_bronze: str | None,
    lake_path_ops: str | None,
):
    """Same lake roots as the claimed writing command (CLI flags or DET_LAKE_PATH*)."""
    return _settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )


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
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
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
    settings = _approval_settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
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
            settings=settings,
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
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
) -> None:
    """Print one approval record (expired status + heartbeat triage at read time)."""
    import json

    from det.runtime.approval import ApprovalError, describe_approval_record

    root = _project_root(project_root)
    settings = _approval_settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
    try:
        record = describe_approval_record(root, approval_id, settings=settings)
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(record, indent=2))


_STATUSES = ("unused", "claimed", "consumed", "expired")


@app.command("list-approvals")
def list_approvals_cmd(
    status: list[str] = typer.Option(
        [],
        "--status",
        help=(
            f"Filter by derived status (repeatable): {', '.join(_STATUSES)}. "
            "Default: unused. Use --status claimed for stuck crash claims "
            "(lake/postgres store; never expire)."
        ),
    ),
    all_: bool = typer.Option(False, "--all", help="Every record regardless of status"),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
) -> None:
    """List approvals from the configured store (defaults to unused, unexpired).

    Use `--status claimed` to find an approval left stuck by a crashed run; a
    claimed record never expires, so it will not show up in the default listing.

    Pass the same ``--lake-path`` / split roots (or ``DET_LAKE_PATH*``) as the
    claimed writing command so listing hits ``{ops}/approvals/``.
    """
    import json

    from det.runtime.approval import list_approval_records

    if all_ and status:
        raise typer.BadParameter("use --all or --status, not both", param_hint="--all")
    unknown = sorted(set(status) - set(_STATUSES))
    if unknown:
        raise typer.BadParameter(
            f"unknown status {', '.join(unknown)} (want {', '.join(_STATUSES)})",
            param_hint="--status",
        )

    root = _project_root(project_root)
    settings = _approval_settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
    statuses = None if all_ else tuple(status or ("unused",))
    records = list_approval_records(root, statuses=statuses, settings=settings)
    typer.echo(json.dumps({"approvals": records}, indent=2))


@app.command("approval-release")
def approval_release_cmd(
    approval_id: str = typer.Argument(..., help="Approval id (apr_…) stuck in claimed"),
    force: bool = typer.Option(
        False, "--force", help="Required; confirms the claiming run is dead"
    ),
    released_by: str | None = typer.Option(
        None,
        "--released-by",
        help=f"Who released it (or set {ENV_APPROVED_BY}); recorded on the file",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
    lake_path: str | None = typer.Option(None, "--lake-path", help=_LAKE_PATH_HELP),
    lake_path_raw: str | None = typer.Option(None, "--lake-path-raw", help=_LAKE_PATH_RAW_HELP),
    lake_path_bronze: str | None = typer.Option(
        None, "--lake-path-bronze", help=_LAKE_PATH_BRONZE_HELP
    ),
    lake_path_ops: str | None = typer.Option(None, "--lake-path-ops", help=_LAKE_PATH_OPS_HELP),
) -> None:
    """Hand a claimed approval back after its run died. Operator-only.

    Kill the worker first. Releasing an approval whose run is still going
    reopens the double-write window that claiming exists to close.

    This is not a TTL bypass: the record returns to unused, so an approval that
    expired while claimed stays dead. It is also deliberately not gated on an
    approval — the recovery path must not depend on the mechanism that is stuck.

    Use the same ``--lake-path`` / split roots (or ``DET_LAKE_PATH*``) as the
    claimed run so release resolves the correct ``{ops}/approvals/`` root.
    """
    import json

    from det.runtime.approval import (
        ApprovalError,
        approved_by_from_env,
        describe_approval_record,
        release_approval,
    )

    if not force:
        raise typer.BadParameter(
            "--force is required; make sure the claiming run is dead first",
            param_hint="--force",
        )
    root = _project_root(project_root)
    settings = _approval_settings(
        root,
        lake_path=lake_path,
        lake_path_raw=lake_path_raw,
        lake_path_bronze=lake_path_bronze,
        lake_path_ops=lake_path_ops,
    )
    who = (released_by or approved_by_from_env() or "").strip()
    try:
        current = describe_approval_record(root, approval_id, settings=settings)
        if current.get("heartbeat_status") == "fresh":
            typer.echo(
                "warning: heartbeat looks fresh — the claiming writer may still be alive; "
                "releasing now can reopen a double-write window",
                err=True,
            )
        record = release_approval(root, approval_id, released_by=who, settings=settings)
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(record, indent=2))
