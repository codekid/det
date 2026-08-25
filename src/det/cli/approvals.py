from __future__ import annotations

from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _PROJECT_ROOT_HELP,
    _project_root,
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

