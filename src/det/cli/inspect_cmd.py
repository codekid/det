from __future__ import annotations

from pathlib import Path

import typer

from det.cli.app import app
from det.cli.common import (
    _PROJECT_ROOT_HELP,
    _project_root,
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
        findings_payload,
        format_findings,
        has_errors,
        has_warnings,
    )
    from det.scaffold.check_dbt import check_project_with_dbt

    root = _project_root(project_root)
    findings = check_project_with_dbt(root, pipeline=pipeline)
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
def list_sources_cmd(
    project_root: Path | None = typer.Option(None, "--project-root", help=_PROJECT_ROOT_HELP),
) -> None:
    from det.runtime.registry import list_sources

    root = _project_root(project_root)
    for name in list_sources(project_root=root):
        typer.echo(name)


@app.command("list-mappers")
def list_mappers_cmd() -> None:
    """Show migrate mappers and the source-row shape each one expects."""
    from det.runtime.registry import describe_mappers

    described = describe_mappers()
    width = max((len(name) for name, _ in described), default=0)
    for name, summary in described:
        typer.echo(f"{name.ljust(width)}  {summary}" if summary else name)

