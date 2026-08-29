from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import typer

from det.cli.app import logger

_PIPELINE_HELP = (
    "Pipeline ref: canonical id (noaa.storm_events), slash form, or YAML path under the project"
)
_PROJECT_ROOT_HELP = "Project root (default: DET_PROJECT_ROOT env, else cwd)"
_APPROVAL_HELP = (
    "Id from `det approve` (apr_…). Validated whenever passed; required when "
    "DET_REQUIRE_APPROVAL=1 or --require-approval. Flags must match the approved "
    "plan exactly — re-approve after changing any of them"
)
_REQUIRE_APPROVAL_HELP = "Fail unless --approval is set (same as DET_REQUIRE_APPROVAL=1)"


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


def _settings(
    project_root: Path | None,
    *,
    lake_path: str | None = None,
    lock_ttl_sec: int | None = None,
):
    """Build DetSettings from env, then apply CLI flag overrides."""
    from det.runtime.settings import DetSettings

    settings = DetSettings.from_env(project_root=project_root)
    overrides: dict = {}
    if lake_path is not None:
        overrides["lake_override"] = lake_path
    if lock_ttl_sec is not None:
        overrides["lock_ttl_sec"] = lock_ttl_sec
    if overrides:
        settings = settings.with_overrides(**overrides)
    return settings


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


# Params each command's *_write_argv builder encodes, so they are covered by
# plan_digest. Keep in lockstep with det.runtime.approval builders.
_BOUND_PARAMS: dict[str, frozenset[str]] = {
    "extract": frozenset({"pipeline", "interval_start", "interval_end", "lake_path", "set_"}),
    "load": frozenset(
        {
            "pipeline",
            "interval_start",
            "interval_end",
            "extract_run_datetime",
            "lake_path",
            "set_",
        }
    ),
    "run": frozenset({"pipeline", "interval_start", "interval_end", "lake_path", "set_"}),
    "migrate": frozenset(
        {
            "pipeline",
            "to_bronze",
            "schema",
            "mapper",
            "interval_start",
            "interval_end",
            "from_raw",
            "wire_version",
            "recreate_iceberg",
            "all_raw",
            "all_raw_runs",
            "ingestion",
            "lake_path",
            "set_",
        }
    ),
    "prune": frozenset({"pipeline", "interval_start", "interval_end", "keep", "apply", "set_"}),
    "dbt": frozenset(
        {"pipeline", "select", "command", "full_refresh", "target", "lake_path", "set_"}
    ),
    "scaffold-dbt": frozenset({"pipeline", "force", "set_"}),
    "scaffold-ops": frozenset({"force"}),
    "init-pipeline": frozenset(
        {
            "name",
            "source_type",
            "destination_type",
            "connection",
            "lake_path",
            "skip_dbt",
            "force",
        }
    ),
    "biglake-register": frozenset(
        {"lake_path", "pipeline", "project", "location", "connection", "skip_ops", "apply"}
    ),
    "lock-release": frozenset(
        {"pipeline", "interval_start", "interval_end", "dataset_id", "force", "lake_path"}
    ),
}

# Params that cannot change what or where anything is written, so they are safe
# to vary under an approval.
_NEUTRAL_PARAMS: frozenset[str] = frozenset(
    {
        "project_root",
        "approval",
        "require_approval",
        "lock_ttl_sec",
        "dry_run",
        "json",
        "json_out",
        "verbose",
        "validate_limit",
        "project_dir",
    }
)


def _schema_for_digest(schema: Path, root: Path) -> str:
    """Project-relative posix form of ``--schema`` for the approval digest.

    Without this, ``schemas/x/y.yaml`` and its absolute equivalent produce
    different digests for the same file.
    """
    candidate = schema if schema.is_absolute() else (root / schema)
    try:
        return candidate.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        # Outside the project root: bind the absolute path, which is the honest
        # description of what would be read.
        return candidate.resolve().as_posix()


def _unbound_params(ctx: typer.Context | None, command: str) -> list[str]:
    """Explicitly-passed params that are neither bound into the digest nor neutral.

    This is the fail-closed half of the approval contract: a flag added later
    lands in neither table, so it is rejected under an approval rather than
    silently escaping plan_digest.
    """
    if ctx is None:
        return []
    bound = _BOUND_PARAMS.get(command, frozenset())
    unbound: list[str] = []
    for name in ctx.params:
        if name in bound or name in _NEUTRAL_PARAMS:
            continue
        source = ctx.get_parameter_source(name)
        # Compare by name, not identity: typer vendors its own click fork, so
        # its ParameterSource enum is a different class from click.core's.
        if getattr(source, "name", None) == "COMMANDLINE":
            unbound.append(name)
    return sorted(unbound)


def _approval_failure_hint(approval_id: str) -> None:
    """Tell operators how to find and recover a claimed approval after a failed write."""
    typer.echo(
        f"approval {approval_id} remains claimed; list with: "
        "det list-approvals --status claimed",
        err=True,
    )
    typer.echo(
        f"release after worker is dead: det approval-release {approval_id} --force",
        err=True,
    )


@contextmanager
def _claimed_approval_work(
    claimed: bool,
    approval: str | None,
) -> Iterator[None]:
    """On failure after a successful claim, print recovery hints then re-raise."""
    try:
        yield
    except BaseException:
        if claimed and approval:
            _approval_failure_hint(approval)
        raise


def _gate_approval(
    root: Path,
    command: str,
    argv: list[str],
    approval: str | None,
    require_approval: bool,
    *,
    ctx: typer.Context | None,
) -> bool:
    """Validate and atomically claim the approval before any write happens.

    Claiming (rather than only checking) closes the window where two concurrent
    runs both validate the same approval and both perform the write.

    ``ctx`` drives the unbound-flag backstop, so writing commands must declare a
    ``typer.Context`` parameter and forward it.

    Returns ``True`` when an approval id was successfully claimed, else ``False``.
    """
    from det.runtime.approval import ApprovalError, claim_approval, require_approvals_enabled

    require = require_approval or require_approvals_enabled()
    if approval:
        unbound = _unbound_params(ctx, command)
        if unbound:
            flags = ", ".join("--" + name.rstrip("_").replace("_", "-") for name in unbound)
            typer.echo(
                f"approval_unbound_flag: {flags} is not covered by the approved plan for "
                f"{command!r}. Re-approve with the flag included, or drop it.",
                err=True,
            )
            raise typer.Exit(code=1)
    try:
        rec = claim_approval(
            root,
            command,
            argv,
            approval,
            require=require,
        )
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    return rec is not None


def _consume_approval(root: Path, approval: str | None) -> None:
    if not approval:
        return
    from det.runtime.approval import ApprovalError, consume_approval

    try:
        consume_approval(root, approval)
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
