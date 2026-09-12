from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import typer

from det.cli.app import logger
from det.runtime.approval_bound import APPROVAL_BOUND_PARAMS as _BOUND_PARAMS
from det.runtime.approval_bound import LAKE_LAYER_PARAMS as _LAKE_LAYER_PARAMS

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
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    lock_ttl_sec: int | None = None,
):
    """Build DetSettings from env, then apply CLI flag overrides."""
    from det.runtime.settings import DetSettings

    settings = DetSettings.from_env(project_root=project_root)
    overrides: dict = {}
    if lake_path is not None:
        overrides["lake_override"] = lake_path
    if lake_path_raw is not None:
        overrides["lake_override_raw"] = lake_path_raw
    if lake_path_bronze is not None:
        overrides["lake_override_bronze"] = lake_path_bronze
    if lake_path_ops is not None:
        overrides["lake_override_ops"] = lake_path_ops
    if lock_ttl_sec is not None:
        overrides["lock_ttl_sec"] = lock_ttl_sec
    if overrides:
        settings = settings.with_overrides(**overrides)
    return settings


def _approval_lake_kwargs(settings) -> dict:
    """Effective lake path kwargs for approval digests (layout 2 only).

    Binds parent ``lake_path`` or split layer roots when settings have
    ``DET_LAKE_PATH`` / overrides / ``DET_LAKE_PATH_*`` — not the implicit
    default ``./data/lake``. Does not bind ``--lake-layout`` (always 2).

    An explicit ``lake_override`` (CLI/MCP ``--lake-path``) cannot be combined
    with split roots: runtime resolution prefers split and would ignore the
    parent, so digests would not match the lake the operator meant to bind.
    """
    from det.runtime.lake import (
        is_split_lake_configured,
        split_lake_specs_from_settings,
    )

    out: dict = {}
    override = (settings.lake_override or "").strip()
    if is_split_lake_configured(settings):
        if override:
            raise ValueError(
                "--lake-path cannot be combined with DET_LAKE_PATH_RAW / "
                "_BRONZE / _OPS (or --lake-path-raw/bronze/ops); omit "
                "--lake-path or unset the split roots"
            )
        raw, bronze, ops = split_lake_specs_from_settings(settings)
        if raw:
            out["lake_path_raw"] = raw
        if bronze:
            out["lake_path_bronze"] = bronze
        if ops:
            out["lake_path_ops"] = ops
        return out
    parent = override or (settings.lake_path or "").strip()
    if parent:
        out["lake_path"] = parent
    return out


_LAKE_PATH_HELP = (
    "Lake parent root (layout 2: derives raw/bronze under this path). "
    "Ignored when split roots are set."
)
_LAKE_PATH_RAW_HELP = "Raw layer root URI (layout 2; requires bronze + ops)."
_LAKE_PATH_BRONZE_HELP = "Bronze layer root URI (layout 2; requires raw + ops)."
_LAKE_PATH_OPS_HELP = "Ops layer root URI for runs/locks (layout 2; requires raw + bronze)."


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
        "as_json",  # Typer param name for --json on inspect/ops/catch-up
        "json_out",
        "verbose",
        "validate_limit",
        "project_dir",
        "list_tables",  # --list on silver-catchup-cleanup (read-only; rejected with --apply)
        "older_than",  # preview/list only; apply approvals bind --created-before
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
        f"approval {approval_id} remains claimed; list with: det list-approvals --status claimed",
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
    project_root: Path | None = None,
    *,
    settings=None,
) -> Iterator[None]:
    """Heartbeat while claimed; on failure print recovery hints then re-raise."""
    import threading

    stop = threading.Event()
    thread: threading.Thread | None = None
    if claimed and approval and project_root is not None:
        root = project_root
        active_settings = settings

        def _loop() -> None:
            from det.runtime.approval import touch_approval_heartbeat
            from det.runtime.approval_store import DEFAULT_HEARTBEAT_INTERVAL_SEC

            while not stop.wait(DEFAULT_HEARTBEAT_INTERVAL_SEC):
                try:
                    touch_approval_heartbeat(root, approval, settings=active_settings)
                except Exception:
                    logger.debug(
                        "approval heartbeat touch failed",
                        approval_id=approval,
                        exc_info=True,
                    )

        thread = threading.Thread(target=_loop, name=f"det-approval-hb-{approval}", daemon=True)
        thread.start()
    try:
        yield
    except BaseException:
        if claimed and approval:
            _approval_failure_hint(approval)
        raise
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=1.0)


def _gate_approval(
    root: Path,
    command: str,
    argv: list[str],
    approval: str | None,
    require_approval: bool,
    *,
    ctx: typer.Context | None,
    settings=None,
) -> bool:
    """Validate and atomically claim the approval before any write happens.

    Claiming (rather than only checking) closes the window where two concurrent
    runs both validate the same approval and both perform the write.

    ``ctx`` drives the unbound-flag backstop, so writing commands must declare a
    ``typer.Context`` parameter and forward it.

    ``settings`` must be the same ``DetSettings`` used for the write (CLI lake
    overrides) so claim/consume/heartbeat hit one approval store.

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
            settings=settings,
        )
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    return rec is not None


def _consume_approval(
    root: Path,
    approval: str | None,
    *,
    settings=None,
) -> None:
    if not approval:
        return
    from det.runtime.approval import ApprovalError, consume_approval

    try:
        consume_approval(root, approval, settings=settings)
    except ApprovalError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
