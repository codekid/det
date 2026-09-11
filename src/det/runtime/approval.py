"""Lake/postgres-backed single-use approval records for writing DET CLI commands.

MCP never creates these records. ``det approve`` writes to the configured store
(default: ``{ops_root}/approvals/`` on the lake; opt-in Postgres via
``DET_APPROVAL_BACKEND=postgres``). Legacy ``.det/approvals`` remains a read-only
fallback for one release.

Writing CLI validates ``--approval`` (always if passed; required when
``DET_REQUIRE_APPROVAL=1`` or ``--require-approval``).

This is an **audit and intent-binding** mechanism, not an authorization boundary:
the shell that can run ``det extract --approval`` can also run ``det approve``.
What it guarantees is that the approved record accurately describes the command
that runs — every flag that can change *what* or *where* is written is bound into
``plan_digest``, and the CLI refuses unbound mutating flags (see
``det.cli.common``). Real authorization requires ``det approve`` out-of-band.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from det.logging import get_logger
from det.runtime.approval_store import (
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    enrich_heartbeat_fields,
    open_approval_store,
)
from det.runtime.approval_store.legacy import legacy_approvals_dir

logger = get_logger(__name__)

DEFAULT_TTL_SEC = 3600
ENV_REQUIRE = "DET_REQUIRE_APPROVAL"
ENV_APPROVED_BY = "DET_APPROVED_BY"
ENV_TTL = "DET_APPROVAL_TTL_SEC"

ApprovalStatus = Literal["unused", "claimed", "consumed", "expired"]

# A resolved pipeline identity: provider.source, or a bare stem for flat configs.
_PIPELINE_ID_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$")


class ApprovalError(Exception):
    """Typed failure; ``code`` is stable for agents and tests."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ApprovalPlan:
    command: str
    argv: tuple[str, ...]
    plan_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "argv": list(self.argv),
            "plan_digest": self.plan_digest,
            "note": (
                "Dry-run only — no write. Operator: det approve --plan <this object> "
                "--approved-by <id>. Agent: writing CLI with --approval <id> in a later "
                "turn (never the same turn as this dry-run)."
            ),
        }


def approvals_dir(project_root: Path) -> Path:
    """Legacy local approvals directory (read fallback / tests)."""
    return legacy_approvals_dir(project_root)


def require_approvals_enabled() -> bool:
    raw = os.environ.get(ENV_REQUIRE, "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def approved_by_from_env() -> str | None:
    value = os.environ.get(ENV_APPROVED_BY, "").strip()
    return value or None


def ttl_sec_from_env() -> int:
    raw = os.environ.get(ENV_TTL, "").strip()
    if not raw:
        return DEFAULT_TTL_SEC
    try:
        ttl = int(raw)
    except ValueError as exc:
        raise ApprovalError("approval_ttl_invalid", f"{ENV_TTL} must be an integer") from exc
    if ttl < 1:
        raise ApprovalError("approval_ttl_invalid", f"{ENV_TTL} must be >= 1")
    return ttl


def plan_digest(command: str, argv: Sequence[str]) -> str:
    import hashlib

    payload = {"argv": list(argv), "command": command}
    blob = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def make_plan(command: str, argv: Sequence[str]) -> ApprovalPlan:
    argv_t = tuple(str(p) for p in argv)
    return ApprovalPlan(
        command=command,
        argv=argv_t,
        plan_digest=plan_digest(command, argv_t),
    )


def plan_from_mapping(doc: dict[str, Any]) -> ApprovalPlan:
    """Accept a stub or a dry-run payload that nests ``approval_plan``."""
    raw_inner = doc.get("approval_plan")
    inner: dict[str, Any] = raw_inner if isinstance(raw_inner, dict) else doc
    command = inner.get("command")
    argv = inner.get("argv")
    if not isinstance(command, str) or not command.strip():
        raise ApprovalError("approval_plan_invalid", "plan is missing command")
    if not isinstance(argv, list) or not all(isinstance(p, str) for p in argv):
        raise ApprovalError("approval_plan_invalid", "plan argv must be a list of strings")
    plan = make_plan(command, argv)
    digest = inner.get("plan_digest")
    if digest is not None and digest != plan.plan_digest:
        raise ApprovalError(
            "approval_plan_invalid",
            "plan_digest does not match command+argv",
        )
    return plan


def utcnow() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse ISO stamps; offset-less values are treated as UTC (never naive)."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def effective_status(record: dict[str, Any], *, now: datetime | None = None) -> ApprovalStatus:
    """Derive status. TTL gates *claiming*, not finishing.

    Once a record is claimed the TTL no longer applies: the claim was taken while
    the approval was valid, so a long-running write must still be able to finalize
    it. Only ``unused`` records can age into ``expired``.
    """
    status = record.get("status")
    if status == "consumed":
        return "consumed"
    if status == "claimed":
        return "claimed"
    try:
        expires = _parse_iso(str(record["expires_at"]))
        clock = now or utcnow()
        if clock.tzinfo is None:
            clock = clock.replace(tzinfo=UTC)
        else:
            clock = clock.astimezone(UTC)
    except (TypeError, ValueError, OverflowError, KeyError):
        # Malformed expiry must not break list/describe; fail closed for claiming.
        return "expired"
    if clock >= expires:
        return "expired"
    return "unused"


def _store(project_root: Path, *, settings: Any | None = None):
    """Open the approval store, honoring active/CLI ``DetSettings`` lake roots."""
    return open_approval_store(project_root, settings=settings)


def create_approval(
    project_root: Path,
    *,
    command: str,
    argv: Sequence[str],
    approved_by: str,
    ttl_sec: int | None = None,
    now: datetime | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    who = (approved_by or "").strip()
    if not who:
        raise ApprovalError(
            "approval_identity_required",
            f"--approved-by or {ENV_APPROVED_BY} is required",
        )
    plan = make_plan(command, argv)
    stamp = now or utcnow()
    ttl = ttl_sec if ttl_sec is not None else ttl_sec_from_env()
    if ttl < 1:
        raise ApprovalError("approval_ttl_invalid", "ttl_sec must be >= 1")
    record_id = "apr_" + secrets.token_hex(8)
    record = {
        "id": record_id,
        "created_at": _iso(stamp),
        "expires_at": _iso(stamp + timedelta(seconds=ttl)),
        "approved_by": who,
        "command": plan.command,
        "argv": list(plan.argv),
        "plan_digest": plan.plan_digest,
        "status": "unused",
        "consumed_at": None,
    }
    return _store(project_root, settings=settings).create(record)


def load_approval(
    project_root: Path,
    approval_id: str,
    *,
    settings: Any | None = None,
) -> dict[str, Any]:
    return _store(project_root, settings=settings).load(approval_id)


def list_approval_records(
    project_root: Path,
    *,
    statuses: Sequence[str] | None = None,
    now: datetime | None = None,
    enrich: bool = True,
    settings: Any | None = None,
) -> list[dict[str, Any]]:
    """Approval records with status derived at read time.

    ``statuses`` filters on the derived status; ``None`` returns everything. A
    ``claimed`` record is otherwise invisible — it never ages into ``expired``
    (see ``effective_status``) — so an operator whose run crashed needs this to
    find it.
    """
    found = _store(project_root, settings=settings).list(statuses=statuses, now=now)
    if not enrich:
        return found
    return [enrich_heartbeat_fields(rec, now=now) for rec in found]


def list_unused_approvals(
    project_root: Path,
    *,
    now: datetime | None = None,
    settings: Any | None = None,
) -> list[dict[str, Any]]:
    return list_approval_records(project_root, statuses=("unused",), now=now, settings=settings)


def check_approval(
    project_root: Path,
    command: str,
    argv: Sequence[str],
    approval_id: str | None,
    *,
    require: bool,
    now: datetime | None = None,
    settings: Any | None = None,
) -> dict[str, Any] | None:
    """
    Validate an approval when required or when an id is passed.

    Returns the record when validation ran; ``None`` when enforcement is off
    and no id was given.

    Rejects legacy-only ``.det/approvals`` ids so Airflow check→consume and CLI
    claim cannot validate a ticket that the primary store cannot mutate.
    """
    if not approval_id:
        if require:
            raise ApprovalError(
                "approval_required",
                "DET_REQUIRE_APPROVAL is set (or --require-approval); pass --approval <id>",
            )
        return None
    store = _store(project_root, settings=settings)
    ensure = getattr(store, "ensure_writable", None)
    if callable(ensure):
        ensure(approval_id)
    rec = store.load(approval_id)
    status = effective_status(rec, now=now)
    if status == "expired":
        raise ApprovalError("approval_expired", f"approval {approval_id} has expired")
    if status == "consumed":
        raise ApprovalError("approval_consumed", f"approval {approval_id} was already used")
    if status == "claimed":
        raise ApprovalError(
            "approval_in_flight",
            f"approval {approval_id} is already claimed by another run",
        )
    if rec.get("command") != command:
        raise ApprovalError(
            "approval_command_mismatch",
            f"approval {approval_id} is for {rec.get('command')!r}, not {command!r}",
        )
    expected = plan_digest(command, argv)
    if rec.get("plan_digest") != expected or list(rec.get("argv") or []) != list(argv):
        raise ApprovalError(
            "approval_argv_mismatch",
            f"approval {approval_id} argv/digest does not match this command",
        )
    return rec


def _runner_identity() -> str:
    """Best-effort identity of the process claiming an approval (audit only)."""
    named = os.environ.get("DET_LOCK_OWNER", "").strip()
    if named:
        return named
    try:
        host = socket.gethostname()
    except OSError:
        host = "unknown"
    return f"{host}/pid:{os.getpid()}"


def claim_approval(
    project_root: Path,
    command: str,
    argv: Sequence[str],
    approval_id: str | None,
    *,
    require: bool,
    now: datetime | None = None,
    heartbeat_interval_sec: int = DEFAULT_HEARTBEAT_INTERVAL_SEC,
    settings: Any | None = None,
) -> dict[str, Any] | None:
    """Validate then **atomically** claim an approval before the write starts.

    A crash between claim and consume leaves the record ``claimed``. Recover by
    issuing a new approval (preferred) or ``det approval-release --force`` after
    the worker is confirmed dead.
    """
    rec = check_approval(
        project_root,
        command,
        argv,
        approval_id,
        require=require,
        now=now,
        settings=settings,
    )
    if rec is None:
        return None
    if approval_id is None:
        raise RuntimeError("approval claim requires a non-None approval_id")

    return _store(project_root, settings=settings).claim(
        approval_id,
        claimed_by=_runner_identity(),
        now=now,
        heartbeat_interval_sec=heartbeat_interval_sec,
    )


def release_approval(
    project_root: Path,
    approval_id: str,
    *,
    released_by: str,
    now: datetime | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    """Hand a stuck ``claimed`` approval back for one more attempt.

    Only ``claimed`` records can be released. There is intentionally no automatic
    or TTL-driven release.
    """
    who = (released_by or "").strip()
    if not who:
        raise ApprovalError(
            "approval_identity_required",
            f"--released-by or {ENV_APPROVED_BY} is required to release an approval",
        )
    return _store(project_root, settings=settings).release(approval_id, released_by=who, now=now)


def consume_approval(
    project_root: Path,
    approval_id: str,
    *,
    now: datetime | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    """Finalize an approval.

    Accepts ``claimed`` (the CLI claim-then-write path) and ``unused`` (the
    Airflow check-then-write path, which does not claim).
    """
    return _store(project_root, settings=settings).consume(approval_id, now=now)


def touch_approval_heartbeat(
    project_root: Path,
    approval_id: str,
    *,
    now: datetime | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    """Advisory heartbeat while a CLI claim is held."""
    return _store(project_root, settings=settings).touch_heartbeat(approval_id, now=now)


def describe_approval_record(
    project_root: Path,
    approval_id: str,
    *,
    now: datetime | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    """Load one record with derived status + heartbeat triage fields."""
    rec = dict(load_approval(project_root, approval_id, settings=settings))
    rec["status"] = effective_status(rec, now=now)
    return enrich_heartbeat_fields(rec, now=now)


# --- write-argv builders (MCP dry-run and CLI must use the same lists) ---
#
# Every builder normalizes its inputs so the same logical command produces the
# same digest regardless of the surface it came from. Without this, approving via
# MCP (canonical id, bare date) and running via CLI (path ref, ISO datetime)
# yields a spurious approval_argv_mismatch.


def _norm_pipeline(pipeline: str) -> str:
    """Require a resolved pipeline *identity*, not a filesystem ref.

    Callers pass ``ResolvedPipeline.canonical_id`` — usually ``provider.source``,
    but a bare stem for a flat or out-of-tree config. Path and slash forms are
    rejected because they depend on cwd and project root, so the same pipeline
    would digest differently depending on how it was spelled on the command line.
    """
    text = str(pipeline).strip()
    if (
        "/" in text
        or "\\" in text
        or text.endswith((".yaml", ".yml"))
        or not _PIPELINE_ID_RE.match(text)
    ):
        raise ValueError(
            "approval argv needs a resolved pipeline id (provider.source), got "
            f"{pipeline!r} — resolve the ref before building the plan"
        )
    return text


def _norm_interval(value: str | None) -> str | None:
    """Normalize an interval bound to ISO-8601 UTC; bare dates become midnight."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    from det.runtime.meta import to_interval_datetime

    return to_interval_datetime(text)


def _require_interval(value: str) -> str:
    norm = _norm_interval(value)
    if norm is None:
        raise ValueError("interval_start is required")
    return norm


def _norm_relpath(value: str) -> str:
    """Normalize a path-ish argv value: posix separators, no leading ``./``."""
    text = str(value).strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    return text


def _set_argv(set_: Sequence[str] | None) -> list[str]:
    """``--set`` overrides, sorted so flag order cannot change the digest."""
    argv: list[str] = []
    for item in sorted(str(s) for s in (set_ or [])):
        argv.extend(["--set", item])
    return argv


def _lake_argv(
    lake_path: str | None = None,
    *,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
) -> list[str]:
    """Lake path / layout flags redirect where data lands, so they must be bound."""
    argv: list[str] = []
    text = (lake_path or "").strip()
    if text:
        argv.extend(["--lake-path", text])
    raw = (lake_path_raw or "").strip()
    if raw:
        argv.extend(["--lake-path-raw", raw])
    bronze = (lake_path_bronze or "").strip()
    if bronze:
        argv.extend(["--lake-path-bronze", bronze])
    ops = (lake_path_ops or "").strip()
    if ops:
        argv.extend(["--lake-path-ops", ops])
    return argv


def extract_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
    *,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["extract", "-p", _norm_pipeline(pipeline), "-s", _require_interval(interval_start)]
    end = _norm_interval(interval_end)
    if end:
        argv.extend(["-e", end])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    argv.extend(_set_argv(set_))
    return argv


def load_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    *,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["load", "-p", _norm_pipeline(pipeline), "-s", _require_interval(interval_start)]
    end = _norm_interval(interval_end)
    if end:
        argv.extend(["-e", end])
    if extract_run_datetime:
        argv.extend(["--extract-run-datetime", extract_run_datetime])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    argv.extend(_set_argv(set_))
    return argv


def run_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
    *,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["run", "-p", _norm_pipeline(pipeline), "-s", _require_interval(interval_start)]
    end = _norm_interval(interval_end)
    if end:
        argv.extend(["-e", end])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    argv.extend(_set_argv(set_))
    return argv


def migrate_write_argv(
    pipeline: str,
    to_bronze: str,
    schema: str,
    mapper: str,
    interval_start: str | None = None,
    *,
    interval_end: str | None = None,
    from_raw: str | None = None,
    wire_version: int | None = None,
    recreate_iceberg: bool = False,
    all_raw: bool = False,
    all_raw_runs: bool = False,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    ingestion: str | None = None,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = [
        "migrate",
        "-p",
        _norm_pipeline(pipeline),
        "--to-bronze",
        to_bronze,
        "--schema",
        _norm_relpath(schema),
        "--mapper",
        mapper,
    ]
    if all_raw:
        argv.append("--all-raw")
    else:
        if interval_start is None:
            raise ValueError("interval_start required unless all_raw=True")
        argv.extend(["-s", _require_interval(interval_start)])
        end = _norm_interval(interval_end)
        if end:
            argv.extend(["-e", end])
    if from_raw:
        argv.extend(["--from-raw", from_raw])
    if wire_version is not None:
        argv.extend(["--wire-version", str(wire_version)])
    if recreate_iceberg:
        argv.append("--recreate-iceberg")
    if all_raw_runs:
        argv.append("--all-raw-runs")
    # --ingestion selects the write path, so it changes how bronze lands.
    if ingestion:
        argv.extend(["--ingestion", str(ingestion).strip()])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    argv.extend(_set_argv(set_))
    return argv


def prune_write_argv(
    pipeline: str,
    interval_start: str,
    *,
    interval_end: str | None = None,
    keep: int = 1,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["prune", "-p", _norm_pipeline(pipeline), "-s", _require_interval(interval_start)]
    end = _norm_interval(interval_end)
    if end:
        argv.extend(["-e", end])
    argv.extend(["--keep", str(keep), "--apply"])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    argv.extend(_set_argv(set_))
    return argv


def backfill_write_argv(interval_start: str, interval_end: str) -> list[str]:
    """Canonical argv for approving an Airflow backfill window (not a det CLI verb)."""
    start = interval_start.strip()[:10]
    end = interval_end.strip()[:10]
    return [
        "backfill",
        "--interval-start",
        start,
        "--interval-end",
        end,
    ]


def init_pipeline_write_argv(
    name: str,
    source_type: str,
    *,
    destination_type: str = "iceberg",
    connection: str | None = None,
    lake_path: str | None = None,
    skip_dbt: bool = False,
    force: bool = False,
) -> list[str]:
    argv = [
        "init-pipeline",
        "--name",
        name,
        "--source-type",
        source_type,
        "--destination-type",
        destination_type,
    ]
    if connection:
        argv.extend(["--connection", connection])
    if lake_path:
        argv.extend(["--lake-path", lake_path])
    if skip_dbt:
        argv.append("--skip-dbt")
    # --force overwrites existing files, so it changes what the write destroys.
    if force:
        argv.append("--force")
    return argv


def scaffold_dbt_write_argv(
    pipeline: str,
    *,
    force: bool = False,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["scaffold-dbt", "-p", _norm_pipeline(pipeline)]
    if force:
        argv.append("--force")
    # Overrides change the config the models are generated from.
    argv.extend(_set_argv(set_))
    return argv


def scaffold_ops_write_argv(*, force: bool = False) -> list[str]:
    argv = ["scaffold-ops"]
    if force:
        argv.append("--force")
    return argv


def dbt_write_argv(
    pipeline: str | None = None,
    *,
    command: str = "build",
    select: Sequence[str] | None = None,
    full_refresh: bool = False,
    catchup: bool = False,
    catchup_manifest: str | None = None,
    target: str | None = None,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["dbt", "--command", command]
    if pipeline:
        argv.extend(["-p", _norm_pipeline(pipeline)])
    for item in select or []:
        argv.extend(["--select", item])
    # --full-refresh drops and rebuilds incremental models; --target picks the
    # warehouse. Both change what the run does, so both are bound.
    if full_refresh:
        argv.append("--full-refresh")
    if catchup:
        argv.append("--catchup")
        mid = (catchup_manifest or "").strip()
        if not mid:
            raise ValueError("--catchup requires --catchup-manifest <scm_…>")
        argv.extend(["--catchup-manifest", mid])
    elif (catchup_manifest or "").strip():
        raise ValueError("--catchup-manifest requires --catchup")
    if target:
        argv.extend(["--target", str(target).strip()])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    argv.extend(_set_argv(set_))
    return argv


def silver_catchup_plan_write_argv(
    pipeline: str | None = None,
    *,
    all_pipelines: bool = False,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_lookback: str | None = None,
    census: bool = False,
    limit: int = 200,
    manifest_id: str | None = None,
    content_digest: str | None = None,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
) -> list[str]:
    from det.runtime.silver_catchup.ids import resolve_catchup_candidate_scope

    argv = ["silver-catchup-plan", "--apply"]
    if all_pipelines:
        argv.append("--all-pipelines")
    elif pipeline:
        argv.extend(["-p", _norm_pipeline(pipeline)])
    else:
        raise ValueError("pipeline required unless all_pipelines=True")
    # Bind effective discovery scope (routine default 48h, explicit lookback,
    # --census, or -s/-e). Digests must not omit an implicit default lookback.
    effective = resolve_catchup_candidate_scope(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        census=census,
    )
    if effective:
        argv.extend(["--extract-lookback", effective])
    elif census:
        argv.append("--census")
    elif interval_start:
        argv.extend(["-s", _require_interval(interval_start)])
        end = _norm_interval(interval_end)
        if end:
            argv.extend(["-e", end])
    if limit != 200:
        argv.extend(["--limit", str(int(limit))])
    mid = (manifest_id or "").strip()
    digest = (content_digest or "").strip()
    if not mid or not digest:
        raise ValueError(
            "silver-catchup-plan --apply approval requires "
            "--manifest-id and --content-digest from dry-run"
        )
    argv.extend(["--manifest-id", mid, "--content-digest", digest])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    return argv


def silver_catchup_cleanup_write_argv(
    *,
    manifest_id: str | None = None,
    created_before: str | None = None,
) -> list[str]:
    """Bound argv for ``det silver-catchup-cleanup --apply``.

    Retention apply binds ``--created-before`` (immutable UTC ISO from dry-run),
    never a relative ``--older-than`` duration that would drift at execution.
    """
    mid = (manifest_id or "").strip()
    before = (created_before or "").strip()
    if mid and before:
        raise ValueError("--manifest-id cannot combine with --created-before")
    if not mid and not before:
        raise ValueError(
            "silver-catchup-cleanup --apply approval requires "
            "exactly one of --manifest-id or --created-before"
        )
    argv = ["silver-catchup-cleanup", "--apply"]
    if mid:
        argv.extend(["--manifest-id", mid])
    else:
        from det.runtime.meta import identity_iso

        argv.extend(["--created-before", identity_iso(before)])
    return argv


def lock_release_write_argv(
    pipeline: str,
    interval_start: str | None = None,
    interval_end: str | None = None,
    *,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    dataset_id: str | None = None,
) -> list[str]:
    argv = [
        "lock-release",
        "-p",
        _norm_pipeline(pipeline),
        "--force",
    ]
    if dataset_id:
        argv.extend(["--dataset-id", str(dataset_id).strip()])
    else:
        argv.extend(["-s", _require_interval(interval_start or "")])
        end = _norm_interval(interval_end)
        if end:
            argv.extend(["-e", end])
    argv.extend(
        _lake_argv(
            lake_path,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
        )
    )
    return argv
