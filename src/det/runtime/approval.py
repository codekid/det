"""File-based, single-use approval records for writing DET CLI commands.

MCP never creates these files. ``det approve`` writes ``.det/approvals/{id}.json``.
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

logger = get_logger(__name__)

DEFAULT_TTL_SEC = 3600
ENV_REQUIRE = "DET_REQUIRE_APPROVAL"
ENV_APPROVED_BY = "DET_APPROVED_BY"
ENV_TTL = "DET_APPROVAL_TTL_SEC"
_DIR_REL = Path(".det") / "approvals"

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
    return project_root.resolve() / _DIR_REL


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
    text = value.replace("Z", "+00:00")
    return datetime.fromisoformat(text)


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
    expires = _parse_iso(str(record["expires_at"]))
    if (now or utcnow()) >= expires:
        return "expired"
    return "unused"


def create_approval(
    project_root: Path,
    *,
    command: str,
    argv: Sequence[str],
    approved_by: str,
    ttl_sec: int | None = None,
    now: datetime | None = None,
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
    path = approvals_dir(project_root) / f"{record_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return record


def _record_path(project_root: Path, approval_id: str) -> Path:
    if not approval_id.startswith("apr_") or "/" in approval_id or "\\" in approval_id:
        raise ApprovalError("approval_not_found", f"invalid approval id {approval_id!r}")
    return approvals_dir(project_root) / f"{approval_id}.json"


def _claim_path(project_root: Path, approval_id: str) -> Path:
    """Sidecar whose exclusive creation is the claim mutex."""
    return _record_path(project_root, approval_id).with_suffix(".claim")


def _write_record(project_root: Path, record: dict[str, Any]) -> None:
    """Replace a record atomically so a crash cannot leave a torn file."""
    path = _record_path(project_root, str(record["id"]))
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


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


def load_approval(project_root: Path, approval_id: str) -> dict[str, Any]:
    path = _record_path(project_root, approval_id)
    if not path.is_file():
        raise ApprovalError("approval_not_found", f"no approval file for {approval_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_approval_records(
    project_root: Path,
    *,
    statuses: Sequence[str] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Approval records with status derived at read time.

    ``statuses`` filters on the derived status; ``None`` returns everything. A
    ``claimed`` record is otherwise invisible — it never ages into ``expired``
    (see ``effective_status``) — so an operator whose run crashed needs this to
    find it.
    """
    folder = approvals_dir(project_root)
    if not folder.is_dir():
        return []
    wanted = set(statuses) if statuses is not None else None
    found: list[dict[str, Any]] = []
    for path in sorted(folder.glob("apr_*.json")):
        rec = dict(json.loads(path.read_text(encoding="utf-8")))
        rec["status"] = effective_status(rec, now=now)
        if wanted is None or rec["status"] in wanted:
            found.append(rec)
    return found


def list_unused_approvals(
    project_root: Path, *, now: datetime | None = None
) -> list[dict[str, Any]]:
    return list_approval_records(project_root, statuses=("unused",), now=now)


def check_approval(
    project_root: Path,
    command: str,
    argv: Sequence[str],
    approval_id: str | None,
    *,
    require: bool,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Validate an approval when required or when an id is passed.

    Returns the record when validation ran; ``None`` when enforcement is off
    and no id was given.
    """
    if not approval_id:
        if require:
            raise ApprovalError(
                "approval_required",
                "DET_REQUIRE_APPROVAL is set (or --require-approval); pass --approval <id>",
            )
        return None
    rec = load_approval(project_root, approval_id)
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


def claim_approval(
    project_root: Path,
    command: str,
    argv: Sequence[str],
    approval_id: str | None,
    *,
    require: bool,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Validate then **atomically** claim an approval before the write starts.

    ``check_approval`` alone leaves a race: two processes both validate while the
    record is still ``unused``, both perform the write, and only the second
    ``consume_approval`` fails — after the duplicate write already landed.
    Claiming closes that window because exclusive creation of the ``.claim``
    sidecar has exactly one winner.

    A crash between claim and consume leaves the record ``claimed``, which is the
    fail-closed outcome for an authorization token: recover by issuing a new
    approval (the default TTL is one hour, so this is cheap).
    """
    rec = check_approval(project_root, command, argv, approval_id, require=require, now=now)
    if rec is None:
        return None
    if approval_id is None:
        # check_approval returns None only without an id when require is off
        raise RuntimeError("approval claim requires a non-None approval_id")

    stamp = _iso(now or utcnow())
    who = _runner_identity()
    claim = _claim_path(project_root, approval_id)
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise ApprovalError(
            "approval_in_flight",
            f"approval {approval_id} is already claimed by another run",
        ) from None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps({"approval_id": approval_id, "claimed_at": stamp, "claimed_by": who})
                + "\n"
            )
    except OSError:
        claim.unlink(missing_ok=True)
        raise

    rec["status"] = "claimed"
    rec["claimed_at"] = stamp
    rec["claimed_by"] = who
    _write_record(project_root, rec)
    return rec


def release_approval(
    project_root: Path,
    approval_id: str,
    *,
    released_by: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Hand a stuck ``claimed`` approval back for one more attempt.

    A run that dies between claim and consume leaves the record ``claimed``
    forever, since a claim never ages out. This is the operator escape hatch,
    deliberately shaped like ``force_release_lock`` for lake leases: explicit,
    manual, and recorded.

    It is **not** a TTL bypass. The record returns to ``unused``, so if
    ``expires_at`` has already passed, ``effective_status`` reports ``expired``
    and the approval is dead anyway — releasing cannot extend a lifetime.

    Only ``claimed`` records can be released. There is intentionally no automatic
    or TTL-driven release: that would silently reopen the double-write window
    that claiming exists to close.
    """
    who = (released_by or "").strip()
    if not who:
        raise ApprovalError(
            "approval_identity_required",
            f"--released-by or {ENV_APPROVED_BY} is required to release an approval",
        )
    rec = load_approval(project_root, approval_id)
    status = effective_status(rec, now=now)
    if status != "claimed":
        raise ApprovalError(
            "approval_not_claimed",
            f"approval {approval_id} is {status}, only a claimed approval can be released",
        )

    _claim_path(project_root, approval_id).unlink(missing_ok=True)
    rec["status"] = "unused"
    rec["released_at"] = _iso(now or utcnow())
    rec["released_by"] = who
    # Keep the claim we tore down; the point of releasing is the audit trail.
    rec["released_from_claim"] = {
        "claimed_at": rec.pop("claimed_at", None),
        "claimed_by": rec.pop("claimed_by", None),
    }
    _write_record(project_root, rec)
    logger.warning(
        "released a claimed approval",
        approval_id=approval_id,
        released_by=who,
        prior_claim=rec["released_from_claim"],
        command=rec.get("command"),
        expires_at=rec.get("expires_at"),
    )
    return rec


def consume_approval(
    project_root: Path,
    approval_id: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Finalize an approval.

    Accepts ``claimed`` (the CLI claim-then-write path) and ``unused`` (the
    Airflow check-then-write path, which does not claim).
    """
    rec = load_approval(project_root, approval_id)
    status = effective_status(rec, now=now)
    if status not in {"unused", "claimed"}:
        raise ApprovalError(f"approval_{status}", f"approval {approval_id} is {status}")
    rec["status"] = "consumed"
    rec["consumed_at"] = _iso(now or utcnow())
    _write_record(project_root, rec)
    return rec


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
    """Lake path flags redirect where data lands, so they must be bound."""
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
    set_: Sequence[str] | None = None,
) -> list[str]:
    argv = ["prune", "-p", _norm_pipeline(pipeline), "-s", _require_interval(interval_start)]
    end = _norm_interval(interval_end)
    if end:
        argv.extend(["-e", end])
    argv.extend(["--keep", str(keep), "--apply"])
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
    limit: int = 200,
    manifest_id: str | None = None,
    content_digest: str | None = None,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
) -> list[str]:
    argv = ["silver-catchup-plan", "--apply"]
    if all_pipelines:
        argv.append("--all-pipelines")
    elif pipeline:
        argv.extend(["-p", _norm_pipeline(pipeline)])
    else:
        raise ValueError("pipeline required unless all_pipelines=True")
    if interval_start:
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
