"""File-based, single-use approval records for writing DET CLI commands.

MCP never creates these files. ``det approve`` writes ``.det/approvals/{id}.json``.
Writing CLI validates ``--approval`` (always if passed; required when
``DET_REQUIRE_APPROVAL=1`` or ``--require-approval``).
"""

from __future__ import annotations

import json
import os
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

DEFAULT_TTL_SEC = 3600
ENV_REQUIRE = "DET_REQUIRE_APPROVAL"
ENV_APPROVED_BY = "DET_APPROVED_BY"
ENV_TTL = "DET_APPROVAL_TTL_SEC"
_DIR_REL = Path(".det") / "approvals"

ApprovalStatus = Literal["unused", "consumed", "expired"]


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
    inner = doc.get("approval_plan") if isinstance(doc.get("approval_plan"), dict) else doc
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
    if record.get("status") == "consumed":
        return "consumed"
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


def load_approval(project_root: Path, approval_id: str) -> dict[str, Any]:
    path = _record_path(project_root, approval_id)
    if not path.is_file():
        raise ApprovalError("approval_not_found", f"no approval file for {approval_id}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_unused_approvals(
    project_root: Path, *, now: datetime | None = None
) -> list[dict[str, Any]]:
    folder = approvals_dir(project_root)
    if not folder.is_dir():
        return []
    found: list[dict[str, Any]] = []
    for path in sorted(folder.glob("apr_*.json")):
        rec = json.loads(path.read_text(encoding="utf-8"))
        rec = dict(rec)
        rec["status"] = effective_status(rec, now=now)
        if rec["status"] == "unused":
            found.append(rec)
    return found


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


def consume_approval(
    project_root: Path,
    approval_id: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    rec = load_approval(project_root, approval_id)
    status = effective_status(rec, now=now)
    if status != "unused":
        raise ApprovalError(f"approval_{status}", f"approval {approval_id} is {status}")
    rec["status"] = "consumed"
    rec["consumed_at"] = _iso(now or utcnow())
    path = _record_path(project_root, approval_id)
    path.write_text(json.dumps(rec, indent=2) + "\n", encoding="utf-8")
    return rec


# --- write-argv builders (MCP dry-run and CLI must use the same lists) ---


def extract_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
) -> list[str]:
    argv = ["extract", "-p", pipeline, "-s", interval_start]
    if interval_end:
        argv.extend(["-e", interval_end])
    return argv


def load_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
) -> list[str]:
    argv = ["load", "-p", pipeline, "-s", interval_start]
    if interval_end:
        argv.extend(["-e", interval_end])
    if extract_run_datetime:
        argv.extend(["--extract-run-datetime", extract_run_datetime])
    return argv


def run_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
) -> list[str]:
    argv = ["run", "-p", pipeline, "-s", interval_start]
    if interval_end:
        argv.extend(["-e", interval_end])
    return argv


def migrate_write_argv(
    pipeline: str,
    to_bronze: str,
    schema: str,
    mapper: str,
    interval_start: str,
    *,
    interval_end: str | None = None,
    from_raw: str | None = None,
    wire_version: int | None = None,
) -> list[str]:
    argv = [
        "migrate",
        "-p",
        pipeline,
        "--to-bronze",
        to_bronze,
        "--schema",
        schema,
        "--mapper",
        mapper,
        "-s",
        interval_start,
    ]
    if interval_end:
        argv.extend(["-e", interval_end])
    if from_raw:
        argv.extend(["--from-raw", from_raw])
    if wire_version is not None:
        argv.extend(["--wire-version", str(wire_version)])
    return argv


def prune_write_argv(
    pipeline: str,
    interval_start: str,
    *,
    interval_end: str | None = None,
    keep: int = 1,
) -> list[str]:
    argv = ["prune", "-p", pipeline, "-s", interval_start]
    if interval_end:
        argv.extend(["-e", interval_end])
    argv.extend(["--keep", str(keep), "--apply"])
    return argv


def init_pipeline_write_argv(
    name: str,
    source_type: str,
    *,
    destination_type: str = "iceberg",
    connection: str | None = None,
    lake_path: str | None = None,
    skip_dbt: bool = False,
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
    return argv


def scaffold_dbt_write_argv(pipeline: str, *, force: bool = False) -> list[str]:
    argv = ["scaffold-dbt", "-p", pipeline]
    if force:
        argv.append("--force")
    return argv


def dbt_write_argv(
    pipeline: str | None = None,
    *,
    command: str = "build",
    select: Sequence[str] | None = None,
) -> list[str]:
    argv = ["dbt", "--command", command]
    if pipeline:
        argv.extend(["-p", pipeline])
    for item in select or []:
        argv.extend(["--select", item])
    return argv


def lock_release_write_argv(
    pipeline: str,
    interval_start: str,
    interval_end: str | None = None,
) -> list[str]:
    argv = ["lock-release", "-p", pipeline, "-s", interval_start, "--force"]
    if interval_end:
        argv.extend(["-e", interval_end])
    return argv
