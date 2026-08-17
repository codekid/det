"""Append-only run receipts: observability for extract/load attempts.

``meta/manifest.json`` stays the authority for landed partitions. Receipts record
what happened (status, duration, error code, owner) in a sibling lake prefix so
a failed extract's rmtree cannot erase the evidence.
"""

from __future__ import annotations

import json
import math
import os
import secrets
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Any

from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError as PydanticValidationError

from det.logging import get_logger, scrub_secrets
from det.runtime.coerce import CoerceError
from det.runtime.lake import LakeRef
from det.runtime.lease import LeaseHeldError, default_lock_owner
from det.runtime.meta import to_interval_datetime, to_partition_value
from det.runtime.secrets import SecretError, SecretNotSetError
from det.sources.http import HttpError, HttpIntegrityError

logger = get_logger(__name__)

RECEIPT_VERSION = 1
ERROR_MESSAGE_MAX = 500
DEFAULT_WINDOW_DAYS = 7
DEFAULT_LIST_LIMIT = 200
STATUS_OK = "ok"
STATUS_ERROR = "error"
COMMANDS = frozenset({"extract", "load"})
STATUSES = frozenset({STATUS_OK, STATUS_ERROR})


class ReceiptDraft:
    """Mutable fields collected during an extract/load attempt."""

    def __init__(
        self,
        *,
        attempt_id: str,
        started_at: datetime,
        pipeline: str,
        command: str,
        interval_start: str,
        interval_end: str,
        extract_run_datetime: str | None = None,
        wire_version: int | None = None,
        destination: str | None = None,
        owner: str = "",
    ) -> None:
        self.attempt_id = attempt_id
        self.started_at = started_at
        self.pipeline = pipeline
        self.command = command
        self.interval_start = interval_start
        self.interval_end = interval_end
        self.extract_run_datetime = extract_run_datetime
        self.wire_version = wire_version
        self.destination = destination
        self.owner = owner
        self.artifacts: int | None = None
        self.raw_bytes: int | None = None
        self.rows: int | None = None
        self.schema_sha256: str | None = None


def receipts_enabled(env: Mapping[str, str] | None = None) -> bool:
    environ = os.environ if env is None else env
    raw = (environ.get("DET_RUN_RECEIPTS") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def sum_artifact_bytes(artifacts: Sequence[Any] | None) -> int:
    total = 0
    for item in artifacts or ():
        if not isinstance(item, dict):
            continue
        raw = item.get("bytes")
        if raw is None:
            continue
        try:
            total += int(raw)
        except (TypeError, ValueError):
            continue
    return total


def classify_error(exc: BaseException) -> tuple[str, str, str]:
    """Return ``(error_code, error_class, error_message)``; most specific type first."""
    error_class = type(exc).__name__
    message = _scrub_error_message(str(exc))
    if isinstance(exc, HttpIntegrityError):
        return "integrity_error", error_class, message
    if isinstance(exc, HttpError):
        return "http_error", error_class, message
    if isinstance(exc, LeaseHeldError):
        return "lease_held", error_class, message
    if isinstance(exc, SecretNotSetError):
        return "secret_not_set", error_class, message
    if isinstance(exc, SecretError):
        return "secret_not_set", error_class, message
    if isinstance(exc, CoerceError):
        return "schema_invalid", error_class, message
    if isinstance(exc, JsonSchemaValidationError):
        return "schema_invalid", error_class, message
    if isinstance(exc, FileNotFoundError):
        return "raw_missing", error_class, message
    if isinstance(exc, FileExistsError):
        return "raw_exists", error_class, message
    if isinstance(exc, PydanticValidationError):
        return "config_invalid", error_class, message
    if isinstance(exc, ValueError):
        return "config_invalid", error_class, message
    return "unknown", error_class, message


def _scrub_error_message(text: str) -> str:
    return scrub_secrets(text)[:ERROR_MESSAGE_MAX]


def receipt_path(
    lake: LakeRef,
    *,
    started_at: datetime,
    pipeline: str,
    command: str,
    interval_start: str,
    interval_end: str,
    attempt_id: str,
) -> LakeRef:
    dt = started_at.astimezone(UTC).date().isoformat()
    interval_key = (
        f"{to_partition_value(interval_start)}_{to_partition_value(interval_end)}"
    )
    name = f"{command}__{interval_key}__{attempt_id}.json"
    return lake / "runs" / f"dt={dt}" / pipeline / name


def _payload(
    draft: ReceiptDraft,
    *,
    error: BaseException | None,
    finished_at: datetime,
) -> dict[str, Any]:
    duration_ms = max(
        0, int((finished_at - draft.started_at).total_seconds() * 1000)
    )
    body: dict[str, Any] = {
        "receipt_version": RECEIPT_VERSION,
        "attempt_id": draft.attempt_id,
        "pipeline": draft.pipeline,
        "command": draft.command,
        "interval_start": draft.interval_start,
        "interval_end": draft.interval_end,
        "status": STATUS_OK if error is None else STATUS_ERROR,
        "started_at": draft.started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": duration_ms,
        "owner": draft.owner,
    }
    if draft.extract_run_datetime:
        body["extract_run_datetime"] = draft.extract_run_datetime
    if draft.wire_version is not None:
        body["wire_version"] = int(draft.wire_version)
    if draft.destination:
        body["destination"] = draft.destination
    if draft.artifacts is not None:
        body["artifacts"] = int(draft.artifacts)
    if draft.raw_bytes is not None:
        body["raw_bytes"] = int(draft.raw_bytes)
    if draft.rows is not None:
        body["rows"] = int(draft.rows)
    if draft.schema_sha256:
        body["schema_sha256"] = draft.schema_sha256
    if error is not None:
        code, error_class, message = classify_error(error)
        body["error_code"] = code
        body["error_class"] = error_class
        body["error_message"] = message
    return body


def write_receipt(
    lake: LakeRef,
    draft: ReceiptDraft,
    *,
    error: BaseException | None = None,
    env: Mapping[str, str] | None = None,
) -> LakeRef | None:
    """Write one receipt object. Never raises — a write failure must not fail the run."""
    try:
        if not receipts_enabled(env):
            return None
        finished_at = datetime.now(UTC)
        path = receipt_path(
            lake,
            started_at=draft.started_at,
            pipeline=draft.pipeline,
            command=draft.command,
            interval_start=draft.interval_start,
            interval_end=draft.interval_end,
            attempt_id=draft.attempt_id,
        )
        body = _payload(draft, error=error, finished_at=finished_at)
        text = json.dumps(body, indent=2, sort_keys=True) + "\n"
        path.write_text(text, encoding="utf-8")
        return path
    except Exception:
        logger.warning(
            "failed to write run receipt",
            pipeline=draft.pipeline,
            command=draft.command,
            attempt_id=draft.attempt_id,
            exc_info=True,
        )
        return None


@contextmanager
def record_attempt(
    lake: LakeRef,
    *,
    pipeline: str,
    command: str,
    interval_start: str,
    interval_end: str,
    extract_run_datetime: str | None = None,
    wire_version: int | None = None,
    destination: str | None = None,
    env: Mapping[str, str] | None = None,
) -> Iterator[ReceiptDraft]:
    """Capture one extract/load attempt. Open this *outside* ``pipeline_lease``."""
    draft = ReceiptDraft(
        attempt_id=secrets.token_hex(8),
        started_at=datetime.now(UTC),
        pipeline=pipeline,
        command=command,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        wire_version=wire_version,
        destination=destination,
        owner=default_lock_owner(env),
    )
    error: BaseException | None = None
    try:
        yield draft
    except BaseException as exc:
        error = exc
        raise
    finally:
        write_receipt(lake, draft, error=error, env=env)


def parse_attempt_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        clock = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return clock.astimezone(UTC).date()
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    iso = to_interval_datetime(str(value))
    parsed = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).date()


def attempt_window(
    since: str | date | datetime | None = None,
    until: str | date | datetime | None = None,
    *,
    now: datetime | None = None,
) -> tuple[date, date]:
    """Half-open ``[since, until)`` attempt-date window. Default: last 7 UTC days."""
    clock = now or datetime.now(UTC)
    today = clock.astimezone(UTC).date()
    if since is None and until is None:
        end = today + timedelta(days=1)
        start = end - timedelta(days=DEFAULT_WINDOW_DAYS)
        return start, end
    if since is None:
        end = parse_attempt_date(until) if until is not None else today + timedelta(days=1)
        start = end - timedelta(days=DEFAULT_WINDOW_DAYS)
        return start, end
    start = parse_attempt_date(since)
    if until is None:
        end = today + timedelta(days=1)
    else:
        end = parse_attempt_date(until)
    if end <= start:
        raise ValueError(f"attempt window end {end.isoformat()} must be after {start.isoformat()}")
    return start, end


def _dt_keys(since: date, until: date) -> list[str]:
    keys: list[str] = []
    cur = since
    while cur < until:
        keys.append(cur.isoformat())
        cur += timedelta(days=1)
    return keys


def _read_receipt(path: LakeRef) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    message = data.get("error_message")
    if isinstance(message, str):
        data["error_message"] = _scrub_error_message(message)
    return data


def iter_receipts(
    lake: LakeRef,
    *,
    pipeline: str | None = None,
    since: str | date | datetime | None = None,
    until: str | date | datetime | None = None,
    status: str | None = None,
    command: str | None = None,
    now: datetime | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield receipts under ``runs/dt=…`` dirs in ``[since, until)``. Never full-scans."""
    start, end = attempt_window(since, until, now=now)
    runs_root = lake / "runs"
    want_pipeline = (pipeline or "").strip() or None
    want_status = (status or "").strip() or None
    want_command = (command or "").strip() or None
    if want_status and want_status not in STATUSES:
        raise ValueError(f"status must be ok or error, got {status!r}")
    if want_command and want_command not in COMMANDS:
        raise ValueError(f"command must be extract or load, got {command!r}")
    for key in _dt_keys(start, end):
        day_dir = runs_root / f"dt={key}"
        if not day_dir.is_dir():
            continue
        if want_pipeline:
            pipe_dirs = [day_dir / want_pipeline]
        else:
            pipe_dirs = [p for p in day_dir.iterdir() if p.is_dir()]
        for pipe_dir in pipe_dirs:
            if not pipe_dir.is_dir():
                continue
            for child in pipe_dir.iterdir():
                if not child.is_file() or not child.name.endswith(".json"):
                    continue
                data = _read_receipt(child)
                if data is None:
                    continue
                if want_pipeline and str(data.get("pipeline") or "") != want_pipeline:
                    continue
                if want_status and str(data.get("status") or "") != want_status:
                    continue
                if want_command and str(data.get("command") or "") != want_command:
                    continue
                data["path"] = str(child)
                yield data


def list_receipts(
    lake: LakeRef,
    *,
    pipeline: str | None = None,
    since: str | date | datetime | None = None,
    until: str | date | datetime | None = None,
    status: str | None = None,
    command: str | None = None,
    limit: int | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    cap = DEFAULT_LIST_LIMIT if limit is None else max(1, int(limit))
    items = list(
        iter_receipts(
            lake,
            pipeline=pipeline,
            since=since,
            until=until,
            status=status,
            command=command,
            now=now,
        )
    )
    items.sort(key=lambda row: str(row.get("started_at") or ""), reverse=True)
    return items[:cap]


def _percentile(values: list[int], p: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    if p <= 0:
        return ordered[0]
    if p >= 100:
        return ordered[-1]
    rank = math.ceil((p / 100.0) * len(ordered))
    return ordered[max(0, rank - 1)]


# Canonical Iceberg / ops-row columns (v1). Unknown JSON extras are ignored.
OPS_RECEIPT_COLUMNS: tuple[str, ...] = (
    "receipt_version",
    "attempt_id",
    "attempt_date",
    "pipeline",
    "command",
    "interval_start",
    "interval_end",
    "extract_run_datetime",
    "wire_version",
    "status",
    "started_at",
    "finished_at",
    "duration_ms",
    "owner",
    "destination",
    "artifacts",
    "raw_bytes",
    "rows",
    "schema_sha256",
    "error_code",
    "error_class",
    "error_message",
)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_receipt(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    """Map a receipt JSON body to the fixed ops row. Returns None if unusable."""
    attempt_id = _optional_str(raw.get("attempt_id"))
    pipeline = _optional_str(raw.get("pipeline"))
    command = _optional_str(raw.get("command"))
    status = _optional_str(raw.get("status"))
    started_raw = raw.get("started_at")
    if not attempt_id or not pipeline or not command or not status or not started_raw:
        return None
    try:
        attempt_date = parse_attempt_date(str(started_raw))
    except (TypeError, ValueError):
        return None
    version = _optional_int(raw.get("receipt_version"))
    if version is None:
        version = RECEIPT_VERSION
    return {
        "receipt_version": version,
        "attempt_id": attempt_id,
        "attempt_date": attempt_date,
        "pipeline": pipeline,
        "command": command,
        "interval_start": _optional_str(raw.get("interval_start")),
        "interval_end": _optional_str(raw.get("interval_end")),
        "extract_run_datetime": _optional_str(raw.get("extract_run_datetime")),
        "wire_version": _optional_int(raw.get("wire_version")),
        "status": status,
        "started_at": str(started_raw),
        "finished_at": _optional_str(raw.get("finished_at")),
        "duration_ms": _optional_int(raw.get("duration_ms")),
        "owner": _optional_str(raw.get("owner")) or "",
        "destination": _optional_str(raw.get("destination")),
        "artifacts": _optional_int(raw.get("artifacts")),
        "raw_bytes": _optional_int(raw.get("raw_bytes")),
        "rows": _optional_int(raw.get("rows")),
        "schema_sha256": _optional_str(raw.get("schema_sha256")),
        "error_code": _optional_str(raw.get("error_code")),
        "error_class": _optional_str(raw.get("error_class")),
        "error_message": (
            _scrub_error_message(str(raw["error_message"]))
            if raw.get("error_message") is not None
            else None
        ),
    }


def summarize_receipts(
    lake: LakeRef,
    *,
    pipeline: str | None = None,
    since: str | date | datetime | None = None,
    until: str | date | datetime | None = None,
    status: str | None = None,
    command: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    start, end = attempt_window(since, until, now=now)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in iter_receipts(
        lake,
        pipeline=pipeline,
        since=start,
        until=end,
        status=status,
        command=command,
        now=now,
    ):
        key = (str(row.get("pipeline") or ""), str(row.get("command") or ""))
        grouped[key].append(row)
    groups: list[dict[str, Any]] = []
    for (pipe, cmd), rows in sorted(grouped.items()):
        durations = [int(r["duration_ms"]) for r in rows if r.get("duration_ms") is not None]
        codes: Counter[str] = Counter()
        ok = 0
        err = 0
        total_rows = 0
        for row in rows:
            if row.get("status") == STATUS_OK:
                ok += 1
            elif row.get("status") == STATUS_ERROR:
                err += 1
                code = str(row.get("error_code") or "unknown")
                codes[code] += 1
            if row.get("rows") is not None:
                try:
                    total_rows += int(row["rows"])
                except (TypeError, ValueError):
                    pass
        groups.append(
            {
                "pipeline": pipe,
                "command": cmd,
                "attempts": len(rows),
                "ok": ok,
                "error": err,
                "error_codes": dict(sorted(codes.items())),
                "p50_ms": _percentile(durations, 50),
                "p95_ms": _percentile(durations, 95),
                "rows": total_rows,
            }
        )
    return {
        "since": start.isoformat(),
        "until": end.isoformat(),
        "groups": groups,
    }
