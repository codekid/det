"""Bronze ↔ silver catch-up: latest-per-interval diff, ops manifest, dbt vars.

Correctness grain: for each interval, the latest bronze ``__extract_run_datetime``
must appear in silver for that same ``(interval_start, interval_end)``. Coverage
keys are ``(interval_start, interval_end, extract_run_datetime)`` — run timestamps
alone are not unique across intervals. Older siblings are informational only.
Catch-up heals via an **immutable** ops manifest pair
(``ops/silver_catchup/<manifest_id>.json`` + ``.runs.jsonl``) and one
``det dbt --catchup`` build (DuckDB ``read_json`` or BigQuery external table on
GCS; not full-refresh).
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

from det.runtime.config import PipelineConfig
from det.runtime.ids import dbt_model_slug, parse_canonical_id
from det.runtime.meta import identity_iso
from det.runtime.silver_catchup.types import CatchupRunRow

MANIFEST_VERSION = 1
MANIFEST_ID_PREFIX = "scm_"
_MANIFEST_ID_RE = re.compile(r"^scm_[0-9a-f]{16}$")
_CONTENT_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_LOOKBACK_RE = re.compile(r"^(\d+)([hHdD])$")
# Safety cap when building an apply manifest (display diffs stay at DEFAULT_LIST_LIMIT).
_APPLY_BRONZE_CAP = 100_000
_SILVER_PROBE_CHUNK = 200

def parse_duration(text: str, *, what: str = "duration") -> timedelta:
    """Parse ``Nh`` / ``Nd`` (hours/days). Fail closed on junk."""
    raw = str(text or "").strip()
    match = _LOOKBACK_RE.fullmatch(raw)
    if not match:
        raise ValueError(f"{what} must look like '48h' or '7d', got {text!r}")
    n = int(match.group(1))
    if n < 1:
        raise ValueError(f"{what} must be >= 1, got {text!r}")
    unit = match.group(2).lower()
    if unit == "h":
        return timedelta(hours=n)
    return timedelta(days=n)


def parse_extract_lookback(text: str) -> timedelta:
    """Parse ``Nh`` / ``Nd`` extract-run lookback (hours/days)."""
    return parse_duration(text, what="extract lookback")


def validate_catchup_candidate_scope(
    *,
    interval_start: str | None,
    interval_end: str | None,
    extract_lookback: str | None,
) -> None:
    """Reject combining extract-run lookback with interval ``-s``/``-e``."""
    if extract_lookback and str(extract_lookback).strip():
        if interval_start is not None or interval_end is not None:
            raise ValueError(
                "--extract-lookback cannot combine with -s/--interval-start "
                "or -e/--interval-end"
            )


def silver_relation(config: PipelineConfig) -> tuple[str, str]:
    """Return ``(schema, table)`` for the scaffolded parent silver model."""
    provider, _ = parse_canonical_id(config.name)
    slug = dbt_model_slug(config.name)
    return f"silver_{provider}", f"silver_{slug}"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def _norm_ts(value: object) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        try:
            return identity_iso(value)  # type: ignore[arg-type]
        except Exception:
            return identity_iso(str(value))
    return identity_iso(str(value))


def _coverage_key(
    interval_start: object,
    interval_end: object,
    extract_run_datetime: object,
) -> tuple[str, str, str] | None:
    """Silver/bronze coverage identity: interval bounds + extract-run timestamp."""
    start = _norm_ts(interval_start)
    end = _norm_ts(interval_end)
    ts = _norm_ts(extract_run_datetime)
    if not start or not end or not ts:
        return None
    return (start, end, ts)

def _runs_jsonl_bytes(runs: Sequence[CatchupRunRow | Mapping[str, Any]]) -> bytes:
    """Serialize runs as NDJSON.

    All three coverage fields are UTC-normalized via ``_norm_ts`` so digest
    and sidecar match coverage membership (offset-equivalent instants collide).
    Coverage keys only — ``detected_at`` is omitted from the sidecar.
    """
    lines: list[str] = []
    for raw in runs:
        row = {
            "pipeline": str(raw.get("pipeline") or ""),
            "interval_start": _norm_ts(raw.get("interval_start")),
            "interval_end": _norm_ts(raw.get("interval_end")),
            "extract_run_datetime": _norm_ts(raw.get("extract_run_datetime")),
        }
        lines.append(json.dumps(row, separators=(",", ":"), sort_keys=True))
    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


def new_catchup_manifest_id() -> str:
    """Allocate an immutable catch-up manifest id (``scm_`` + 16 hex)."""
    return MANIFEST_ID_PREFIX + secrets.token_hex(8)


def validate_catchup_manifest_id(manifest_id: str) -> str:
    mid = str(manifest_id or "").strip()
    if not _MANIFEST_ID_RE.fullmatch(mid):
        raise ValueError(
            f"catch-up manifest_id must match scm_<16 hex>, got {manifest_id!r}"
        )
    return mid


def validate_catchup_content_digest(digest: str) -> str:
    text = str(digest or "").strip()
    if not _CONTENT_DIGEST_RE.fullmatch(text):
        raise ValueError(
            f"catch-up content_digest must match sha256:<64 hex>, got {digest!r}"
        )
    return text


def catchup_content_digest(runs: Sequence[CatchupRunRow | Mapping[str, Any]]) -> str:
    """Digest over coverage keys only (stable across plan timestamps).

    All three coverage fields (``interval_start``, ``interval_end``,
    ``extract_run_datetime``) are UTC-normalized via ``_norm_ts``, matching
    ``_coverage_key`` membership.
    """
    rows: list[dict[str, str]] = []
    for raw in runs:
        rows.append(
            {
                "pipeline": str(raw.get("pipeline") or ""),
                "interval_start": _norm_ts(raw.get("interval_start")),
                "interval_end": _norm_ts(raw.get("interval_end")),
                "extract_run_datetime": _norm_ts(raw.get("extract_run_datetime")),
            }
        )
    rows.sort(
        key=lambda r: (
            r["pipeline"],
            r["interval_start"],
            r["interval_end"],
            r["extract_run_datetime"],
        )
    )
    blob = json.dumps(rows, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(blob).hexdigest()

