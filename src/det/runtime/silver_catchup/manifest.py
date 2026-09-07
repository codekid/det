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

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from det.errors import DetConflictError
from det.logging import get_logger
from det.runtime.config import load_pipeline_config
from det.runtime.ids import dbt_model_slug
from det.runtime.lake import LakeRef
from det.runtime.limits import DEFAULT_LIST_LIMIT
from det.runtime.pipelines import resolve_pipeline_ref
from det.runtime.settings import DetSettings
from det.runtime.silver_catchup.diff import (
    diff_bronze_silver,
    diff_bronze_silver_fleet,
)
from det.runtime.silver_catchup.ids import (
    MANIFEST_VERSION,
    _norm_ts,
    _runs_jsonl_bytes,
    catchup_content_digest,
    new_catchup_manifest_id,
    validate_catchup_content_digest,
    validate_catchup_manifest_id,
)
from det.runtime.silver_catchup.paths import (
    CATCHUP_DIR,
    catchup_manifest_ref,
    catchup_runs_ref,
    resolve_ops_lake,
)
from det.runtime.silver_catchup.types import (
    CatchupManifestPayload,
    CatchupRunRow,
    CatchupSidecarRunRow,
)

logger = get_logger(__name__)


def load_catchup_runs_from_jsonl(runs_path: LakeRef) -> list[CatchupSidecarRunRow]:
    """Parse sibling ``.runs.jsonl`` into coverage-key rows (no ``detected_at``)."""
    text = runs_path.read_text(encoding="utf-8")
    rows: list[CatchupSidecarRunRow] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"catch-up runs NDJSON line {line_no} is not JSON: {runs_path}"
            ) from exc
        if not isinstance(raw, dict):
            raise ValueError(
                f"catch-up runs NDJSON line {line_no} must be an object: {runs_path}"
            )
        rows.append(cast(CatchupSidecarRunRow, raw))
    return rows


def assert_catchup_runs_sidecar_matches(
    runs_path: LakeRef,
    *,
    expected_digest: str,
) -> None:
    """Fail closed when ``.runs.jsonl`` coverage keys drift from scm digest."""
    want = validate_catchup_content_digest(expected_digest)
    got = catchup_content_digest(load_catchup_runs_from_jsonl(runs_path))
    if got != want:
        raise ValueError(
            "catch-up runs NDJSON does not match manifest content_digest; "
            f"manifest {want}, sidecar {got} ({runs_path})"
        )


def _require_detected_at(value: object, *, where: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"catch-up {where} requires non-empty detected_at")
    return text


def manifest_payload_from_catchup(
    catchup_runs: Sequence[CatchupRunRow | CatchupSidecarRunRow | Mapping[str, Any]],
    *,
    detected_at: str | None = None,
    manifest_id: str | None = None,
) -> CatchupManifestPayload:
    stamp = _require_detected_at(
        detected_at or datetime.now(UTC).isoformat(),
        where="manifest",
    )
    rows: list[CatchupRunRow] = []
    for i, raw in enumerate(catchup_runs):
        rows.append(
            {
                "pipeline": str(raw["pipeline"]),
                "extract_run_datetime": _norm_ts(raw["extract_run_datetime"]),
                "interval_start": _norm_ts(raw.get("interval_start")),
                "interval_end": _norm_ts(raw.get("interval_end")),
                "detected_at": _require_detected_at(
                    raw.get("detected_at") or stamp,
                    where=f"runs[{i}]",
                ),
            }
        )
    mid = validate_catchup_manifest_id(manifest_id or new_catchup_manifest_id())
    digest = catchup_content_digest(rows)
    return {
        "manifest_version": MANIFEST_VERSION,
        "manifest_id": mid,
        "content_digest": digest,
        "updated_at": stamp,
        "runs": rows,
    }


def write_catchup_manifest(
    payload: CatchupManifestPayload | Mapping[str, Any],
    *,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    """Write an immutable catch-up manifest + sibling runs NDJSON; refuse if id exists.

    Publishes ``.runs.jsonl`` first; scm ``.json`` is the commit marker. An
    identical orphan sidecar (JSON missing) is recoverable on retry; a completed
    manifest or a different sidecar still conflicts.
    """
    mid = validate_catchup_manifest_id(str(payload.get("manifest_id") or ""))
    digest = validate_catchup_content_digest(str(payload.get("content_digest") or ""))
    live = catchup_content_digest(payload.get("runs") or [])
    if live != digest:
        raise ValueError(
            "catch-up content_digest does not match runs; "
            f"payload has {digest}, runs hash to {live}"
        )
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    path = catchup_manifest_ref(ops, mid)
    runs_path = catchup_runs_ref(ops, mid)
    if path.exists():
        raise DetConflictError(
            f"catch-up manifest already exists (immutable): {path}"
        )
    runs_raw = list(payload.get("runs") or [])
    runs: list[CatchupRunRow] = []
    for i, raw in enumerate(runs_raw):
        if not isinstance(raw, Mapping):
            raise ValueError(f"catch-up manifest runs[{i}] must be an object")
        runs.append(
            {
                "pipeline": str(raw.get("pipeline") or ""),
                "interval_start": _norm_ts(raw.get("interval_start")),
                "interval_end": _norm_ts(raw.get("interval_end")),
                "extract_run_datetime": _norm_ts(raw.get("extract_run_datetime")),
                "detected_at": _require_detected_at(
                    raw.get("detected_at"), where=f"runs[{i}]"
                ),
            }
        )
    body: CatchupManifestPayload = {
        "manifest_version": int(payload.get("manifest_version") or MANIFEST_VERSION),
        "manifest_id": mid,
        "content_digest": digest,
        "updated_at": str(payload.get("updated_at") or ""),
        "runs": runs,
    }
    serialized = (json.dumps(body, indent=2, sort_keys=True) + "\n").encode("utf-8")
    runs_bytes = _runs_jsonl_bytes(body["runs"])
    try:
        runs_path.create_exclusive(runs_bytes)
    except FileExistsError as exc:
        existing = runs_path.read_bytes()
        if existing != runs_bytes:
            raise DetConflictError(
                f"catch-up runs NDJSON already exists (immutable): {runs_path}"
            ) from exc
        # Identical orphan sidecar from a prior failed commit — continue.
    try:
        path.create_exclusive(serialized)
    except FileExistsError as exc:
        raise DetConflictError(
            f"catch-up manifest already exists (immutable): {path}"
        ) from exc
    n_runs = len(body["runs"])
    logger.info(
        "silver catchup manifest written",
        path=str(path),
        runs_path=str(runs_path),
        manifest_id=mid,
        runs=n_runs,
    )
    return path


def read_catchup_manifest(
    *,
    manifest_id: str,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> CatchupManifestPayload | None:
    """Load one immutable catch-up manifest by id."""
    mid = validate_catchup_manifest_id(manifest_id)
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    path = catchup_manifest_ref(ops, mid)
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"catch-up manifest must be a JSON object: {path}")
    return cast(CatchupManifestPayload, raw)


def assert_catchup_digest_matches(
    payload: CatchupManifestPayload | Mapping[str, Any],
    *,
    expected_digest: str,
) -> None:
    """Fail closed when the live plan no longer matches the approved digest."""
    want = validate_catchup_content_digest(expected_digest)
    got = str(payload.get("content_digest") or "")
    if got != want:
        raise ValueError(
            "catch-up content changed since dry-run; "
            f"approved {want}, live plan {got or '(missing)'}. "
            "Re-run silver_catchup_dry_run / --dry-run and re-approve."
        )


def catchup_vars_from_manifest(
    payload: CatchupManifestPayload | Mapping[str, Any],
) -> dict[str, Any]:
    """Tiny dbt ``--vars`` pointer: id only (heal set stays in the scm file)."""
    mid = validate_catchup_manifest_id(str(payload.get("manifest_id") or ""))
    return {
        "det_catchup": True,
        "det_catchup_manifest_id": mid,
    }


def catchup_select_from_manifest(
    payload: CatchupManifestPayload | Mapping[str, Any],
    *,
    project_root: Path,
) -> list[str]:
    """dbt ``--select`` for silver models listed in the manifest."""
    root = project_root.resolve()
    seen: set[str] = set()
    selects: list[str] = []
    for row in payload.get("runs") or []:
        pipe = str(row.get("pipeline") or "").strip()
        if not pipe or pipe in seen:
            continue
        seen.add(pipe)
        try:
            resolved = resolve_pipeline_ref(pipe, project_root=root)
            config = load_pipeline_config(resolved.path)
        except Exception:
            slug = dbt_model_slug(pipe)
            selects.append(f"silver_{slug}")
            continue
        slug = dbt_model_slug(config.name)
        selects.append(f"silver_{slug}")
    return selects


def plan_catchup_manifest(
    *,
    project_root: Path,
    pipeline: str | None = None,
    all_pipelines: bool = False,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_lookback: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    manifest_id: str | None = None,
) -> dict[str, Any]:
    """Build an immutable catch-up manifest payload (does not write).

    Always diffs with ``complete=True`` so apply never persists a truncated
    catch-up set. ``limit`` remains for CLI/approval argv parity only.
    When ``manifest_id`` is set (approved apply), reuse that id; otherwise
    allocate a new ``scm_…`` id for the dry-run / approval plan.
    """
    _ = limit
    root = project_root.resolve()
    mid = (
        validate_catchup_manifest_id(manifest_id)
        if manifest_id is not None
        else new_catchup_manifest_id()
    )
    rel = "/".join((*CATCHUP_DIR, f"{mid}.json"))
    if all_pipelines:
        fleet = diff_bronze_silver_fleet(
            project_root=root,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
            analytics_db=analytics_db,
            complete=True,
        )
        if any(r.get("truncated") for r in (fleet.get("results") or [])):
            raise ValueError(
                "catch-up plan is truncated; refuse to build an incomplete apply manifest"
            )
        payload = manifest_payload_from_catchup(
            fleet.get("catchup_runs") or [], manifest_id=mid
        )
        return {
            "dry_run": True,
            "diff": fleet,
            "manifest": payload,
            "manifest_id": mid,
            "content_digest": payload["content_digest"],
            "manifest_relpath": rel,
            "candidate_mode": fleet.get("candidate_mode"),
            **(
                {"extract_lookback": fleet["extract_lookback"]}
                if fleet.get("extract_lookback")
                else {}
            ),
        }
    if pipeline is None:
        raise ValueError("pipeline is required unless all_pipelines=True")
    one = diff_bronze_silver(
        pipeline,
        project_root=root,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_lookback=extract_lookback,
        analytics_db=analytics_db,
        complete=True,
    )
    if one.get("truncated"):
        raise ValueError(
            "catch-up plan is truncated; refuse to build an incomplete apply manifest"
        )
    payload = manifest_payload_from_catchup(
        one.get("catchup_runs") or [], manifest_id=mid
    )
    return {
        "dry_run": True,
        "diff": one,
        "manifest": payload,
        "manifest_id": mid,
        "content_digest": payload["content_digest"],
        "manifest_relpath": rel,
        "candidate_mode": one.get("candidate_mode"),
        **(
            {"extract_lookback": one["extract_lookback"]}
            if one.get("extract_lookback")
            else {}
        ),
    }

