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
import os
import re
import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from det.errors import DetConflictError
from det.logging import get_logger
from det.optional_deps import require_duckdb
from det.runtime.bronze_runs import list_bronze_runs
from det.runtime.config import PipelineConfig, load_pipeline_config
from det.runtime.ids import dbt_model_slug, parse_canonical_id
from det.runtime.lake import LakeRef, relpath, resolve_lake_roots
from det.runtime.limits import DEFAULT_LIST_LIMIT, clamp_list_limit
from det.runtime.meta import identity_iso
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref
from det.runtime.settings import DetSettings, get_active_settings
from det.runtime.warehouse_paths import analytics_duckdb_path

logger = get_logger(__name__)

CATCHUP_DIR = ("ops", "silver_catchup")
MANIFEST_VERSION = 1
MANIFEST_ID_PREFIX = "scm_"
CATCHUP_BQ_EXTERNAL_TABLE_PREFIX = "_det_catchup_runs_"
_MANIFEST_ID_RE = re.compile(r"^scm_[0-9a-f]{16}$")
_CONTENT_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
# Safety cap when building an apply manifest (display diffs stay at DEFAULT_LIST_LIMIT).
_APPLY_BRONZE_CAP = 100_000


def silver_relation(config: PipelineConfig) -> tuple[str, str]:
    """Return ``(schema, table)`` for the scaffolded parent silver model."""
    provider, _ = parse_canonical_id(config.name)
    slug = dbt_model_slug(config.name)
    return f"silver_{provider}", f"silver_{slug}"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def catchup_manifest_ref(ops_lake: LakeRef, manifest_id: str) -> LakeRef:
    mid = validate_catchup_manifest_id(manifest_id)
    ref = ops_lake
    for part in CATCHUP_DIR:
        ref = ref / part
    return ref / f"{mid}.json"


def catchup_runs_ref(ops_lake: LakeRef, manifest_id: str) -> LakeRef:
    """Sibling NDJSON (``<mid>.runs.jsonl``) for BigQuery external-table heal."""
    mid = validate_catchup_manifest_id(manifest_id)
    ref = ops_lake
    for part in CATCHUP_DIR:
        ref = ref / part
    return ref / f"{mid}.runs.jsonl"


def catchup_runs_ref_from_manifest(manifest_ref: LakeRef) -> LakeRef:
    """Derive ``<mid>.runs.jsonl`` from an ``<mid>.json`` lake ref."""
    name = manifest_ref.name
    if not name.endswith(".json"):
        raise ValueError(f"catch-up manifest path must end with .json, got {name!r}")
    return manifest_ref.parent / f"{name[:-5]}.runs.jsonl"


def _runs_jsonl_bytes(runs: Sequence[dict[str, Any]]) -> bytes:
    lines: list[str] = []
    for raw in runs:
        row = {
            "pipeline": str(raw.get("pipeline") or ""),
            "interval_start": str(raw.get("interval_start") or ""),
            "interval_end": str(raw.get("interval_end") or ""),
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


def catchup_content_digest(runs: Sequence[dict[str, Any]]) -> str:
    """Digest over coverage keys only (stable across plan timestamps)."""
    rows: list[dict[str, str]] = []
    for raw in runs:
        rows.append(
            {
                "pipeline": str(raw.get("pipeline") or ""),
                "interval_start": str(raw.get("interval_start") or ""),
                "interval_end": str(raw.get("interval_end") or ""),
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


def load_catchup_runs_from_jsonl(runs_path: LakeRef) -> list[dict[str, Any]]:
    """Parse sibling ``.runs.jsonl`` into run dicts (one JSON object per line)."""
    text = runs_path.read_text(encoding="utf-8")
    rows: list[dict[str, Any]] = []
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
        rows.append(raw)
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


def resolve_ops_lake(
    *,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    active = settings if settings is not None else get_active_settings()
    if active is None:
        active = DetSettings.from_env(project_root=project_root)
    if lake_path is not None and str(lake_path).strip():
        active = active.with_overrides(lake_override=str(lake_path).strip())
    roots = resolve_lake_roots(active, project_root=project_root)
    return roots.ops


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


def analytics_target_is_bigquery() -> bool:
    """True when ``DET_DBT_TARGET=bigquery`` (same signal as dbt profiles)."""
    return (os.environ.get("DET_DBT_TARGET") or "").strip() == "bigquery"


def _list_silver_extract_runs_duckdb(
    config: PipelineConfig,
    *,
    project_root: Path,
    analytics_db: Path | None = None,
) -> tuple[set[tuple[str, str, str]], str | None]:
    schema, table = silver_relation(config)
    db_path = analytics_db if analytics_db is not None else analytics_duckdb_path(project_root)
    if not db_path.is_file():
        return (
            set(),
            "catch-up silver coverage uses DuckDB analytics; "
            f"DuckDB file not found: {db_path}",
        )
    duckdb = require_duckdb()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        try:
            exists = con.execute(
                """
                select count(*) from information_schema.tables
                where table_schema = ? and table_name = ?
                """,
                [schema, table],
            ).fetchone()
        except Exception as exc:
            return set(), f"catch-up silver coverage uses DuckDB analytics; {exc}"
        if not exists or exists[0] == 0:
            return (
                set(),
                "catch-up silver coverage uses DuckDB analytics; "
                f"table not found: {schema}.{table}",
            )
        # schema/table from silver_relation (pipeline ids), not caller SQL.
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        try:
            rows = con.execute(
                f"""
                select distinct
                    "__interval_start_datetime",
                    "__interval_end_datetime",
                    "__extract_run_datetime"
                from {qualified}
                """  # noqa: S608
            ).fetchall()
        except Exception as exc:
            # Missing interval columns (or other SELECT failures) must not look
            # like a valid empty silver — that would invent full catch-up holes.
            raise ValueError(
                "catch-up silver coverage query failed for "
                f"{schema}.{table}: {exc}"
            ) from exc
    finally:
        con.close()
    out: set[tuple[str, str, str]] = set()
    for start_raw, end_raw, run_raw in rows:
        key = _coverage_key(start_raw, end_raw, run_raw)
        if key is not None:
            out.add(key)
    return out, None


def _list_silver_extract_runs_bigquery(
    config: PipelineConfig,
) -> tuple[set[tuple[str, str, str]], str | None]:
    project = (
        os.environ.get("DET_GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or ""
    ).strip()
    if not project:
        return (
            set(),
            "catch-up silver coverage uses BigQuery; "
            "DET_GCP_PROJECT (or GOOGLE_CLOUD_PROJECT) is required",
        )
    try:
        from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]
    except ImportError:
        return (
            set(),
            "catch-up silver coverage uses BigQuery; "
            'install google-cloud-bigquery (uv pip install -e ".[bigquery]")',
        )
    schema, table = silver_relation(config)
    qualified = f"`{project}.{schema}.{table}`"
    sql = f"""
        select distinct
            `__interval_start_datetime`,
            `__interval_end_datetime`,
            `__extract_run_datetime`
        from {qualified}
    """  # noqa: S608
    try:
        client = bigquery.Client(project=project)
        rows = list(client.query(sql).result())
    except Exception as exc:
        return set(), f"catch-up silver coverage uses BigQuery; {exc}"
    out: set[tuple[str, str, str]] = set()
    for row in rows:
        key = _coverage_key(row[0], row[1], row[2])
        if key is not None:
            out.add(key)
    return out, None


def list_silver_extract_runs(
    config: PipelineConfig,
    *,
    project_root: Path,
    analytics_db: Path | None = None,
) -> tuple[set[tuple[str, str, str]], str | None]:
    """Distinct silver coverage keys ``(interval_start, interval_end, extract_run)``.

    When ``DET_DBT_TARGET=bigquery``, reads BigQuery silver
    (``DET_GCP_PROJECT`` / custom schema from ``silver_relation``). Otherwise
    reads DuckDB analytics (``analytics_duckdb_path`` / ``DET_ANALYTICS_DUCKDB``).

    Extract-run timestamps alone are not unique across intervals (parallel runs can
    share a second-precision clock), so membership must include interval bounds.
    """
    if analytics_target_is_bigquery():
        return _list_silver_extract_runs_bigquery(config)
    return _list_silver_extract_runs_duckdb(
        config, project_root=project_root, analytics_db=analytics_db
    )


def _latest_per_interval(
    bronze_runs: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map (interval_start, interval_end) → bronze run with max extract_run_datetime."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for run in bronze_runs:
        start = str(run.get("interval_start") or "")
        end = str(run.get("interval_end") or "")
        ts = _norm_ts(run.get("extract_run_datetime"))
        if not start or not end or not ts:
            continue
        key = (start, end)
        prev = best.get(key)
        if prev is None or _norm_ts(prev.get("extract_run_datetime")) < ts:
            best[key] = {
                "interval_start": start,
                "interval_end": end,
                "extract_run_datetime": ts,
            }
    return best


def diff_bronze_silver(
    pipeline: str | PipelineConfig,
    *,
    project_root: Path,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    detected_at: str | None = None,
    complete: bool = False,
) -> dict[str, Any]:
    """Compare latest bronze extract-run per interval to silver coverage.

    When ``complete=True`` (manifest plan/apply), list bronze without the MCP
    display clamp and return every catch-up row. Raises if the safety cap is hit.
    """
    root = project_root.resolve()
    if isinstance(pipeline, PipelineConfig):
        config = pipeline
    else:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        config = load_pipeline_config(resolved.path)

    if complete:
        capped = _APPLY_BRONZE_CAP
    else:
        capped = clamp_list_limit(limit)
    bronze_runs, bronze_note = list_bronze_runs(
        config,
        root=root,
        limit=capped,
        interval_start=interval_start,
        interval_end=interval_end,
    )
    if complete and len(bronze_runs) >= _APPLY_BRONZE_CAP:
        raise ValueError(
            "catch-up apply found too many bronze runs "
            f"(>={_APPLY_BRONZE_CAP}); narrow -s/-e or raise the apply cap"
        )
    silver_keys, silver_note = list_silver_extract_runs(
        config, project_root=root, analytics_db=analytics_db
    )
    if complete and silver_note:
        raise ValueError(
            "catch-up apply requires readable silver coverage; "
            f"{silver_note}"
        )

    latest = _latest_per_interval(bronze_runs)
    stamp = detected_at or datetime.now(UTC).isoformat()

    catchup: list[dict[str, Any]] = []
    ok_intervals: list[dict[str, Any]] = []
    stale_siblings: list[dict[str, Any]] = []

    latest_keys = {
        (
            r["interval_start"],
            r["interval_end"],
            r["extract_run_datetime"],
        )
        for r in latest.values()
    }

    for (start, end), run in sorted(latest.items(), key=lambda kv: kv[0]):
        ts = run["extract_run_datetime"]
        row = {
            "pipeline": config.name,
            "interval_start": start,
            "interval_end": end,
            "extract_run_datetime": ts,
        }
        cov = _coverage_key(start, end, ts)
        if cov is not None and cov in silver_keys:
            ok_intervals.append(row)
        else:
            catchup.append({**row, "detected_at": stamp})

    for run in bronze_runs:
        start = str(run.get("interval_start") or "")
        end = str(run.get("interval_end") or "")
        ts = _norm_ts(run.get("extract_run_datetime"))
        key = (start, end, ts)
        if key in latest_keys:
            continue
        cov = _coverage_key(start, end, ts)
        if cov is not None and cov not in silver_keys:
            stale_siblings.append(
                {
                    "pipeline": config.name,
                    "interval_start": start,
                    "interval_end": end,
                    "extract_run_datetime": ts,
                }
            )

    schema, table = silver_relation(config)
    if complete:
        catchup_out = catchup
        ok_out = ok_intervals
        stale_out = stale_siblings
        truncated = False
    else:
        catchup_out = catchup[:capped]
        ok_out = ok_intervals[:capped]
        stale_out = stale_siblings[:capped]
        truncated = (
            len(catchup) > capped
            or len(ok_intervals) > capped
            or len(stale_siblings) > capped
            or len(bronze_runs) >= capped
        )
    out: dict[str, Any] = {
        "pipeline": config.name,
        "materialized": config.dbt.silver.materialized,
        "silver_schema": schema,
        "silver_table": table,
        "limit": capped if not complete else len(bronze_runs),
        "complete": complete,
        "catchup_runs": catchup_out,
        "ok_intervals": ok_out,
        "stale_siblings_ignored": stale_out,
        "catchup_count": len(catchup),
        "ok_count": len(ok_intervals),
        "stale_siblings_count": len(stale_siblings),
        "truncated": truncated,
    }
    notes = [n for n in (bronze_note, silver_note) if n]
    if notes:
        out["note"] = "; ".join(notes)
    return out


def diff_bronze_silver_fleet(
    *,
    project_root: Path,
    pipelines: Sequence[str] | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    analytics_db: Path | None = None,
    detected_at: str | None = None,
    complete: bool = False,
) -> dict[str, Any]:
    """Run :func:`diff_bronze_silver` for many pipelines; aggregate catch-up rows."""
    root = project_root.resolve()
    ids = list(pipelines) if pipelines is not None else list_pipeline_ids(root)
    stamp = detected_at or datetime.now(UTC).isoformat()
    per_pipeline: list[dict[str, Any]] = []
    catchup_all: list[dict[str, Any]] = []
    for pipe_id in ids:
        one = diff_bronze_silver(
            pipe_id,
            project_root=root,
            interval_start=interval_start,
            interval_end=interval_end,
            limit=limit,
            analytics_db=analytics_db,
            detected_at=stamp,
            complete=complete,
        )
        per_pipeline.append(one)
        catchup_all.extend(one.get("catchup_runs") or [])
    return {
        "pipelines": ids,
        "pipeline_count": len(ids),
        "catchup_runs": catchup_all,
        "catchup_count": len(catchup_all),
        "results": per_pipeline,
        "detected_at": stamp,
        "complete": complete,
    }


def manifest_payload_from_catchup(
    catchup_runs: Sequence[dict[str, Any]],
    *,
    detected_at: str | None = None,
    manifest_id: str | None = None,
) -> dict[str, Any]:
    stamp = detected_at or datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for raw in catchup_runs:
        rows.append(
            {
                "pipeline": str(raw["pipeline"]),
                "extract_run_datetime": _norm_ts(raw["extract_run_datetime"]),
                "interval_start": str(raw.get("interval_start") or ""),
                "interval_end": str(raw.get("interval_end") or ""),
                "detected_at": str(raw.get("detected_at") or stamp),
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
    payload: dict[str, Any],
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
    body = dict(payload)
    body["manifest_id"] = mid
    body["content_digest"] = digest
    serialized = (json.dumps(body, indent=2, sort_keys=True) + "\n").encode("utf-8")
    runs_bytes = _runs_jsonl_bytes(body.get("runs") or [])
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
    n_runs = len(body.get("runs") or [])
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
) -> dict[str, Any] | None:
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
    return raw


def assert_catchup_digest_matches(
    payload: dict[str, Any],
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


def catchup_manifest_file_path(
    *,
    manifest_id: str,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    """Absolute/URI path to ``ops/silver_catchup/<manifest_id>.json``."""
    mid = validate_catchup_manifest_id(manifest_id)
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    return catchup_manifest_ref(ops, mid)


def catchup_runs_file_path(
    *,
    manifest_id: str,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    """Absolute/URI path to ``ops/silver_catchup/<manifest_id>.runs.jsonl``."""
    mid = validate_catchup_manifest_id(manifest_id)
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    return catchup_runs_ref(ops, mid)


def catchup_bq_external_table_name(manifest_id: str) -> str:
    """BigQuery table id for one catch-up heal (isolated per ``scm_…``)."""
    mid = validate_catchup_manifest_id(manifest_id)
    return f"{CATCHUP_BQ_EXTERNAL_TABLE_PREFIX}{mid}"


def catchup_bq_relation(*, project: str, dataset: str, manifest_id: str) -> str:
    """Quoted ``project.dataset._det_catchup_runs_<scm_…>`` for macros/env."""
    table = catchup_bq_external_table_name(manifest_id)
    return f"`{project}.{dataset}.{table}`"


def ensure_bq_catchup_external_table(*, runs_uri: str, manifest_id: str) -> str:
    """Create/replace a manifest-scoped external table over GCS NDJSON.

    Table name is ``_det_catchup_runs_<manifest_id>`` so overlapping catch-up
    builds do not share one relation. Returns the quoted relation string for
    ``DET_CATCHUP_BQ_RELATION``.
    """
    uri = str(runs_uri or "").strip()
    if not uri.startswith("gs://"):
        raise ValueError(
            "BigQuery catch-up requires a gs:// runs NDJSON URI "
            f"(got {uri!r}). Use a GCS ops lake; local-lake → BQ heal is unsupported."
        )
    mid = validate_catchup_manifest_id(manifest_id)
    project = (
        os.environ.get("DET_GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or ""
    ).strip()
    if not project:
        raise ValueError(
            "BigQuery catch-up requires DET_GCP_PROJECT (or GOOGLE_CLOUD_PROJECT)"
        )
    dataset = (os.environ.get("DET_BQ_DATASET") or "analytics").strip() or "analytics"
    location = (os.environ.get("DET_BQ_LOCATION") or "US").strip() or "US"
    try:
        from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]
    except ImportError as exc:
        raise RuntimeError(
            'google-cloud-bigquery is required. Install: uv pip install -e ".[bigquery]"'
        ) from exc

    client = bigquery.Client(project=project)
    _ensure_bq_dataset(client, project, dataset, location)
    table_name = catchup_bq_external_table_name(mid)
    table_id = f"{project}.{dataset}.{table_name}"
    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    external_config.source_uris = [uri]
    external_config.schema = [
        bigquery.SchemaField("pipeline", "STRING"),
        bigquery.SchemaField("interval_start", "STRING"),
        bigquery.SchemaField("interval_end", "STRING"),
        bigquery.SchemaField("extract_run_datetime", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=external_config.schema)
    table.external_data_configuration = external_config
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(table)
    relation = catchup_bq_relation(project=project, dataset=dataset, manifest_id=mid)
    logger.info(
        "silver catchup BQ external table ready",
        relation=relation,
        runs_uri=uri,
        manifest_id=mid,
    )
    return relation


def _ensure_bq_dataset(
    client: Any, project: str, dataset_id: str, location: str
) -> None:
    from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]

    ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ref.location = location
    try:
        client.get_dataset(ref)
    except Exception:
        client.create_dataset(ref, exists_ok=True)


def catchup_vars_from_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """Tiny dbt ``--vars`` pointer: id only (heal set stays in the scm file)."""
    mid = validate_catchup_manifest_id(str(payload.get("manifest_id") or ""))
    return {
        "det_catchup": True,
        "det_catchup_manifest_id": mid,
    }


def catchup_select_from_manifest(
    payload: dict[str, Any],
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
        }
    if pipeline is None:
        raise ValueError("pipeline is required unless all_pipelines=True")
    one = diff_bronze_silver(
        pipeline,
        project_root=root,
        interval_start=interval_start,
        interval_end=interval_end,
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
    }


def manifest_relpath_for_root(project_root: Path, path: LakeRef) -> str:
    return relpath(path, project_root)
