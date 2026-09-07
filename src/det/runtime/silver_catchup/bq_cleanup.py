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

from datetime import UTC, datetime
from typing import Any

from det.logging import get_logger
from det.runtime.meta import identity_iso
from det.runtime.silver_catchup.bq_heal import (
    CATCHUP_BQ_EXTERNAL_TABLE_PREFIX,
    _as_utc,
    _manifest_id_from_catchup_table_name,
    catchup_bq_external_table_name,
    catchup_bq_relation,
)
from det.runtime.silver_catchup.ids import (
    parse_duration,
    validate_catchup_manifest_id,
)

logger = get_logger(__name__)

def validate_bq_catchup_cleanup_scope(
    *,
    manifest_id: str | None,
    older_than: str | None,
    created_before: str | None = None,
    list_mode: bool = False,
) -> None:
    """Reject illegal flag combinations for BQ catch-up cleanup."""
    mid = bool(manifest_id and str(manifest_id).strip())
    older = bool(older_than and str(older_than).strip())
    before = bool(created_before and str(created_before).strip())
    if older and before:
        raise ValueError("--older-than cannot combine with --created-before")
    if mid and older:
        raise ValueError("--manifest-id cannot combine with --older-than")
    if mid and before:
        raise ValueError("--manifest-id cannot combine with --created-before")
    if list_mode:
        if mid:
            raise ValueError("--list cannot combine with --manifest-id")
        return
    if not mid and not older and not before:
        raise ValueError(
            "exactly one of --manifest-id, --older-than, or --created-before "
            "is required"
        )


def resolve_bq_catchup_cleanup_cutoff(
    *,
    older_than: str | None = None,
    created_before: str | None = None,
    now: datetime | None = None,
) -> tuple[datetime | None, str | None, str | None]:
    """Resolve an immutable UTC cutoff for retention cleanup.

    Returns ``(cutoff_dt, created_before_iso, older_than_raw)``. Relative
    ``older_than`` is converted once at plan time; apply should pass the
    resulting ``created_before`` so the target set cannot drift.
    """
    older_raw = str(older_than).strip() if older_than is not None else ""
    before_raw = str(created_before).strip() if created_before is not None else ""
    if older_raw and before_raw:
        raise ValueError("--older-than cannot combine with --created-before")
    if before_raw:
        iso = identity_iso(before_raw)
        return datetime.fromisoformat(iso), iso, None
    if older_raw:
        delta = parse_duration(older_raw, what="older-than")
        stamp = now if now is not None else datetime.now(UTC)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        else:
            stamp = stamp.astimezone(UTC)
        cutoff = stamp - delta
        iso = identity_iso(cutoff)
        return datetime.fromisoformat(iso), iso, older_raw
    return None, None, None

def list_bq_catchup_external_tables(
    *,
    older_than: str | None = None,
    created_before: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """List ``_det_catchup_runs_*`` tables; optional age filter on BQ ``created``.

    Prefer ``created_before`` (immutable ISO UTC) for approve→apply. Relative
    ``older_than`` is for inspect/list and dry-run preview only.

    When a cutoff is set, rows with missing ``created`` are skipped (never
    treated as older).
    """
    cutoff, _cutoff_iso, _older = resolve_bq_catchup_cleanup_cutoff(
        older_than=older_than, created_before=created_before, now=now
    )
    from det.runtime import silver_catchup as _sc

    client, project, dataset, _location = _sc._bq_client()
    rows: list[dict[str, Any]] = []
    for item in client.list_tables(f"{project}.{dataset}"):
        table_name = str(getattr(item, "table_id", "") or "")
        if not table_name.startswith(CATCHUP_BQ_EXTERNAL_TABLE_PREFIX):
            continue
        mid = _manifest_id_from_catchup_table_name(table_name)
        created = _as_utc(getattr(item, "created", None))
        if created is None:
            # list_tables often omits created — fetch full metadata once.
            try:
                full = client.get_table(f"{project}.{dataset}.{table_name}")
                created = _as_utc(getattr(full, "created", None))
            except Exception:
                created = None
        if cutoff is not None:
            if created is None or created > cutoff:
                continue
        relation = (
            catchup_bq_relation(project=project, dataset=dataset, manifest_id=mid)
            if mid
            else f"`{project}.{dataset}.{table_name}`"
        )
        rows.append(
            {
                "table_id": table_name,
                "relation": relation,
                "created": created.isoformat() if created is not None else None,
                "manifest_id": mid,
            }
        )
    rows.sort(key=lambda r: (r.get("created") or "", r["table_id"]))
    return rows

def drop_bq_catchup_external_table(*, manifest_id: str) -> dict[str, Any]:
    """Delete one manifest-scoped catch-up external table (``not_found_ok``)."""
    mid = validate_catchup_manifest_id(manifest_id)
    from det.runtime import silver_catchup as _sc

    client, project, dataset, _location = _sc._bq_client()
    table_name = catchup_bq_external_table_name(mid)
    table_id = f"{project}.{dataset}.{table_name}"
    relation = catchup_bq_relation(project=project, dataset=dataset, manifest_id=mid)
    existed = False
    try:
        client.get_table(table_id)
        existed = True
    except Exception:
        existed = False
    client.delete_table(table_id, not_found_ok=True)
    logger.info(
        "silver catchup BQ external table dropped",
        relation=relation,
        manifest_id=mid,
        existed=existed,
    )
    return {
        "manifest_id": mid,
        "relation": relation,
        "existed": existed,
        "dropped": existed,
    }

def plan_bq_catchup_cleanup(
    *,
    manifest_id: str | None = None,
    older_than: str | None = None,
    created_before: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Dry-run payload: which ``_det_catchup_runs_*`` tables would be dropped.

    Retention plans always emit ``created_before`` (UTC ISO) so approve→apply
    can bind an immutable cutoff instead of a drifting relative duration.
    """
    validate_bq_catchup_cleanup_scope(
        manifest_id=manifest_id,
        older_than=older_than,
        created_before=created_before,
        list_mode=False,
    )
    mid_raw = str(manifest_id).strip() if manifest_id else ""
    from det.runtime import silver_catchup as _sc

    project, dataset, _location = _sc._bq_project_dataset_location()
    targets: list[dict[str, Any]] = []
    cutoff_iso: str | None = None
    older_raw: str | None = None
    if mid_raw:
        mid = validate_catchup_manifest_id(mid_raw)
        relation = catchup_bq_relation(
            project=project, dataset=dataset, manifest_id=mid
        )
        client, _, _, _ = _sc._bq_client()
        table_id = f"{project}.{dataset}.{catchup_bq_external_table_name(mid)}"
        existed = False
        created: str | None = None
        try:
            full = client.get_table(table_id)
            existed = True
            created_dt = _as_utc(getattr(full, "created", None))
            created = created_dt.isoformat() if created_dt is not None else None
        except Exception:
            existed = False
        targets.append(
            {
                "table_id": catchup_bq_external_table_name(mid),
                "relation": relation,
                "created": created,
                "manifest_id": mid,
                "existed": existed,
            }
        )
        mode = "manifest_id"
    else:
        _cutoff, cutoff_iso, older_raw = resolve_bq_catchup_cleanup_cutoff(
            older_than=older_than, created_before=created_before, now=now
        )
        listed = list_bq_catchup_external_tables(
            created_before=cutoff_iso, now=now
        )
        for row in listed:
            if not row.get("manifest_id"):
                continue
            targets.append({**row, "existed": True})
        mode = "created_before"
    return {
        "mode": mode,
        "manifest_id": mid_raw or None,
        "older_than": older_raw,
        "created_before": cutoff_iso,
        "project": project,
        "dataset": dataset,
        "targets": targets,
        "target_count": len(targets),
    }

def apply_bq_catchup_cleanup(
    *,
    manifest_id: str | None = None,
    older_than: str | None = None,
    created_before: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Drop planned catch-up external tables (single id or age-filtered set).

    Retention apply requires immutable ``created_before`` (from plan/dry-run).
    Relative ``older_than`` is rejected so the target set cannot drift with
    ``now``. Manifest-id apply is unchanged.
    """
    mid_raw = str(manifest_id).strip() if manifest_id else ""
    older_raw = str(older_than).strip() if older_than else ""
    before_raw = str(created_before).strip() if created_before else ""
    if older_raw:
        raise ValueError(
            "apply_bq_catchup_cleanup rejects --older-than; pass "
            "--created-before from plan/dry-run (relative duration would drift)"
        )
    if not mid_raw and not before_raw:
        raise ValueError(
            "apply_bq_catchup_cleanup requires --manifest-id or --created-before"
        )
    planned = plan_bq_catchup_cleanup(
        manifest_id=mid_raw or None,
        created_before=before_raw or None,
        now=now,
    )
    results: list[dict[str, Any]] = []
    for row in planned["targets"]:
        mid = row.get("manifest_id")
        if not mid:
            continue
        results.append(drop_bq_catchup_external_table(manifest_id=str(mid)))
    dropped_count = sum(1 for r in results if r.get("dropped"))
    return {
        **planned,
        "results": results,
        "dropped_count": dropped_count,
    }

