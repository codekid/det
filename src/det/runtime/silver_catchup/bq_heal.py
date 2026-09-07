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

import os
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from det.logging import get_logger
from det.runtime.config import PipelineConfig
from det.runtime.silver_catchup.ids import (
    _MANIFEST_ID_RE,
    _SILVER_PROBE_CHUNK,
    _coverage_key,
    silver_relation,
    validate_catchup_manifest_id,
)

logger = get_logger(__name__)

CATCHUP_BQ_EXTERNAL_TABLE_PREFIX = "_det_catchup_runs_"

def analytics_target_is_bigquery() -> bool:
    """True when ``DET_DBT_TARGET=bigquery`` (same signal as dbt profiles)."""
    return (os.environ.get("DET_DBT_TARGET") or "").strip() == "bigquery"

def _list_silver_extract_runs_bigquery(
    config: PipelineConfig,
    *,
    intervals: Sequence[tuple[str, str]] | None = None,
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
    if intervals is not None and len(intervals) == 0:
        return set(), None
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
    try:
        client = bigquery.Client(project=project)
        if intervals is None:
            sql = f"""
                select distinct
                    `__interval_start_datetime`,
                    `__interval_end_datetime`,
                    `__extract_run_datetime`
                from {qualified}
            """  # noqa: S608
            rows = list(client.query(sql).result())
        else:
            rows = []
            for i in range(0, len(intervals), _SILVER_PROBE_CHUNK):
                chunk = intervals[i : i + _SILVER_PROBE_CHUNK]
                ors = " or ".join(
                    f"(`__interval_start_datetime` = @s{j} "
                    f"and `__interval_end_datetime` = @e{j})"
                    for j in range(len(chunk))
                )
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        p
                        for j, (start, end) in enumerate(chunk)
                        for p in (
                            bigquery.ScalarQueryParameter(f"s{j}", "TIMESTAMP", start),
                            bigquery.ScalarQueryParameter(f"e{j}", "TIMESTAMP", end),
                        )
                    ]
                )
                sql = f"""
                    select distinct
                        `__interval_start_datetime`,
                        `__interval_end_datetime`,
                        `__extract_run_datetime`
                    from {qualified}
                    where {ors}
                """  # noqa: S608
                rows.extend(list(client.query(sql, job_config=job_config).result()))
    except Exception as exc:
        return set(), f"catch-up silver coverage uses BigQuery; {exc}"
    out: set[tuple[str, str, str]] = set()
    for row in rows:
        key = _coverage_key(row[0], row[1], row[2])
        if key is not None:
            out.add(key)
    return out, None

def catchup_bq_external_table_name(manifest_id: str) -> str:
    """BigQuery table id for one catch-up heal (isolated per ``scm_…``)."""
    mid = validate_catchup_manifest_id(manifest_id)
    return f"{CATCHUP_BQ_EXTERNAL_TABLE_PREFIX}{mid}"


def catchup_bq_relation(*, project: str, dataset: str, manifest_id: str) -> str:
    """Quoted ``project.dataset._det_catchup_runs_<scm_…>`` for macros/env."""
    table = catchup_bq_external_table_name(manifest_id)
    return f"`{project}.{dataset}.{table}`"


def _bq_project_dataset_location() -> tuple[str, str, str]:
    """Resolve ``(project, dataset, location)`` for catch-up BQ helpers."""
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
    return project, dataset, location


def _bq_client() -> tuple[Any, str, str, str]:
    try:
        from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]
    except ImportError as exc:
        raise RuntimeError(
            'google-cloud-bigquery is required. Install: uv pip install -e ".[bigquery]"'
        ) from exc
    project, dataset, location = _bq_project_dataset_location()
    return bigquery.Client(project=project), project, dataset, location

def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _manifest_id_from_catchup_table_name(table_id: str) -> str | None:
    name = str(table_id or "").strip()
    prefix = CATCHUP_BQ_EXTERNAL_TABLE_PREFIX
    if not name.startswith(prefix):
        return None
    suffix = name[len(prefix) :]
    if not _MANIFEST_ID_RE.fullmatch(suffix):
        return None
    return suffix

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
    client, project, dataset, location = _bq_client()
    from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]

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

