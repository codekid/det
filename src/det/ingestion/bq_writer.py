"""BigQuery bronze writer (replace-by-extract-run).

Project comes from ``DetSettings.bq_project`` / ``DET_GCP_PROJECT`` — never YAML.
Dataset defaults to ``{prefix}_{provider}`` (prefix = ``DET_BQ_DATASET_PREFIX`` or
the medallion ``bronze``); ``destination.bq_dataset`` overrides the full dataset id.
Table is always ``{source}_v{wire_version}`` from ``config.canonical_id``.
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from det.ingestion.chunks import iter_chunks
from det.ingestion.sql_replace import (
    assert_chunk_matches_identity,
    resolve_run_identity,
)
from det.logging import get_logger
from det.optional_deps import require_bigquery
from det.runtime.config import DestinationConfig, PipelineConfig
from det.runtime.config.destination import BigQueryDestinationPartitionConfig
from det.runtime.ids import medallion_prefix, parse_canonical_id
from det.runtime.schema_shapes import is_array_prop, is_object_prop
from det.runtime.settings import DetSettings, get_active_settings

logger = get_logger(__name__)

ENV_GCP_PROJECT = "DET_GCP_PROJECT"
ENV_BQ_DATASET_PREFIX = "DET_BQ_DATASET_PREFIX"

# BQ dataset/table ids: letters, digits, underscores.
_BQ_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BQ_IDENT_MAX = 1024

# DET meta → BigQuery types (order matches attach_meta / sql_types._META_ORDER).
BQ_META_TYPES: dict[str, str] = {
    "__row_hash": "STRING",
    "__filename": "STRING",
    "__extract_run_datetime": "TIMESTAMP",
    "__bronze_loaded_at": "TIMESTAMP",
    "__interval_start_datetime": "TIMESTAMP",
    "__interval_end_datetime": "TIMESTAMP",
    "__data_interval_date": "DATE",
}

__all__ = [
    "BQ_META_TYPES",
    "ENV_BQ_DATASET_PREFIX",
    "ENV_GCP_PROJECT",
    "apply_bq_table_layout",
    "bq_dataset_for_config",
    "bq_layout_destination",
    "bq_normalize_ident",
    "bq_project_from_settings",
    "bq_schema_columns",
    "bq_schema_from_json_schema",
    "bq_table_for_config",
    "bq_type_for_prop",
    "resolve_bq_partition_by",
    "write_to_bigquery",
]

_GRANULARITY_TO_BQ = {
    "hour": "HOUR",
    "day": "DAY",
    "month": "MONTH",
    "year": "YEAR",
}


# Columns the replace-by-run DELETE already predicates on — safe with
# require_partition_filter.
_DELETE_PARTITION_SAFE_FIELDS = frozenset(
    {
        "__extract_run_datetime",
        "__interval_start_datetime",
        "__interval_end_datetime",
    }
)


def bq_layout_destination(config: PipelineConfig) -> DestinationConfig:
    """
    Destination used for BigQuery table create/layout.

    Native ``destination.type: bigquery`` pipelines use their config as-is.
    ``det export-bq`` reads bronze from filesystem/iceberg, so this returns a
    synthetic bigquery destination (default extract_run grain) without mutating
    the source config used for bronze reads.
    """
    dest = config.destination
    if dest.type == "bigquery":
        return dest
    return DestinationConfig(type="bigquery", partition="extract_run")


def resolve_bq_partition_by(
    destination: DestinationConfig,
) -> BigQueryDestinationPartitionConfig | None:
    """
    Resolve BigQuery physical partition for create_table.

    Precedence: ``destination.bigquery.partition_by`` (source grain) wins.
    Else ``destination.partition == extract_run`` → ``__extract_run_datetime`` DAY.
    Else ``none`` → unpartitioned (``None``).
    """
    if destination.type != "bigquery":
        raise ValueError(
            f"resolve_bq_partition_by requires destination.type bigquery, got {destination.type}"
        )
    block = destination.bigquery
    if block is not None and block.partition_by is not None:
        part = block.partition_by
        if (
            block.require_partition_filter
            and part.field not in _DELETE_PARTITION_SAFE_FIELDS
        ):
            raise ValueError(
                "destination.bigquery.require_partition_filter with "
                f"partition_by.field={part.field!r} is incompatible with "
                "replace-by-run DELETE (predicates are only on "
                f"{sorted(_DELETE_PARTITION_SAFE_FIELDS)}); use "
                "__extract_run_datetime or omit require_partition_filter"
            )
        return part
    grain = destination.partition or "extract_run"
    if grain == "none":
        return None
    return BigQueryDestinationPartitionConfig(
        field="__extract_run_datetime",
        data_type="timestamp",
        granularity="day",
    )


def apply_bq_table_layout(table_ref: Any, bigquery: Any, destination: DestinationConfig) -> None:
    """Set time_partitioning / clustering on a ``bigquery.Table`` before create."""
    partition = resolve_bq_partition_by(destination)
    if partition is not None:
        type_name = _GRANULARITY_TO_BQ.get(partition.granularity, "DAY")
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=getattr(bigquery.TimePartitioningType, type_name),
            field=partition.field,
        )
    block = destination.bigquery
    if block is not None and block.cluster_by:
        table_ref.clustering_fields = list(block.cluster_by)
    if block is not None and block.require_partition_filter and partition is not None:
        table_ref.require_partition_filter = True


def bq_normalize_ident(name: str, *, what: str = "BigQuery identifier") -> str:
    """Normalize hyphens to underscores and validate a BQ dataset/table id."""
    text = str(name).strip().replace("-", "_")
    if not text or len(text) > _BQ_IDENT_MAX or not _BQ_IDENT.fullmatch(text):
        raise ValueError(
            f"{what} must be [A-Za-z_][A-Za-z0-9_]* "
            f"(max {_BQ_IDENT_MAX} chars), got {name!r}"
        )
    return text


def bq_project_from_settings(settings: DetSettings | None = None) -> str:
    """Resolve GCP project for BQ bronze writes."""
    active = settings if settings is not None else get_active_settings()
    project = (active.bq_project if active is not None else None) or ""
    project = str(project).strip()
    if not project:
        raise ValueError(
            f"BigQuery bronze requires {ENV_GCP_PROJECT} "
            "(or GOOGLE_CLOUD_PROJECT)"
        )
    return project


def bq_dataset_for_config(
    config: PipelineConfig,
    settings: DetSettings | None = None,
    *,
    bq_dataset_override: str | None = None,
) -> str:
    """
    Resolve the BQ dataset id for a pipeline.

    Precedence: explicit override → ``destination.bq_dataset`` →
    ``{prefix}_{provider}`` where prefix is ``settings.bq_dataset_prefix`` or
    the medallion prefix (default ``bronze``).
    """
    override = (bq_dataset_override or "").strip() or (config.destination.bq_dataset or "").strip()
    if override:
        return bq_normalize_ident(override, what="destination.bq_dataset")

    active = settings if settings is not None else get_active_settings()
    prefix = (
        (active.bq_dataset_prefix if active is not None else None)
        or medallion_prefix(config.destination.dataset)
        or config.medallion.bronze_prefix
        or "bronze"
    )
    prefix = bq_normalize_ident(str(prefix).strip(), what="bq dataset prefix")
    provider, _ = parse_canonical_id(config.canonical_id)
    provider = bq_normalize_ident(provider, what="provider")
    return f"{prefix}_{provider}"


def bq_table_for_config(config: PipelineConfig) -> str:
    """Table leaf: ``{source}_v{wire_version}`` from ``config.canonical_id``."""
    _, source_name = parse_canonical_id(config.canonical_id)
    return bq_normalize_ident(source_name, what="bq table")


def _allowed_types(prop: dict[str, Any]) -> set[str]:
    raw = prop.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {t for t in raw if isinstance(t, str)}
    return set()


def bq_type_for_prop(prop: dict[str, Any]) -> str:
    """Map a JSON Schema property to a BigQuery type name. Nested → JSON.

    ``number`` wins over ``integer`` when both appear (e.g.
    ``type: [integer, number, "null"]``) so fractional values stay representable
    as FLOAT64. Integer-only schemas still map to INT64.
    """
    if is_object_prop(prop) or is_array_prop(prop):
        return "JSON"
    allowed = _allowed_types(prop)
    if "number" in allowed:
        return "FLOAT64"
    if "integer" in allowed:
        return "INT64"
    if "boolean" in allowed:
        return "BOOL"
    return "STRING"


def bq_schema_columns(schema: dict[str, Any]) -> list[tuple[str, str]]:
    """CREATE/LOAD columns: JSON Schema properties then DET meta."""
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, prop in props.items():
        if not isinstance(name, str):
            continue
        bq_type = bq_type_for_prop(prop) if isinstance(prop, dict) else "STRING"
        out.append((name, bq_type))
        seen.add(name)
    for name, bq_type in BQ_META_TYPES.items():
        if name in seen:
            continue
        out.append((name, bq_type))
    return out


def bq_schema_from_json_schema(
    schema: dict[str, Any],
    *,
    partition: BigQueryDestinationPartitionConfig | None = None,
) -> list[Any]:
    """Build ``google.cloud.bigquery.SchemaField`` list from a DET JSON Schema.

    When ``partition`` targets a payload column, override that column's BQ type
    with ``partition.data_type`` so time partitioning is valid (JSON Schema
    strings otherwise become STRING). Meta columns keep ``BQ_META_TYPES``.
    """
    bigquery = require_bigquery()
    columns = dict(bq_schema_columns(schema))
    if partition is not None:
        if partition.field not in columns:
            raise ValueError(
                f"partition_by.field {partition.field!r} is not a schema column"
            )
        if partition.field not in BQ_META_TYPES:
            columns[partition.field] = partition.data_type.upper()
    fields: list[Any] = []
    for name, bq_type in columns.items():
        fields.append(bigquery.SchemaField(name, bq_type, mode="NULLABLE"))
    return fields


def _jsonable_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _rows_to_ndjson(rows: Sequence[dict[str, Any]]) -> bytes:
    buf = io.BytesIO()
    for row in rows:
        payload = {k: _jsonable_cell(v) for k, v in row.items()}
        buf.write(json.dumps(payload, default=str).encode("utf-8"))
        buf.write(b"\n")
    return buf.getvalue()


def write_to_bigquery(
    records: Iterable[dict[str, Any]],
    *,
    config: PipelineConfig,
    json_schema: dict[str, Any],
    settings: DetSettings | None = None,
    bq_dataset_override: str | None = None,
    chunk_rows: int = 10_000,
    run_identity: tuple[str, str, str] | None = None,
    on_chunk: Callable[[], None] | None = None,
) -> Path:
    """
    Replace-by-extract-run DET bronze write into BigQuery.

    Ensures the dataset and table exist (layout from ``destination.partition`` /
    ``destination.bigquery``), deletes any rows for this extract run, then loads
    NDJSON via a BQ load job. Returns a logical path
    ``bq/{project}/{dataset}/{table}``.
    """
    bigquery = require_bigquery()
    active = settings if settings is not None else get_active_settings()
    project = bq_project_from_settings(active)
    dataset = bq_dataset_for_config(
        config, active, bq_dataset_override=bq_dataset_override
    )
    table = bq_table_for_config(config)
    table_id = f"{project}.{dataset}.{table}"

    chunks = iter_chunks(records, chunk_rows)
    first_chunk = next(chunks, None)
    identity = resolve_run_identity(run_identity, first_chunk)
    layout = bq_layout_destination(config)
    partition = resolve_bq_partition_by(layout)
    schema_fields = bq_schema_from_json_schema(json_schema, partition=partition)
    client = bigquery.Client(project=project)

    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    client.create_dataset(dataset_ref, exists_ok=True)

    table_ref = bigquery.Table(table_id, schema=schema_fields)
    apply_bq_table_layout(table_ref, bigquery, layout)
    client.create_table(table_ref, exists_ok=True)

    start, end, run = identity
    # project/dataset/table are normalized BQ idents; values are query params.
    delete_sql = (
        f"DELETE FROM `{project}.{dataset}.{table}` "  # noqa: S608
        f"WHERE __interval_start_datetime = @start "
        f"AND __interval_end_datetime = @end "
        f"AND __extract_run_datetime = @run"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
            bigquery.ScalarQueryParameter("run", "TIMESTAMP", run),
        ]
    )
    client.query(delete_sql, job_config=job_config).result()

    total = 0
    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema_fields,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=False,
    )

    def _load_chunk(chunk: list[dict[str, Any]]) -> None:
        nonlocal total
        assert_chunk_matches_identity(chunk, identity)
        payload = _rows_to_ndjson(chunk)
        job = client.load_table_from_file(
            io.BytesIO(payload),
            table_id,
            job_config=load_config,
            rewind=True,
        )
        job.result()
        total += len(chunk)
        if on_chunk is not None:
            on_chunk()

    try:
        if first_chunk is not None:
            _load_chunk(first_chunk)
            for chunk in chunks:
                _load_chunk(chunk)
    except Exception:
        try:
            client.query(delete_sql, job_config=job_config).result()
        except Exception:
            logger.warning(
                "failed to clean partial BigQuery load",
                table=table_id,
                exc_info=True,
            )
        raise

    logger.info(
        "bigquery load finished",
        table=table_id,
        rows=total,
        pipeline=config.name,
        extract_run_datetime=run,
    )
    return Path("bq") / project / dataset / table
