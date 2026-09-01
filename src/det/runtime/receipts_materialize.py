"""Project `{lake}/runs/` JSON receipts into Iceberg ``ops.run_receipts``.

JSON under ``runs/`` remains the attempt log; Iceberg is a replace-by-day projection
for dbt ops models. Requires ``det[iceberg]``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, cast

from det.ingestion.iceberg_catalog_factory import (
    lake_ref_uri,
    maybe_bind_location,
    resolve_iceberg_catalog,
)
from det.ingestion.iceberg_writer import (
    _chunk_to_arrow,
    _live_type_name,
    _pyiceberg_type,
    _require_iceberg,
    iceberg_schema_from_columns,
)
from det.logging import get_logger
from det.runtime.lake import LakeRef
from det.runtime.receipts import (
    _dt_keys,
    attempt_window,
    iter_receipts,
    normalize_receipt,
)
from det.runtime.sql_types import incompatible_column_error, types_compatible

logger = get_logger(__name__)

OPS_NAMESPACE = "ops"
OPS_TABLE = "run_receipts"
_ATTEMPT_DATE = "attempt_date"

# Fixed CREATE types for ops.run_receipts (not inferred from row values).
OPS_COLUMN_TYPES: list[tuple[str, str]] = [
    ("receipt_version", "INTEGER"),
    ("attempt_id", "STRING"),
    ("attempt_date", "DATE"),
    ("pipeline", "STRING"),
    ("command", "STRING"),
    ("interval_start", "STRING"),
    ("interval_end", "STRING"),
    ("extract_run_datetime", "STRING"),
    ("wire_version", "INTEGER"),
    ("status", "STRING"),
    ("started_at", "TIMESTAMPTZ"),
    ("finished_at", "TIMESTAMPTZ"),
    ("duration_ms", "BIGINT"),
    ("owner", "STRING"),
    ("destination", "STRING"),
    ("artifacts", "INTEGER"),
    ("raw_bytes", "BIGINT"),
    ("rows", "BIGINT"),
    ("schema_sha256", "STRING"),
    ("error_code", "STRING"),
    ("error_class", "STRING"),
    ("error_message", "STRING"),
]


@dataclass(frozen=True)
class MaterializeStats:
    since: date
    until: date
    days_touched: int
    rows_written: int
    table_location: str
    skipped: int = 0


def ops_run_receipts_location(lake: LakeRef) -> LakeRef:
    return lake / "ops" / OPS_TABLE


def _attempt_date_partition_spec(schema: Any) -> Any:
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    src = schema.find_field(_ATTEMPT_DATE)
    return PartitionSpec(
        PartitionField(
            source_id=src.field_id,
            field_id=1000,
            transform=IdentityTransform(),
            name=_ATTEMPT_DATE,
        )
    )


def ensure_ops_run_receipts_table(*, catalog: Any, location: str) -> Any:
    """Create or evolve ``ops.run_receipts`` partitioned by ``attempt_date``."""
    from pyiceberg.exceptions import NoSuchTableError

    identifier = (OPS_NAMESPACE, OPS_TABLE)
    schema = iceberg_schema_from_columns(OPS_COLUMN_TYPES)
    maybe_bind_location(catalog, identifier, location)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        catalog.create_namespace(OPS_NAMESPACE)
        return catalog.create_table(
            identifier,
            schema=schema,
            location=location,
            partition_spec=_attempt_date_partition_spec(schema),
        )

    live = {field.name: _live_type_name(field.field_type) for field in table.schema().fields}
    to_add: list[tuple[str, str]] = []
    for name, expected in OPS_COLUMN_TYPES:
        live_type = live.get(name)
        if live_type is None:
            to_add.append((name, expected))
            continue
        if not types_compatible(live_type, expected):
            raise incompatible_column_error(
                sql_schema=OPS_NAMESPACE,
                table=OPS_TABLE,
                column=name,
                live_type=live_type,
                expected_type=expected,
                kind="iceberg",
            )
    if to_add:
        with table.update_schema() as update:
            for name, expected in to_add:
                update.add_column(name, _pyiceberg_type(expected))
        table = catalog.load_table(identifier)
    return table


def _day_filter(day: date) -> Any:
    from pyiceberg.expressions import EqualTo

    return EqualTo(_ATTEMPT_DATE, day)  # type: ignore[call-arg]


def _live_attempt_dates(ice_table: Any) -> set[date]:
    from det.ingestion.iceberg_writer import _live_arrow

    arrow = _live_arrow(ice_table)
    if arrow.num_rows == 0 or _ATTEMPT_DATE not in arrow.column_names:
        return set()
    out: set[date] = set()
    for value in arrow.column(_ATTEMPT_DATE).to_pylist():
        if value is None:
            continue
        if isinstance(value, datetime):
            out.add(value.date())
        elif isinstance(value, date):
            out.add(value)
        else:
            out.add(date.fromisoformat(str(value)[:10]))
    return out


def materialize_receipts(
    lake: LakeRef,
    *,
    since: str | date | datetime | None = None,
    until: str | date | datetime | None = None,
    now: datetime | None = None,
) -> MaterializeStats:
    """
    Replace Iceberg partitions for each attempt-date in ``[since, until)``.

    Source of truth is ``{lake}/runs/`` JSON. Empty days delete the partition.
    """
    _require_iceberg()
    start, end = attempt_window(since, until, now=now)
    by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    skipped = 0
    for raw in iter_receipts(lake, since=start, until=end, now=now):
        body = {k: v for k, v in raw.items() if k != "path"}
        row = normalize_receipt(body)
        if row is None:
            skipped += 1
            continue
        day_raw = row.get("attempt_date")
        if not isinstance(day_raw, date):
            skipped += 1
            continue
        day = day_raw
        by_day[day].append(cast(dict[str, Any], row))

    table_location = ops_run_receipts_location(lake)
    location = lake_ref_uri(table_location)
    catalog = resolve_iceberg_catalog(lake)
    ice_table = ensure_ops_run_receipts_table(catalog=catalog, location=location)
    pa_schema = ice_table.schema().as_arrow()
    live_days = _live_attempt_dates(ice_table)

    days = [date.fromisoformat(key) for key in _dt_keys(start, end)]
    rows_written = 0
    days_touched = 0
    for day in days:
        rows = by_day.get(day, [])
        days_touched += 1
        if not rows and day not in live_days:
            continue
        txn = ice_table.transaction()
        if day in live_days:
            txn.delete(delete_filter=_day_filter(day))
        if rows:
            rows.sort(key=lambda r: str(r.get("attempt_id") or ""))
            arrow = _chunk_to_arrow(rows, OPS_COLUMN_TYPES, pa_schema)
            txn.append(arrow)
            rows_written += len(rows)
        txn.commit_transaction()
        ice_table = catalog.load_table((OPS_NAMESPACE, OPS_TABLE))
        live_days = _live_attempt_dates(ice_table)

    logger.info(
        "ops run_receipts materialize finished",
        location=location,
        since=start.isoformat(),
        until=end.isoformat(),
        days=days_touched,
        rows=rows_written,
        skipped=skipped,
    )
    return MaterializeStats(
        since=start,
        until=end,
        days_touched=days_touched,
        rows_written=rows_written,
        table_location=str(table_location),
        skipped=skipped,
    )


def scan_ops_run_receipts(
    lake: LakeRef,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Test helper: read live ops.run_receipts rows."""
    from det.ingestion.iceberg_writer import _jsonable_cell, _live_arrow, load_iceberg_table

    ice = load_iceberg_table(
        lake=lake,
        namespace=OPS_NAMESPACE,
        table=OPS_TABLE,
        table_location=ops_run_receipts_location(lake),
    )
    if ice is None:
        return []
    rows = _live_arrow(ice).to_pylist()
    out = [{k: _jsonable_cell(v) for k, v in row.items()} for row in rows]
    out.sort(
        key=lambda r: (str(r.get("attempt_date") or ""), str(r.get("attempt_id") or ""))
    )
    return out[:limit]
