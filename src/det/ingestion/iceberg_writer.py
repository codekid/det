"""DET-owned Iceberg+Parquet bronze writer (PyIceberg + PyArrow).

Not dlt.pipeline, not Spark. Catalog is env-selected (hadoop default; rest/glue
via ``DET_ICEBERG_CATALOG``). Table files live at
``{lake}/bronze/{provider}/{source}_vN/`` (Iceberg owns data-file names).
"""

from __future__ import annotations

import json
import warnings
from collections.abc import Callable, Iterable
from datetime import date, datetime
from typing import Any

from det.ingestion.chunks import iter_chunks
from det.ingestion.iceberg_catalog_factory import (
    ensure_iceberg_namespace,
    hadoop_catalog,
    lake_ref_uri,
    maybe_bind_location,
    resolve_iceberg_catalog,
)
from det.ingestion.sql_replace import (
    assert_chunk_matches_identity,
    resolve_run_identity,
)
from det.logging import get_logger
from det.optional_deps import pip_extra_hint
from det.runtime.config import IcebergPartition
from det.runtime.lake import LakeRef
from det.runtime.meta import identity_iso
from det.runtime.sql_types import (
    bronze_iceberg_columns,
    incompatible_column_error,
    normalize_sql_type,
    types_compatible,
)

logger = get_logger(__name__)

_START = "__interval_start_datetime"
_END = "__interval_end_datetime"
_RUN = "__extract_run_datetime"
_ICEBERG_HINT = pip_extra_hint("iceberg")

# Re-export for callers that imported these from iceberg_writer.
__all__ = [
    "hadoop_catalog",
    "lake_ref_uri",
    "load_iceberg_table",
    "purge_iceberg_table",
    "resolve_iceberg_catalog",
    "write_iceberg_table",
]


def _require_iceberg() -> None:
    try:
        import pyarrow  # noqa: F401
        import pyiceberg  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            f"Iceberg bronze requires the optional extra: {_ICEBERG_HINT}"
        ) from exc


def _pyiceberg_type(type_name: str):
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
    )

    mapped = normalize_sql_type(type_name)
    if mapped in {"INTEGER", "SMALLINT"}:
        return IntegerType()
    if mapped == "BIGINT":
        return LongType()
    if mapped in {"DOUBLE", "REAL"}:
        return DoubleType()
    if mapped == "BOOLEAN":
        return BooleanType()
    if mapped == "DATE":
        return DateType()
    if mapped == "TIMESTAMPTZ":
        return TimestamptzType()
    if mapped == "TIMESTAMP":
        return TimestampType()
    return StringType()


def _live_type_name(field_type: object) -> str:
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
    )

    if isinstance(field_type, IntegerType):
        return "INTEGER"
    if isinstance(field_type, LongType):
        return "BIGINT"
    if isinstance(field_type, DoubleType):
        return "DOUBLE"
    if isinstance(field_type, FloatType):
        return "REAL"
    if isinstance(field_type, BooleanType):
        return "BOOLEAN"
    if isinstance(field_type, DateType):
        return "DATE"
    if isinstance(field_type, TimestamptzType):
        return "TIMESTAMPTZ"
    if isinstance(field_type, TimestampType):
        return "TIMESTAMP"
    if isinstance(field_type, StringType):
        return "STRING"
    return str(field_type).upper()


def iceberg_schema_from_columns(columns: list[tuple[str, str]]):
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    fields = [
        NestedField(i, name, _pyiceberg_type(typ), required=False)
        for i, (name, typ) in enumerate(columns, start=1)
    ]
    return Schema(*fields)


def partition_spec_for(mode: IcebergPartition, schema):
    """Build Iceberg PartitionSpec for YAML ``destination.partition``.

    ``extract_run`` — identity on interval start, interval end, then extract run
    (same grain as raw hive; inspect/prune keys).
    ``none`` — unpartitioned.
    """
    from pyiceberg.partitioning import (
        UNPARTITIONED_PARTITION_SPEC,
        PartitionField,
        PartitionSpec,
    )
    from pyiceberg.transforms import IdentityTransform

    if mode == "none":
        return UNPARTITIONED_PARTITION_SPEC
    fields = []
    for i, col in enumerate((_START, _END, _RUN), start=1000):
        src = schema.find_field(col)
        fields.append(
            PartitionField(
                source_id=src.field_id,
                field_id=i,
                transform=IdentityTransform(),
                name=col,
            )
        )
    return PartitionSpec(*fields)


def _live_partition_summary(table: Any) -> str:
    """Human-readable live partition fields for mismatch warnings."""
    fields = list(table.spec().fields)
    if not fields:
        return "none"
    parts = []
    for pf in fields:
        src = table.schema().find_field(pf.source_id)
        name = src.name if src is not None else str(pf.source_id)
        parts.append(f"{pf.transform}({name})")
    return ",".join(parts)


def _expected_partition_summary(mode: IcebergPartition) -> str:
    if mode == "none":
        return "none"
    return ",".join(f"identity({c})" for c in (_START, _END, _RUN))


def purge_iceberg_table(
    *,
    lake: LakeRef,
    table_location: LakeRef,
    namespace: str,
    table: str,
) -> None:
    """Remove Iceberg catalog hint and delete the table location tree.

    Idempotent when the table/hint is already absent. Hint-only drop is not
    enough — orphan metadata/data would block a clean recreate.
    """
    from pyiceberg.exceptions import NoSuchTableError

    _require_iceberg()
    catalog = resolve_iceberg_catalog(lake)
    ident = (namespace, table)
    location = lake_ref_uri(table_location)
    maybe_bind_location(catalog, ident, location)
    try:
        catalog.drop_table(ident)
    except NoSuchTableError:
        pass
    if table_location.exists():
        table_location.rmtree(ignore_errors=True)
    logger.info(
        "purged iceberg table",
        table=f"{namespace}.{table}",
        location=location,
    )


def _as_utc_datetime(value: Any) -> datetime | None:
    import pendulum
    from pendulum import DateTime

    if value is None:
        return None
    if isinstance(value, DateTime):
        return value.in_timezone("UTC")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return pendulum.instance(value, tz="UTC")
        return pendulum.instance(value).in_timezone("UTC")
    parsed = pendulum.parse(str(value))
    if not isinstance(parsed, DateTime):
        raise ValueError(f"not a timestamp: {value!r}")
    return parsed.in_timezone("UTC")


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _cell(value: Any, iceberg_type: str) -> Any:
    if value is None:
        return None
    mapped = normalize_sql_type(iceberg_type)
    if mapped in {"INTEGER", "SMALLINT", "BIGINT"}:
        return int(value)
    if mapped in {"DOUBLE", "REAL"}:
        return float(value)
    if mapped == "BOOLEAN":
        return bool(value)
    if mapped == "DATE":
        return _as_date(value)
    if mapped in {"TIMESTAMP", "TIMESTAMPTZ"}:
        return _as_utc_datetime(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def _chunk_to_arrow(
    chunk: list[dict[str, Any]],
    columns: list[tuple[str, str]],
    pa_schema: Any,
) -> Any:
    import pyarrow as pa

    arrays = []
    by_name = dict(columns)
    for field in pa_schema:
        iceberg_type = by_name.get(field.name, "STRING")
        values = [_cell(row.get(field.name), iceberg_type) for row in chunk]
        arrays.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(arrays, schema=pa_schema)


def _run_filter(identity: tuple[str, str, str]):
    from pyiceberg.expressions import And, EqualTo

    start, end, run = identity
    # Stubs for EqualTo/And are incomplete across pyiceberg versions.
    return And(  # type: ignore[call-arg]
        EqualTo(_START, _as_utc_datetime(start)),  # type: ignore[call-arg]
        And(  # type: ignore[call-arg]
            EqualTo(_END, _as_utc_datetime(end)),  # type: ignore[call-arg]
            EqualTo(_RUN, _as_utc_datetime(run)),  # type: ignore[call-arg]
        ),
    )


def ensure_iceberg_table(
    *,
    catalog: Any,
    identifier: tuple[str, str],
    location: str,
    columns: list[tuple[str, str]],
    partition: IcebergPartition = "extract_run",
    table_properties: dict[str, str] | None = None,
) -> Any:
    from pyiceberg.exceptions import NoSuchTableError

    schema = iceberg_schema_from_columns(columns)
    maybe_bind_location(catalog, identifier, location)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        ensure_iceberg_namespace(
            catalog, identifier[0], table_location=location
        )
        props = dict(table_properties or {})
        return catalog.create_table(
            identifier,
            schema=schema,
            location=location,
            partition_spec=partition_spec_for(partition, schema),
            properties=props,
        )

    live_summary = _live_partition_summary(table)
    expected_summary = _expected_partition_summary(partition)
    if live_summary != expected_summary:
        raise ValueError(
            f"iceberg partition YAML ({partition!r} → {expected_summary}) does not "
            f"match live table {identifier[0]}.{identifier[1]} ({live_summary}). "
            "Refuse to load/migrate onto the wrong shape. Fix with "
            "`det migrate … --recreate-iceberg` (purges the bronze table then "
            "rebuilds from raw in -s/-e) or wipe the table location so the next "
            "write creates with the YAML profile."
        )

    live = {field.name: _live_type_name(field.field_type) for field in table.schema().fields}
    to_add: list[tuple[str, str]] = []
    for name, expected in columns:
        live_type = live.get(name)
        if live_type is None:
            to_add.append((name, expected))
            continue
        if not types_compatible(live_type, expected):
            raise incompatible_column_error(
                sql_schema=identifier[0],
                table=identifier[1],
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


def load_iceberg_table(
    *,
    lake: LakeRef,
    namespace: str,
    table: str,
    table_location: LakeRef,
) -> Any | None:
    from pyiceberg.exceptions import NoSuchTableError

    _require_iceberg()
    catalog = resolve_iceberg_catalog(lake)
    ident = (namespace, table)
    location = lake_ref_uri(table_location)
    maybe_bind_location(catalog, ident, location)
    try:
        return catalog.load_table(ident)
    except NoSuchTableError:
        return None


def write_iceberg_table(
    records: Iterable[dict[str, Any]],
    *,
    lake: LakeRef,
    table_location: LakeRef,
    namespace: str,
    table: str,
    json_schema: dict[str, Any],
    chunk_rows: int = 10_000,
    partition: IcebergPartition = "extract_run",
    table_properties: dict[str, str] | None = None,
    run_identity: tuple[str, str, str] | None = None,
    on_chunk: Callable[[], None] | None = None,
) -> LakeRef:
    """
    Replace-by-extract-run DET bronze write into an Iceberg table.

    Unconditionally deletes rows matching the three run-identity columns
    (predicate may match zero rows), then appends Parquet in one transaction.
    Does not scan the live table to decide whether to delete.

    ``partition`` applies on create only; existing tables must match YAML or
    ``ensure_iceberg_table`` raises (use migrate ``--recreate-iceberg`` to purge
    and recreate). ``table_properties`` apply on create only; live property
    reconcile is an external maintain runner (Airflow/Spark).

    ``run_identity`` is required for empty streams so replace-by-run still runs.
    """
    _require_iceberg()
    chunks = iter_chunks(records, chunk_rows)
    first_chunk = next(chunks, None)
    identity = resolve_run_identity(run_identity, first_chunk)
    col_types = bronze_iceberg_columns(json_schema)
    catalog = resolve_iceberg_catalog(lake)
    ident = (namespace, table)
    location = lake_ref_uri(table_location)
    ice_table = ensure_iceberg_table(
        catalog=catalog,
        identifier=ident,
        location=location,
        columns=col_types,
        partition=partition,
        table_properties=table_properties,
    )
    pa_schema = ice_table.schema().as_arrow()
    filt = _run_filter(identity)

    def _arrow(chunk: list[dict[str, Any]]) -> Any:
        assert_chunk_matches_identity(chunk, identity)
        return _chunk_to_arrow(chunk, col_types, pa_schema)

    txn = ice_table.transaction()
    # Unconditional replace-by-run: zero matches is fine (first write / empty table).
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Delete operation did not match any records",
            category=UserWarning,
        )
        txn.delete(delete_filter=filt)
    total = 0
    if first_chunk is not None:
        txn.append(_arrow(first_chunk))
        total = len(first_chunk)
        if on_chunk is not None:
            on_chunk()
        for chunk in chunks:
            txn.append(_arrow(chunk))
            total += len(chunk)
            if on_chunk is not None:
                on_chunk()
    txn.commit_transaction()
    logger.info(
        "iceberg load finished",
        table=f"{namespace}.{table}",
        location=location,
        rows=total,
        partition=partition,
    )
    return table_location


def _partition_value_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return identity_iso(value)
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return identity_iso(text) if text else None


def _identity_from_partition_struct(partition: Any) -> tuple[str, str, str] | None:
    """Map Iceberg partition struct / dict to DET run identity."""
    if partition is None:
        return None
    if hasattr(partition, "as_py"):
        partition = partition.as_py()
    if isinstance(partition, dict):
        start = _partition_value_iso(partition.get(_START))
        end = _partition_value_iso(partition.get(_END))
        run = _partition_value_iso(partition.get(_RUN))
    else:
        # Named tuple / Record-like: attribute access by field name.
        start = _partition_value_iso(getattr(partition, _START, None))
        end = _partition_value_iso(getattr(partition, _END, None))
        run = _partition_value_iso(getattr(partition, _RUN, None))
    if start is None or end is None or run is None:
        return None
    return (start, end, run)


def _identities_from_partitions_table(ice_table: Any) -> list[tuple[str, str, str]]:
    """List run identities from ``inspect.partitions()`` (metadata only)."""
    arrow = ice_table.inspect.partitions()
    if arrow.num_rows == 0 or "partition" not in arrow.column_names:
        return []
    seen: set[tuple[str, str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for partition in arrow.column("partition").to_pylist():
        ident = _identity_from_partition_struct(partition)
        if ident is None or ident in seen:
            continue
        seen.add(ident)
        out.append(ident)
    return out


def _identities_from_file_bounds(ice_table: Any) -> list[tuple[str, str, str]]:
    """Unpartitioned fallback: distinct identities from file column bounds.

    DET writes one identity per file, so lower_bound == upper_bound for the
    three identity columns. Mixed-bound files (compaction) are skipped here;
    callers that need those rows should use a bounded scan.
    """
    arrow = ice_table.inspect.files()
    if arrow.num_rows == 0 or "readable_metrics" not in arrow.column_names:
        return []
    seen: set[tuple[str, str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for metrics in arrow.column("readable_metrics").to_pylist():
        if not isinstance(metrics, dict):
            continue
        vals: list[str] = []
        mixed = False
        for col in (_START, _END, _RUN):
            cell = metrics.get(col) or {}
            lo = _partition_value_iso(cell.get("lower_bound"))
            hi = _partition_value_iso(cell.get("upper_bound"))
            if lo is None or hi is None or lo != hi:
                mixed = True
                break
            vals.append(lo)
        if mixed:
            continue
        ident = (vals[0], vals[1], vals[2])
        if ident in seen:
            continue
        seen.add(ident)
        out.append(ident)
    return out


def list_iceberg_extract_runs(
    ice_table: Any,
    *,
    window_start: str | None = None,
    window_end: str | None = None,
    extract_run_since: str | None = None,
    limit: int | None = None,
) -> list[tuple[str, str, str]]:
    """List distinct extract-run identities from Iceberg metadata (not Parquet)."""
    if ice_table.metadata.current_snapshot() is None:
        return []
    if list(ice_table.spec().fields):
        candidates = _identities_from_partitions_table(ice_table)
    else:
        candidates = _identities_from_file_bounds(ice_table)

    since = identity_iso(extract_run_since) if extract_run_since else None
    out: list[tuple[str, str, str]] = []
    for ident in candidates:
        if window_start is not None:
            end = window_end if window_end is not None else ident[0] + "\uffff"
            if not (window_start <= ident[0] < end):
                continue
        if since is not None and ident[2] < since:
            continue
        out.append(ident)
    out.sort()
    if limit is not None:
        return out[:limit]
    return out


def delete_iceberg_extract_run(ice_table: Any, identity: tuple[str, str, str]) -> None:
    ice_table.delete(delete_filter=_run_filter(identity))


def _scan_row_filter(
    *,
    interval_start: str | None,
    interval_end: str | None,
    extract_run_datetime: str | None,
    extract_run_since: str | None = None,
) -> Any:
    """Build a PyIceberg BooleanExpression, or None for an unfiltered scan."""
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan

    from det.runtime.meta import resolve_interval, to_interval_datetime

    parts: list[Any] = []
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)
        parts.append(GreaterThanOrEqual(_START, _as_utc_datetime(window[0])))  # type: ignore[call-arg]
        parts.append(LessThan(_START, _as_utc_datetime(window[1])))  # type: ignore[call-arg]
    if extract_run_datetime is not None:
        want = to_interval_datetime(extract_run_datetime)
        parts.append(EqualTo(_RUN, _as_utc_datetime(want)))  # type: ignore[call-arg]
    elif extract_run_since is not None:
        parts.append(
            GreaterThanOrEqual(_RUN, _as_utc_datetime(extract_run_since))  # type: ignore[call-arg]
        )
    if not parts:
        return None
    filt: Any = parts[0]
    for part in parts[1:]:
        filt = And(filt, part)  # type: ignore[call-arg]
    return filt


def _read_planned_parquet(
    ice_table: Any,
    *,
    row_filter: Any = None,
    limit: int | None = None,
) -> Any:
    """Read planned parquet files without PyArrow dataset ``__filename`` collision.

    Stops once ``limit`` rows are collected when set.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    target = ice_table.schema().as_arrow()
    scan = ice_table.scan(row_filter=row_filter) if row_filter is not None else ice_table.scan()
    tables: list[Any] = []
    total = 0
    for task in scan.plan_files():
        with ice_table.io.new_input(task.file.file_path).open() as fh:
            raw = pq.ParquetFile(fh).read()
        cols = []
        for field in target:
            if field.name in raw.column_names:
                cols.append(raw.column(field.name).cast(field.type))
            else:
                cols.append(pa.nulls(raw.num_rows, type=field.type))
        piece = pa.Table.from_arrays(cols, schema=target)
        if limit is not None:
            remaining = limit - total
            if remaining <= 0:
                break
            if piece.num_rows > remaining:
                piece = piece.slice(0, remaining)
        tables.append(piece)
        total += piece.num_rows
        if limit is not None and total >= limit:
            break
    if not tables:
        names = [f.name for f in ice_table.schema().fields]
        return pa.table({n: [] for n in names})
    return pa.concat_tables(tables)


def scan_iceberg_rows(
    ice_table: Any,
    *,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    extract_run_since: str | None = None,
) -> list[dict[str, Any]]:
    """Bounded row sample: push filters into Iceberg scan, stop at ``limit``."""
    from det.runtime.meta import resolve_interval, to_interval_datetime

    row_filter = _scan_row_filter(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        extract_run_since=extract_run_since,
    )
    # Filtered: hard-stop while reading. Unfiltered (soaks): read planned files,
    # then sort + slice — tables are tiny in tests.
    arrow = _read_planned_parquet(
        ice_table,
        row_filter=row_filter,
        limit=limit if row_filter is not None else None,
    )
    rows = arrow.to_pylist()
    window: tuple[str, str] | None = None
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)
    want_run = (
        to_interval_datetime(extract_run_datetime) if extract_run_datetime else None
    )
    since = identity_iso(extract_run_since) if extract_run_since else None
    matched: list[dict[str, Any]] = []
    for row in rows:
        start = identity_iso(row.get(_START))
        if window is not None and not (window[0] <= start < window[1]):
            continue
        run = identity_iso(row.get(_RUN))
        if want_run is not None and run != want_run:
            continue
        if since is not None and run < since:
            continue
        matched.append({k: _jsonable_cell(v) for k, v in row.items()})
    matched.sort(
        key=lambda r: (
            str(r.get(_RUN) or ""),
            str(r.get("__row_hash") or ""),
        )
    )
    return matched[:limit]


def _jsonable_cell(value: Any) -> Any:
    if isinstance(value, datetime):
        return identity_iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value
