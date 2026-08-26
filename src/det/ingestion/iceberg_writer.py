"""DET-owned Iceberg+Parquet bronze writer (PyIceberg + PyArrow).

Not dlt.pipeline, not Spark. Hadoop-style catalog on the lake root; table files
live at ``{lake}/bronze/{provider}/{source}_vN/`` (Iceberg owns data-file names).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

from det.ingestion.chunks import iter_chunks
from det.ingestion.sql_replace import (
    assert_chunk_matches_identity,
    resolve_run_identity,
)
from det.logging import get_logger
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
_ICEBERG_HINT = "pip install 'det[iceberg]'"


def _require_iceberg() -> None:
    try:
        import pyarrow  # noqa: F401
        import pyiceberg  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            f"Iceberg bronze requires the optional extra: {_ICEBERG_HINT}"
        ) from exc


def lake_ref_uri(ref: LakeRef) -> str:
    """URI PyIceberg FileIO accepts (file://, s3://, gs://)."""
    if ref.is_local:
        return ref.to_path().resolve().as_uri()
    text = str(ref)
    if text.startswith("gcs://"):
        return "gs://" + text[len("gcs://") :]
    return text


def hadoop_catalog(lake: LakeRef, *, env: Mapping[str, str] | None = None):
    _require_iceberg()
    from det.ingestion.iceberg_catalog import LakeHadoopCatalog
    from det.runtime.object_store import iceberg_gcs_properties, iceberg_s3_properties

    warehouse = lake_ref_uri(lake)
    props: dict[str, str] = {"warehouse": warehouse}
    if warehouse.startswith("s3://"):
        props.update(iceberg_s3_properties(env))
    elif warehouse.startswith("gs://"):
        props.update(iceberg_gcs_properties(env))
    return LakeHadoopCatalog("det", **props)


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

    ``extract_run`` — identity on ``__extract_run_datetime`` only (ETL prune).
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
    src = schema.find_field(_RUN)
    return PartitionSpec(
        PartitionField(
            source_id=src.field_id,
            field_id=1000,
            transform=IdentityTransform(),
            name=_RUN,
        )
    )


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
    return f"identity({_RUN})"


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
    catalog = hadoop_catalog(lake)
    ident = (namespace, table)
    location = lake_ref_uri(table_location)
    catalog.bind_location(ident, location)
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
) -> Any:
    from pyiceberg.exceptions import NoSuchTableError

    schema = iceberg_schema_from_columns(columns)
    catalog.bind_location(identifier, location)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        catalog.create_namespace(identifier[0])
        return catalog.create_table(
            identifier,
            schema=schema,
            location=location,
            partition_spec=partition_spec_for(partition, schema),
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
    catalog = hadoop_catalog(lake)
    ident = (namespace, table)
    location = lake_ref_uri(table_location)
    catalog.bind_location(ident, location)
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
    run_identity: tuple[str, str, str] | None = None,
) -> LakeRef:
    """
    Replace-by-extract-run DET bronze write into an Iceberg table.

    Deletes rows matching the three run-identity columns, then appends Parquet
    in one transaction. ``partition`` applies on create only; existing tables
    must match YAML or ``ensure_iceberg_table`` raises (use migrate
    ``--recreate-iceberg`` to purge and recreate).

    ``run_identity`` is required for empty streams so replace-by-run still runs.
    """
    _require_iceberg()
    chunks = iter_chunks(records, chunk_rows)
    first_chunk = next(chunks, None)
    identity = resolve_run_identity(run_identity, first_chunk)
    col_types = bronze_iceberg_columns(json_schema)
    catalog = hadoop_catalog(lake)
    ident = (namespace, table)
    location = lake_ref_uri(table_location)
    ice_table = ensure_iceberg_table(
        catalog=catalog,
        identifier=ident,
        location=location,
        columns=col_types,
        partition=partition,
    )
    pa_schema = ice_table.schema().as_arrow()
    filt = _run_filter(identity)

    def _arrow(chunk: list[dict[str, Any]]) -> Any:
        assert_chunk_matches_identity(chunk, identity)
        return _chunk_to_arrow(chunk, col_types, pa_schema)

    total = 0
    live_runs = set(list_iceberg_extract_runs(ice_table))
    identity_key = tuple(identity_iso(part) for part in identity)
    if first_chunk is None:
        if identity_key in live_runs:
            txn = ice_table.transaction()
            txn.delete(delete_filter=filt)
            txn.commit_transaction()
        logger.info(
            "iceberg load finished",
            table=f"{namespace}.{table}",
            location=location,
            rows=0,
            partition=partition,
        )
        return table_location

    txn = ice_table.transaction()
    if identity_key in live_runs:
        txn.delete(delete_filter=filt)
    first_arrow = _arrow(first_chunk)
    txn.append(first_arrow)
    total += len(first_chunk)
    for chunk in chunks:
        txn.append(_arrow(chunk))
        total += len(chunk)
    txn.commit_transaction()
    logger.info(
        "iceberg load finished",
        table=f"{namespace}.{table}",
        location=location,
        rows=total,
        partition=partition,
    )
    return table_location


def _live_arrow(ice_table: Any):
    """Read live snapshot parquet without PyArrow dataset ``__filename`` collision."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    target = ice_table.schema().as_arrow()
    tables = []
    for task in ice_table.scan().plan_files():
        with ice_table.io.new_input(task.file.file_path).open() as fh:
            raw = pq.ParquetFile(fh).read()
        cols = []
        for field in target:
            if field.name in raw.column_names:
                cols.append(raw.column(field.name).cast(field.type))
            else:
                cols.append(pa.nulls(raw.num_rows, type=field.type))
        tables.append(pa.Table.from_arrays(cols, schema=target))
    if not tables:
        names = [f.name for f in ice_table.schema().fields]
        return pa.table({n: [] for n in names})
    return pa.concat_tables(tables)


def list_iceberg_extract_runs(
    ice_table: Any,
    *,
    window_start: str | None = None,
    window_end: str | None = None,
    limit: int | None = None,
) -> list[tuple[str, str, str]]:
    cols = (_START, _END, _RUN)
    arrow = _live_arrow(ice_table)
    seen: set[tuple[str, str, str]] = set()
    out: list[tuple[str, str, str]] = []
    as_dict = arrow.to_pydict()
    n = arrow.num_rows
    for i in range(n):
        ident = (
            identity_iso(as_dict[cols[0]][i]),
            identity_iso(as_dict[cols[1]][i]),
            identity_iso(as_dict[cols[2]][i]),
        )
        if ident in seen:
            continue
        if window_start is not None:
            end = window_end if window_end is not None else ident[0] + "\uffff"
            if not (window_start <= ident[0] < end):
                continue
        seen.add(ident)
        out.append(ident)
    out.sort()
    if limit is not None:
        return out[:limit]
    return out


def delete_iceberg_extract_run(ice_table: Any, identity: tuple[str, str, str]) -> None:
    ice_table.delete(delete_filter=_run_filter(identity))


def scan_iceberg_rows(
    ice_table: Any,
    *,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
) -> list[dict[str, Any]]:
    from det.runtime.meta import resolve_interval, to_interval_datetime

    arrow = _live_arrow(ice_table)
    rows = arrow.to_pylist()
    window: tuple[str, str] | None = None
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)
    want_run = (
        to_interval_datetime(extract_run_datetime) if extract_run_datetime else None
    )
    matched: list[dict[str, Any]] = []
    for row in rows:
        start = identity_iso(row.get(_START))
        if window is not None and not (window[0] <= start < window[1]):
            continue
        if want_run is not None and identity_iso(row.get(_RUN)) != want_run:
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
