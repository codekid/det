from __future__ import annotations

from typing import Any, Literal

from det.scaffold.flatten import is_array_prop, is_object_prop

Dialect = Literal["duckdb", "postgres"]

# DET runtime meta — not JSON Schema properties. Order matches attach_meta.
_META_ORDER = (
    "__row_hash",
    "__filename",
    "__extract_run_datetime",
    "__bronze_loaded_at",
    "__interval_start_datetime",
    "__interval_end_datetime",
    "__data_interval_date",
)

META_SQL_TYPES: dict[Dialect, dict[str, str]] = {
    "duckdb": {
        "__row_hash": "VARCHAR",
        "__filename": "VARCHAR",
        "__extract_run_datetime": "TIMESTAMP",
        "__bronze_loaded_at": "TIMESTAMP",
        "__interval_start_datetime": "TIMESTAMP",
        "__interval_end_datetime": "TIMESTAMP",
        "__data_interval_date": "DATE",
    },
    "postgres": {
        "__row_hash": "TEXT",
        "__filename": "TEXT",
        "__extract_run_datetime": "TIMESTAMPTZ",
        "__bronze_loaded_at": "TIMESTAMPTZ",
        "__interval_start_datetime": "TIMESTAMPTZ",
        "__interval_end_datetime": "TIMESTAMPTZ",
        "__data_interval_date": "DATE",
    },
}

# Scaffold / read_json uses the DuckDB dialect.
META_SQL_TYPES_DUCKDB = META_SQL_TYPES["duckdb"]

# Iceberg / PyArrow bronze — nested objects/arrays land as JSON strings (STRING),
# matching SQL JSON/JSONB. Meta timestamps are timestamptz.
ICEBERG_META_TYPES: dict[str, str] = {
    "__row_hash": "STRING",
    "__filename": "STRING",
    "__extract_run_datetime": "TIMESTAMPTZ",
    "__bronze_loaded_at": "TIMESTAMPTZ",
    "__interval_start_datetime": "TIMESTAMPTZ",
    "__interval_end_datetime": "TIMESTAMPTZ",
    "__data_interval_date": "DATE",
}

_TYPE_ALIASES = {
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP TZ": "TIMESTAMPTZ",
    "TIMESTAMPNTZ": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMPTZ",
    "CHARACTER VARYING": "VARCHAR",
    "DOUBLE PRECISION": "DOUBLE",
    "INT": "INTEGER",
    "INT4": "INTEGER",
    "INT8": "BIGINT",
    "INT2": "SMALLINT",
    "INT32": "INTEGER",
    "INT64": "BIGINT",
    "BOOL": "BOOLEAN",
    "FLOAT8": "DOUBLE",
    "FLOAT4": "REAL",
    "FLOAT": "DOUBLE",
    "FLOAT64": "DOUBLE",
    "FLOAT32": "REAL",
    "STRING": "VARCHAR",
    "UTF8": "VARCHAR",
    "LONG": "BIGINT",
    "BPCHAR": "CHAR",
    "CHARACTER": "CHAR",
    "BINARY": "BYTES",
}

_COMPAT_GROUPS = (
    frozenset({"INTEGER", "BIGINT", "SMALLINT"}),
    frozenset({"DOUBLE", "REAL"}),
    frozenset({"VARCHAR", "TEXT", "CHAR"}),
    frozenset({"JSON", "JSONB"}),
    frozenset({"TIMESTAMP", "TIMESTAMPTZ"}),
    frozenset({"DATE"}),
    frozenset({"BOOLEAN"}),
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _allowed_types(prop: dict[str, Any]) -> set[str]:
    raw = prop.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {t for t in raw if isinstance(t, str)}
    return set()


def sql_type_for_prop(prop: dict[str, Any], dialect: Dialect) -> str:
    """Map a JSON Schema property to a dialect SQL type. No $ref / allOf."""
    if is_object_prop(prop) or is_array_prop(prop):
        return "JSON" if dialect == "duckdb" else "JSONB"
    allowed = _allowed_types(prop)
    if "integer" in allowed:
        return "BIGINT"
    if "number" in allowed:
        return "DOUBLE" if dialect == "duckdb" else "DOUBLE PRECISION"
    if "boolean" in allowed:
        return "BOOLEAN"
    if dialect == "duckdb":
        return "VARCHAR"
    return "TEXT"


def duckdb_type_for_prop(prop: dict[str, Any]) -> str:
    """Map a JSON Schema property to a DuckDB read_json / CREATE TABLE type."""
    return sql_type_for_prop(prop, "duckdb")


def iceberg_type_for_prop(prop: dict[str, Any]) -> str:
    """Map a JSON Schema property to an Iceberg type name. Nested → STRING (JSON)."""
    if is_object_prop(prop) or is_array_prop(prop):
        return "STRING"
    allowed = _allowed_types(prop)
    if "integer" in allowed:
        return "BIGINT"
    if "number" in allowed:
        return "DOUBLE"
    if "boolean" in allowed:
        return "BOOLEAN"
    return "STRING"


def bronze_iceberg_columns(schema: dict[str, Any]) -> list[tuple[str, str]]:
    """CREATE/append columns for Iceberg bronze: schema properties then DET meta."""
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, prop in props.items():
        if not isinstance(name, str):
            continue
        iceberg_type = (
            iceberg_type_for_prop(prop) if isinstance(prop, dict) else "STRING"
        )
        out.append((name, iceberg_type))
        seen.add(name)
    for name in _META_ORDER:
        if name in seen:
            continue
        out.append((name, ICEBERG_META_TYPES[name]))
    return out


def bronze_sql_columns(
    schema: dict[str, Any], dialect: Dialect
) -> list[tuple[str, str]]:
    """
    CREATE/INSERT columns: JSON Schema properties (file order) then DET meta.

    Types come only from the schema. Row values are never inspected.
    """
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        props = {}
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, prop in props.items():
        if not isinstance(name, str):
            continue
        sql_type = (
            sql_type_for_prop(prop, dialect)
            if isinstance(prop, dict)
            else ("VARCHAR" if dialect == "duckdb" else "TEXT")
        )
        out.append((name, sql_type))
        seen.add(name)
    meta = META_SQL_TYPES[dialect]
    for name in _META_ORDER:
        if name in seen:
            continue
        out.append((name, meta[name]))
    return out


def normalize_sql_type(sql_type: str) -> str:
    text = " ".join(sql_type.upper().replace("_", " ").split())
    return _TYPE_ALIASES.get(text, text)


def types_compatible(live: str, expected: str) -> bool:
    """True when live and expected are the same type family (INTEGER≡BIGINT)."""
    a, b = normalize_sql_type(live), normalize_sql_type(expected)
    if a == b:
        return True
    for group in _COMPAT_GROUPS:
        if a in group and b in group:
            return True
    return False


def incompatible_column_error(
    *,
    sql_schema: str,
    table: str,
    column: str,
    live_type: str,
    expected_type: str,
    kind: str = "SQL",
) -> ValueError:
    if kind == "iceberg":
        return ValueError(
            f"bronze Iceberg table {sql_schema}.{table} column {column} has type "
            f"{live_type}, expected {expected_type}. Drop the bronze Iceberg table "
            f"(not raw) and reload."
        )
    return ValueError(
        f"bronze SQL table {sql_schema}.{table} column {column} has type "
        f"{live_type}, expected {expected_type}. Drop the bronze SQL table "
        f"(not raw) and reload."
    )
