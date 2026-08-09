from __future__ import annotations

import json
from typing import Any

from det.logging import get_logger

logger = get_logger(__name__)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _infer_pg_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    if isinstance(value, (dict, list)):
        return "TEXT"
    return "TEXT"


def _column_types(records: list[dict[str, Any]]) -> dict[str, str]:
    types: dict[str, str] = {}
    for row in records:
        for key, value in row.items():
            if value is None:
                types.setdefault(key, "TEXT")
                continue
            inferred = _infer_pg_type(value)
            existing = types.get(key)
            if existing is None or existing == "TEXT":
                types[key] = inferred
    return types


def _cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def write_postgres_table(
    records: list[dict[str, Any]],
    *,
    dsn: str,
    schema: str,
    table: str,
) -> str:
    """
    Append-only DET bronze write into Postgres.

    Creates schema/table if needed from the record shape, then INSERTs rows.
    Returns the DSN (connection identity for RunResult.partition_dir).
    """
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            'Postgres destination requires the optional extra: pip install -e ".[postgres]"'
        ) from exc

    schema_sql = _quote_ident(schema)
    table_sql = _quote_ident(table)
    qualified = f"{schema_sql}.{table_sql}"

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_sql}")
            if records:
                col_types = _column_types(records)
                columns = list(col_types)
                defs = ", ".join(
                    f"{_quote_ident(name)} {col_types[name]}" for name in columns
                )
                cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({defs})")
                col_list = ", ".join(_quote_ident(c) for c in columns)
                placeholders = ", ".join(["%s"] * len(columns))
                insert_sql = f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders})"
                rows = [tuple(_cell(row.get(c)) for c in columns) for row in records]
                cur.executemany(insert_sql, rows)
        conn.commit()

    logger.info(
        "postgres load finished",
        table=f"{schema}.{table}",
        rows=len(records),
    )
    return dsn
