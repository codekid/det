from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from det.logging import get_logger

logger = get_logger(__name__)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _infer_duckdb_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, (dict, list)):
        return "VARCHAR"
    return "VARCHAR"


def _column_types(records: list[dict[str, Any]]) -> dict[str, str]:
    types: dict[str, str] = {}
    for row in records:
        for key, value in row.items():
            if value is None:
                types.setdefault(key, "VARCHAR")
                continue
            inferred = _infer_duckdb_type(value)
            existing = types.get(key)
            if existing is None or existing == "VARCHAR":
                types[key] = inferred
    return types


def _cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def write_duckdb_table(
    records: list[dict[str, Any]],
    *,
    connection_path: Path,
    schema: str,
    table: str,
) -> Path:
    """
    Append-only DET bronze write into DuckDB.

    Creates schema/table if needed from the record shape (including __* meta),
    then INSERTs all rows. Does not delete prior extract runs.
    """
    connection_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = _quote_ident(schema)
    table_sql = _quote_ident(table)
    qualified = f"{schema_sql}.{table_sql}"

    con = duckdb.connect(str(connection_path))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_sql}")
        if records:
            col_types = _column_types(records)
            columns = list(col_types)
            defs = ", ".join(
                f"{_quote_ident(name)} {col_types[name]}" for name in columns
            )
            con.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({defs})")
            placeholders = ", ".join("?" for _ in columns)
            col_list = ", ".join(_quote_ident(c) for c in columns)
            insert_sql = f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders})"
            rows = [tuple(_cell(row.get(c)) for c in columns) for row in records]
            con.executemany(insert_sql, rows)
        logger.info(
            "duckdb load finished",
            path=str(connection_path),
            table=f"{schema}.{table}",
            rows=len(records),
        )
    finally:
        con.close()
    return connection_path
