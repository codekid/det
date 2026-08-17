from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from det.runtime.sql_types import (
    Dialect,
    incompatible_column_error,
    quote_ident,
    types_compatible,
)

FetchallFn = Callable[[str, Sequence[Any] | None], list[tuple[Any, ...]]]
ExecuteFn = Callable[[str, Sequence[Any] | None], Any]


def _placeholder(dialect: Dialect) -> str:
    return "?" if dialect == "duckdb" else "%s"


def fetch_live_columns(
    fetchall: FetchallFn,
    *,
    sql_schema: str,
    table: str,
    dialect: Dialect,
) -> list[tuple[str, str]]:
    ph = _placeholder(dialect)
    sql = (
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = {ph} AND table_name = {ph} "
        "ORDER BY ordinal_position"
    )
    rows = fetchall(sql, (sql_schema, table))
    return [(str(name), str(typ)) for name, typ in rows]


def ensure_bronze_table(
    *,
    sql_schema: str,
    table: str,
    columns: Sequence[tuple[str, str]],
    dialect: Dialect,
    execute: ExecuteFn,
    fetchall: FetchallFn,
) -> None:
    """
    CREATE TABLE from schema columns, ALTER ADD missing ones, refuse mismatches.

    Extra live columns are ignored. Runs inside the caller's transaction.
    """
    schema_sql = quote_ident(sql_schema)
    table_sql = quote_ident(table)
    qualified = f"{schema_sql}.{table_sql}"
    live = {
        name: typ
        for name, typ in fetch_live_columns(
            fetchall, sql_schema=sql_schema, table=table, dialect=dialect
        )
    }
    if not live:
        defs = ", ".join(
            f"{quote_ident(name)} {sql_type}" for name, sql_type in columns
        )
        execute(f"CREATE TABLE {qualified} ({defs})", None)
        return
    for name, expected in columns:
        live_type = live.get(name)
        if live_type is None:
            execute(
                f"ALTER TABLE {qualified} ADD COLUMN {quote_ident(name)} {expected}",
                None,
            )
            continue
        if not types_compatible(live_type, expected):
            raise incompatible_column_error(
                sql_schema=sql_schema,
                table=table,
                column=name,
                live_type=live_type,
                expected_type=expected,
            )
