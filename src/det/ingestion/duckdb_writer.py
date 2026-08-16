from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import duckdb

from det.ingestion.chunks import iter_chunks
from det.ingestion.sql_ddl import ensure_bronze_table
from det.ingestion.sql_replace import delete_extract_run_sql, require_bronze_run_identity
from det.logging import get_logger
from det.runtime.sql_types import bronze_sql_columns, quote_ident

logger = get_logger(__name__)


def _cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def write_duckdb_table(
    records: Iterable[dict[str, Any]],
    *,
    connection_path: Path,
    schema: str,
    table: str,
    json_schema: dict[str, Any],
    chunk_rows: int = 10_000,
) -> Path:
    """
    Replace-by-extract-run DET bronze write into DuckDB.

    Deletes any rows for this ``__extract_run_datetime`` (same interval bounds),
    then INSERTs in chunks of ``chunk_rows`` in one transaction. Sibling extract
    runs are kept. CREATE TABLE types come from ``json_schema``, not row values.
    """
    chunks = iter_chunks(records, chunk_rows)
    first_chunk = next(chunks, None)
    if first_chunk is None:
        return connection_path

    first_identity = require_bronze_run_identity(first_chunk)
    col_types = bronze_sql_columns(json_schema, "duckdb")
    columns = [name for name, _ in col_types]
    connection_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = quote_ident(schema)
    table_sql = quote_ident(table)
    qualified = f"{schema_sql}.{table_sql}"

    con = duckdb.connect(str(connection_path))
    total = 0
    try:
        con.execute("BEGIN")
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_sql}")

            def execute(sql: str, params: Sequence[Any] | None = None) -> None:
                if params is None:
                    con.execute(sql)
                else:
                    con.execute(sql, list(params))

            def fetchall(
                sql: str, params: Sequence[Any] | None = None
            ) -> list[tuple[Any, ...]]:
                rel = (
                    con.execute(sql)
                    if params is None
                    else con.execute(sql, list(params))
                )
                return rel.fetchall()

            ensure_bronze_table(
                sql_schema=schema,
                table=table,
                columns=col_types,
                dialect="duckdb",
                execute=execute,
                fetchall=fetchall,
            )
            con.execute(
                delete_extract_run_sql(qualified, placeholder="?"),
                list(first_identity),
            )
            placeholders = ", ".join("?" for _ in columns)
            col_list = ", ".join(quote_ident(c) for c in columns)
            insert_sql = (
                f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders})"
            )

            def _insert(chunk: list[dict[str, Any]]) -> None:
                rows = [tuple(_cell(row.get(c)) for c in columns) for row in chunk]
                con.executemany(insert_sql, rows)

            _insert(first_chunk)
            total = len(first_chunk)
            for chunk in chunks:
                identity = require_bronze_run_identity(chunk)
                if identity != first_identity:
                    raise ValueError(
                        "bronze run identity does not match the batch "
                        f"({identity!r} vs {first_identity!r})"
                    )
                _insert(chunk)
                total += len(chunk)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        logger.info(
            "duckdb load finished",
            path=str(connection_path),
            table=f"{schema}.{table}",
            rows=total,
        )
    finally:
        con.close()
    return connection_path
