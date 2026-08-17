from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any

from det.ingestion.chunks import iter_chunks
from det.ingestion.sql_ddl import ensure_bronze_table
from det.ingestion.sql_replace import delete_extract_run_sql, require_bronze_run_identity
from det.logging import get_logger
from det.runtime.lease import advisory_lock_keys
from det.runtime.sql_types import bronze_sql_columns, quote_ident

logger = get_logger(__name__)


def _cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            'Postgres destination requires the optional extra: pip install -e ".[postgres]"'
        ) from exc
    return psycopg


def write_postgres_table(
    records: Iterable[dict[str, Any]],
    *,
    dsn: str,
    schema: str,
    table: str,
    json_schema: dict[str, Any],
    chunk_rows: int = 10_000,
    pipeline: str | None = None,
) -> str:
    """
    Replace-by-extract-run DET bronze write into Postgres.

    Deletes any rows for this ``__extract_run_datetime`` (same interval bounds),
    then INSERTs in chunks of ``chunk_rows`` in one transaction. Sibling extract
    runs are kept. CREATE TABLE types come from ``json_schema``, not row values.
    Returns the DSN (connection identity for RunResult.partition_dir).
    """
    psycopg = _import_psycopg()
    chunks = iter_chunks(records, chunk_rows)
    first_chunk = next(chunks, None)
    if first_chunk is None:
        return dsn

    first_identity = require_bronze_run_identity(first_chunk)
    col_types = bronze_sql_columns(json_schema, "postgres")
    columns = [name for name, _ in col_types]
    schema_sql = quote_ident(schema)
    table_sql = quote_ident(table)
    qualified = f"{schema_sql}.{table_sql}"

    total = 0
    with psycopg.connect(dsn) as conn:
        try:
            with conn.cursor() as cur:

                def execute(sql: str, params: Sequence[Any] | None = None) -> None:
                    if params is None:
                        cur.execute(sql)
                    else:
                        cur.execute(sql, params)

                def fetchall(
                    sql: str, params: Sequence[Any] | None = None
                ) -> list[tuple[Any, ...]]:
                    execute(sql, params)
                    return list(cur.fetchall())

                execute(f"CREATE SCHEMA IF NOT EXISTS {schema_sql}", None)
                if pipeline:
                    k1, k2 = advisory_lock_keys(
                        pipeline, first_identity[0], first_identity[1]
                    )
                    execute("SELECT pg_advisory_lock(%s, %s)", (k1, k2))
                ensure_bronze_table(
                    sql_schema=schema,
                    table=table,
                    columns=col_types,
                    dialect="postgres",
                    execute=execute,
                    fetchall=fetchall,
                )
                execute(
                    delete_extract_run_sql(qualified, placeholder="%s"),
                    first_identity,
                )
                col_list = ", ".join(quote_ident(c) for c in columns)
                placeholders = ", ".join(["%s"] * len(columns))
                insert_sql = (
                    f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders})"
                )

                def _insert(chunk: list[dict[str, Any]]) -> None:
                    rows = [tuple(_cell(row.get(c)) for c in columns) for row in chunk]
                    cur.executemany(insert_sql, rows)

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
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    logger.info(
        "postgres load finished",
        table=f"{schema}.{table}",
        rows=total,
    )
    return dsn
