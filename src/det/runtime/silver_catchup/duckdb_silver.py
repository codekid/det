"""Bronze ↔ silver catch-up: latest-per-interval diff, ops manifest, dbt vars.

Correctness grain: for each interval, the latest bronze ``__extract_run_datetime``
must appear in silver for that same ``(interval_start, interval_end)``. Coverage
keys are ``(interval_start, interval_end, extract_run_datetime)`` — run timestamps
alone are not unique across intervals. Older siblings are informational only.
Catch-up heals via an **immutable** ops manifest pair
(``ops/silver_catchup/<manifest_id>.json`` + ``.runs.jsonl``) and one
``det dbt --catchup`` build (DuckDB ``read_json`` or BigQuery external table on
GCS; not full-refresh).
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from det.optional_deps import require_duckdb
from det.runtime.config import PipelineConfig
from det.runtime.silver_catchup.ids import (
    _SILVER_PROBE_CHUNK,
    _coverage_key,
    _quote_ident,
    silver_relation,
)
from det.runtime.warehouse_paths import analytics_duckdb_path


def _list_silver_extract_runs_duckdb(
    config: PipelineConfig,
    *,
    project_root: Path,
    analytics_db: Path | None = None,
    intervals: Sequence[tuple[str, str]] | None = None,
) -> tuple[set[tuple[str, str, str]], str | None]:
    schema, table = silver_relation(config)
    db_path = analytics_db if analytics_db is not None else analytics_duckdb_path(project_root)
    if not db_path.is_file():
        return (
            set(),
            "catch-up silver coverage uses DuckDB analytics; "
            f"DuckDB file not found: {db_path}",
        )
    if intervals is not None and len(intervals) == 0:
        return set(), None
    duckdb = require_duckdb()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        try:
            exists = con.execute(
                """
                select count(*) from information_schema.tables
                where table_schema = ? and table_name = ?
                """,
                [schema, table],
            ).fetchone()
        except Exception as exc:
            return set(), f"catch-up silver coverage uses DuckDB analytics; {exc}"
        if not exists or exists[0] == 0:
            return (
                set(),
                "catch-up silver coverage uses DuckDB analytics; "
                f"table not found: {schema}.{table}",
            )
        # schema/table from silver_relation (pipeline ids), not caller SQL.
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        try:
            if intervals is None:
                rows = con.execute(
                    f"""
                    select distinct
                        "__interval_start_datetime",
                        "__interval_end_datetime",
                        "__extract_run_datetime"
                    from {qualified}
                    """  # noqa: S608
                ).fetchall()
            else:
                rows = []
                for i in range(0, len(intervals), _SILVER_PROBE_CHUNK):
                    chunk = intervals[i : i + _SILVER_PROBE_CHUNK]
                    ors = " or ".join(
                        "("
                        'cast("__interval_start_datetime" as timestamptz) '
                        "= cast(? as timestamptz) and "
                        'cast("__interval_end_datetime" as timestamptz) '
                        "= cast(? as timestamptz)"
                        ")"
                        for _ in chunk
                    )
                    params: list[Any] = []
                    for start, end in chunk:
                        params.extend([start, end])
                    rows.extend(
                        con.execute(
                            f"""
                            select distinct
                                "__interval_start_datetime",
                                "__interval_end_datetime",
                                "__extract_run_datetime"
                            from {qualified}
                            where {ors}
                            """,  # noqa: S608
                            params,
                        ).fetchall()
                    )
        except Exception as exc:
            # Missing interval columns (or other SELECT failures) must not look
            # like a valid empty silver — that would invent full catch-up holes.
            raise ValueError(
                "catch-up silver coverage query failed for "
                f"{schema}.{table}: {exc}"
            ) from exc
    finally:
        con.close()
    out: set[tuple[str, str, str]] = set()
    for start_raw, end_raw, run_raw in rows:
        key = _coverage_key(start_raw, end_raw, run_raw)
        if key is not None:
            out.add(key)
    return out, None

