"""Bronze extract-run listing across filesystem / Iceberg / SQL destinations.

Kernel API used by silver catch-up and MCP inspect. MCP may wrap with sandboxing;
this module must not import ``det.mcp``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.destinations.models import (
    bronze_dataset_dir,
    duckdb_connection_path,
    postgres_dsn,
)
from det.optional_deps import require_duckdb
from det.runtime.config import PipelineConfig
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import LakeRef
from det.runtime.lake import relpath as lake_relpath
from det.runtime.limits import DEFAULT_LIST_LIMIT, clamp_list_limit
from det.runtime.manifest import is_committed_raw_dir
from det.runtime.meta import (
    from_partition_value,
    identity_iso,
    resolve_interval,
)
from det.runtime.secrets import SecretError

__all__ = [
    "DEFAULT_LIST_LIMIT",
    "clamp_list_limit",
    "list_bronze_runs",
    "run_dict",
    "walk_hive_runs",
]


def run_dict(
    *,
    interval_start: str,
    interval_end: str,
    extract_run_datetime: str,
    path: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "interval_start": interval_start,
        "interval_end": interval_end,
        "extract_run_datetime": extract_run_datetime,
    }
    if path is not None:
        out["path"] = path
    return out


def _parse_hive_key(dirname: str, prefix: str) -> str | None:
    if not dirname.startswith(prefix):
        return None
    return dirname[len(prefix) :]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def walk_hive_runs(
    dataset_dir: Path | LakeRef,
    *,
    root: Path,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
    normalize_iso: bool = True,
    require_committed: bool = False,
) -> list[dict[str, Any]]:
    """Walk hive interval/extract-run dirs; optionally filter and normalize to ISO."""
    out: list[dict[str, Any]] = []
    if not dataset_dir.is_dir():
        return out

    window: tuple[str, str] | None = None
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)

    for start_dir in sorted(dataset_dir.iterdir()):
        if not start_dir.is_dir():
            continue
        start_raw = _parse_hive_key(start_dir.name, "__interval_start_datetime=")
        if start_raw is None:
            continue
        start_val = from_partition_value(start_raw) if normalize_iso else start_raw
        if window is not None and not (window[0] <= start_val < window[1]):
            continue
        for end_dir in sorted(start_dir.iterdir()):
            if not end_dir.is_dir():
                continue
            end_raw = _parse_hive_key(end_dir.name, "__interval_end_datetime=")
            if end_raw is None:
                continue
            end_val = from_partition_value(end_raw) if normalize_iso else end_raw
            for run_dir in sorted(end_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                run_raw = _parse_hive_key(run_dir.name, "__extract_run_datetime=")
                if run_raw is None:
                    continue
                if require_committed and not is_committed_raw_dir(run_dir):
                    continue
                run_val = from_partition_value(run_raw) if normalize_iso else run_raw
                out.append(
                    run_dict(
                        interval_start=start_val,
                        interval_end=end_val,
                        extract_run_datetime=run_val,
                        path=lake_relpath(run_dir, root),
                    )
                )
                if len(out) >= limit:
                    return out
    return out


def _list_bronze_sql_runs(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Distinct bronze extract-run keys from DuckDB or Postgres. Returns (runs, note)."""
    dest = config.destination
    window: tuple[str, str] | None = None
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)

    schema, table = sql_names_for_config(config)
    qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"

    if dest.type == "duckdb":
        db_path = duckdb_connection_path(dest, root)
        if not db_path.exists():
            return [], f"DuckDB file not found: {lake_relpath(db_path, root)}"
        duckdb = require_duckdb()
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            exists = con.execute(
                """
                select count(*) from information_schema.tables
                where table_schema = ? and table_name = ?
                """,
                [schema, table],
            ).fetchone()
            if not exists or exists[0] == 0:
                return [], f"table not found: {schema}.{table}"
            if window is None:
                rows = con.execute(
                    f"""
                    select distinct
                        __interval_start_datetime,
                        __interval_end_datetime,
                        __extract_run_datetime
                    from {qualified}
                    order by 1, 2, 3
                    limit ?
                    """,
                    [limit],
                ).fetchall()
            else:
                rows = con.execute(
                    f"""
                    select distinct
                        __interval_start_datetime,
                        __interval_end_datetime,
                        __extract_run_datetime
                    from {qualified}
                    where __interval_start_datetime >= ?
                      and __interval_start_datetime < ?
                    order by 1, 2, 3
                    limit ?
                    """,
                    [window[0], window[1], limit],
                ).fetchall()
        finally:
            con.close()
        return [
            run_dict(
                interval_start=identity_iso(r[0]),
                interval_end=identity_iso(r[1]),
                extract_run_datetime=identity_iso(r[2]),
            )
            for r in rows
        ], None

    if dest.type == "postgres":
        try:
            import psycopg
        except ImportError:
            return [], (
                'Postgres inspect requires the optional extra: pip install -e ".[postgres]"'
            )
        try:
            dsn = postgres_dsn(dest, backend="env")
        except (SecretError, ValueError) as exc:
            return [], str(exc)
        _ro = "-c default_transaction_read_only=on"
        with psycopg.connect(dsn, options=_ro) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select count(*) from information_schema.tables
                    where table_schema = %s and table_name = %s
                    """,
                    (schema, table),
                )
                exists = cur.fetchone()
                if not exists or exists[0] == 0:
                    return [], f"table not found: {schema}.{table}"
                if window is None:
                    query = f"""
                        select distinct
                            __interval_start_datetime,
                            __interval_end_datetime,
                            __extract_run_datetime
                        from {qualified}
                        order by 1, 2, 3
                        limit %s
                        """
                    cur.execute(query, (limit,))  # pyright: ignore[reportArgumentType]
                else:
                    query = f"""
                        select distinct
                            __interval_start_datetime,
                            __interval_end_datetime,
                            __extract_run_datetime
                        from {qualified}
                        where __interval_start_datetime >= %s
                          and __interval_start_datetime < %s
                        order by 1, 2, 3
                        limit %s
                        """
                    cur.execute(
                        query, (window[0], window[1], limit)
                    )  # pyright: ignore[reportArgumentType]
                rows = cur.fetchall()
        return [
            run_dict(
                interval_start=identity_iso(r[0]),
                interval_end=identity_iso(r[1]),
                extract_run_datetime=identity_iso(r[2]),
            )
            for r in rows
        ], None

    return [], f"unsupported destination.type={dest.type!r}"


def _list_bronze_iceberg_runs(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    from det.destinations.models import lake_root
    from det.ingestion.iceberg_writer import list_iceberg_extract_runs, load_iceberg_table

    schema, table = sql_names_for_config(config)
    window: tuple[str, str] | None = None
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)
    try:
        ice = load_iceberg_table(
            lake=lake_root(config.destination, root),
            namespace=schema,
            table=table,
            table_location=bronze_dataset_dir(config, root),
        )
    except ImportError as exc:
        return [], str(exc)
    if ice is None:
        return [], f"Iceberg table not found: {schema}.{table}"
    rows = list_iceberg_extract_runs(
        ice,
        window_start=window[0] if window else None,
        window_end=window[1] if window else None,
        limit=limit,
    )
    return [
        run_dict(
            interval_start=start,
            interval_end=end,
            extract_run_datetime=run,
        )
        for start, end, run in rows
    ], None


def list_bronze_runs(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    dest = config.destination
    if dest.type == "filesystem":
        runs = walk_hive_runs(
            bronze_dataset_dir(config, root),
            root=root,
            limit=limit,
            interval_start=interval_start,
            interval_end=interval_end,
            normalize_iso=True,
        )
        return runs, None
    if dest.type == "iceberg":
        return _list_bronze_iceberg_runs(
            config,
            root=root,
            limit=limit,
            interval_start=interval_start,
            interval_end=interval_end,
        )
    return _list_bronze_sql_runs(
        config,
        root=root,
        limit=limit,
        interval_start=interval_start,
        interval_end=interval_end,
    )
