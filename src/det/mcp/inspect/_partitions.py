"""Bronze/raw partition listing, diff, and run resolution helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.destinations.models import (
    bronze_dataset_dir,
    duckdb_connection_path,
    postgres_dsn,
    raw_dataset_dir,
)
from det.mcp.errors import sanitize_detail
from det.optional_deps import require_duckdb
from det.runtime.config import PipelineConfig
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import LakeRef
from det.runtime.manifest import is_committed_raw_dir
from det.runtime.meta import (
    from_partition_value,
    identity_iso,
    resolve_interval,
    to_interval_datetime,
)
from det.runtime.secrets import SecretError

from ._common import (
    DEFAULT_LIST_LIMIT,
    _assert_under_raw,
    _load_pipeline,
    _parse_hive_key,
    _quote_ident,
    _rel,
    _resolve_lake_run_path,
    _root,
    _run_dict,
    _run_key,
    clamp_list_limit,
    walk_hive_runs,
)


def to_interval_or_partition(value: str) -> str:
    """Normalize a caller interval/run value to ISO UTC."""
    compact = value.strip()
    # Hive compact form: 20260801T000000Z
    if len(compact) == 16 and compact.endswith("Z") and ":" not in compact:
        try:
            return from_partition_value(compact)
        except Exception:  # noqa: S110  # fall through to to_interval_datetime
            pass
    return to_interval_datetime(compact)


def _raw_run_dir(run: dict[str, Any], root: Path) -> Path | LakeRef:
    path = run.get("path")
    if not path:
        raise FileNotFoundError("resolved run has no path")
    return _resolve_lake_run_path(str(path), root=root)


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
            return [], f"DuckDB file not found: {_rel(db_path, root)}"
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
            _run_dict(
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
            return [], sanitize_detail(exc)
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
                    cur.execute(query, (window[0], window[1], limit))  # pyright: ignore[reportArgumentType]
                rows = cur.fetchall()
        return [
            _run_dict(
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
        _run_dict(
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


def diff_partitions(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    limit: int = DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_list_limit(limit)

    raw_dir = raw_dataset_dir(config, base)
    # Oversample slightly so set math is meaningful when both sides are large.
    walk_cap = capped
    raw_runs = walk_hive_runs(
        raw_dir,
        root=base,
        limit=walk_cap,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
        require_committed=True,
    )
    bronze_runs, bronze_note = list_bronze_runs(
        config,
        root=base,
        limit=walk_cap,
        interval_start=interval_start,
        interval_end=interval_end,
    )

    raw_by_key = {_run_key(r): r for r in raw_runs}
    bronze_by_key = {_run_key(r): r for r in bronze_runs}
    raw_keys = set(raw_by_key)
    bronze_keys = set(bronze_by_key)

    only_raw_keys = sorted(raw_keys - bronze_keys)
    only_bronze_keys = sorted(bronze_keys - raw_keys)
    both_keys = raw_keys & bronze_keys

    only_raw = [raw_by_key[k] for k in only_raw_keys[:capped]]
    only_bronze = [bronze_by_key[k] for k in only_bronze_keys[:capped]]

    out: dict[str, Any] = {
        "pipeline": config.name,
        "destination_type": config.destination.type,
        "limit": capped,
        "only_raw": only_raw,
        "only_bronze": only_bronze,
        "both_count": len(both_keys),
        "only_raw_count": len(only_raw_keys),
        "only_bronze_count": len(only_bronze_keys),
        "truncated": (
            len(only_raw_keys) > capped
            or len(only_bronze_keys) > capped
            or len(raw_runs) >= walk_cap
            or len(bronze_runs) >= walk_cap
        ),
        "raw_dataset_dir": _rel(raw_dir, base),
    }
    if config.destination.type == "filesystem":
        out["bronze_dataset_dir"] = _rel(bronze_dataset_dir(config, base), base)
    else:
        sql_schema, sql_table = sql_names_for_config(config)
        out["bronze_schema"] = sql_schema
        out["bronze_table"] = sql_table
    if bronze_note:
        out["note"] = bronze_note
    return out


def resolve_raw_run(
    config: PipelineConfig,
    *,
    root: Path,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
) -> dict[str, Any]:
    """Resolve a raw extract-run directory; prefer run_path, else latest match."""
    if run_path is not None:
        run_dir = _resolve_lake_run_path(run_path, root=root)
        if not run_dir.is_dir():
            raise FileNotFoundError(f"run path is not a directory: {run_dir}")
        _assert_under_raw(run_dir, root=root)
        if not is_committed_raw_dir(run_dir):
            raise FileNotFoundError(
                f"run path is not a committed extract (no meta/manifest.json): "
                f"{_rel(run_dir, root)}"
            )
        # Derive keys from hive path when possible.
        start = end = run = None
        try:
            run_compact = _parse_hive_key(run_dir.name, "__extract_run_datetime=")
            end_dir = run_dir.parent
            start_dir = end_dir.parent
            end_compact = _parse_hive_key(end_dir.name, "__interval_end_datetime=")
            start_compact = _parse_hive_key(start_dir.name, "__interval_start_datetime=")
            if start_compact and end_compact and run_compact:
                start = from_partition_value(start_compact)
                end = from_partition_value(end_compact)
                run = from_partition_value(run_compact)
        except Exception:  # noqa: S110  # skip unparseable hive keys
            pass
        return _run_dict(
            interval_start=start or "",
            interval_end=end or "",
            extract_run_datetime=run or "",
            path=_rel(run_dir, root),
        )

    runs = walk_hive_runs(
        raw_dataset_dir(config, root),
        root=root,
        limit=DEFAULT_LIST_LIMIT,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
        require_committed=True,
    )
    if extract_run_datetime is not None:
        want = to_interval_or_partition(extract_run_datetime)
        runs = [r for r in runs if r["extract_run_datetime"] == want]
    if not runs:
        raise FileNotFoundError(
            f"no raw runs found for pipeline {config.name}"
            + (f" in window starting {interval_start}" if interval_start else "")
        )
    # Latest by extract_run_datetime (ISO sorts lexicographically for UTC).
    return max(runs, key=lambda r: r["extract_run_datetime"])
