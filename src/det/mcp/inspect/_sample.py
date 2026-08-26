"""Raw and bronze sample/validation helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from det.destinations.models import bronze_dataset_dir, duckdb_connection_path, postgres_dsn
from det.mcp.errors import sanitize_detail
from det.optional_deps import require_duckdb
from det.plugins import load_plugins
from det.runtime.coerce import CoerceError, coerce_record
from det.runtime.config import PipelineConfig, resolve_path
from det.runtime.lake import LakeRef
from det.runtime.manifest import read_manifest as read_raw_manifest
from det.runtime.meta import resolve_interval
from det.runtime.naming import apply_naming
from det.runtime.registry import get_source
from det.runtime.secrets import SecretError
from det.sources.base import merge_source_config
from det.validation.jsonschema_validator import load_json_schema

from ._common import (
    DEFAULT_LIST_LIMIT,
    DEFAULT_SAMPLE_LIMIT,
    MAX_WIRE_CHARS,
    SampleStage,
    _assert_under_bronze,
    _load_pipeline,
    _quote_ident,
    _rel,
    _resolve_lake_run_path,
    _root,
    clamp_sample_limit,
    walk_hive_runs,
)
from ._partitions import (
    _raw_run_dir,
    resolve_raw_run,
    to_interval_or_partition,
)


def _iter_load_rows(
    config: PipelineConfig,
    *,
    root: Path,
    run: dict[str, Any],
    limit: int,
    stage: SampleStage,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    """Yield up to *limit* rows at the requested stage. Returns (rows, errors, truncated)."""
    load_plugins()
    run_dir = _raw_run_dir(run, root)
    manifest = read_raw_manifest(run_dir)
    source = get_source(config.source.type)
    effective = merge_source_config(source.defaults(), config.source.overrides)
    schema = load_json_schema(resolve_path(root, config.schema_path))

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    seen = 0
    truncated = False
    for source_row in source.records_from_raw(
        config=effective, raw_dir=run_dir, manifest=manifest
    ):
        if seen >= limit:
            truncated = True
            break
        idx = seen
        seen += 1
        data = source_row.data
        if stage == "rows":
            rows.append(
                {
                    "index": idx,
                    "filename": source_row.filename,
                    "data": data,
                }
            )
            continue
        try:
            named = apply_naming(data, config.bronze.naming)
        except Exception as exc:
            errors.append({"index": idx, "phase": "naming", "message": str(exc)})
            continue
        if stage == "named":
            rows.append(
                {
                    "index": idx,
                    "filename": source_row.filename,
                    "data": named,
                }
            )
            continue
        # coerced
        try:
            typed = coerce_record(named, schema)
        except CoerceError as exc:
            errors.append({"index": idx, "phase": "coerce", "message": str(exc)})
            continue
        rows.append(
            {
                "index": idx,
                "filename": source_row.filename,
                "data": typed,
            }
        )
    return rows, errors, truncated


def _sample_wire(
    run_dir: Path | LakeRef, *, root: Path, limit: int
) -> tuple[list[dict[str, Any]], bool]:
    data_dir = run_dir / "data"
    artifacts: list[Path | LakeRef] = []
    if data_dir.is_dir():
        artifacts = sorted(p for p in data_dir.rglob("*") if p.is_file())
    peeks: list[dict[str, Any]] = []
    for path in artifacts[:limit]:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            peeks.append(
                {
                    "path": _rel(path, root),
                    "error": sanitize_detail(exc),
                }
            )
            continue
        truncated_text = len(text) > MAX_WIRE_CHARS
        peeks.append(
            {
                "path": _rel(path, root),
                "bytes": path.stat().st_size,
                "preview": text[:MAX_WIRE_CHARS],
                "truncated": truncated_text,
            }
        )
    return peeks, len(artifacts) > limit


def sample_raw(
    pipeline: str,
    *,
    stage: SampleStage = "named",
    limit: int = DEFAULT_SAMPLE_LIMIT,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    if stage not in {"wire", "rows", "named", "coerced"}:
        raise ValueError(
            f"stage must be one of wire|rows|named|coerced, got {stage!r}"
        )
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_sample_limit(limit)
    run = resolve_raw_run(
        config,
        root=base,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    run_dir = _raw_run_dir(run, base)

    if stage == "wire":
        peeks, truncated = _sample_wire(run_dir, root=base, limit=capped)
        return {
            "pipeline": config.name,
            "stage": stage,
            "limit": capped,
            "run": run,
            "run_path": run["path"],
            "rows": peeks,
            "errors": [],
            "truncated": truncated,
        }

    rows, errors, truncated = _iter_load_rows(
        config, root=base, run=run, limit=capped, stage=stage
    )
    return {
        "pipeline": config.name,
        "stage": stage,
        "limit": capped,
        "run": run,
        "run_path": run["path"],
        "rows": rows,
        "errors": errors,
        "truncated": truncated,
    }


def collect_validation_errors(
    records: list[dict[str, Any]],
    schema: dict[str, Any],
    *,
    max_errors: int = 20,
) -> list[dict[str, Any]]:
    """Non-raising JSON Schema error collection for MCP."""
    validator = Draft202012Validator(schema)
    errors: list[dict[str, Any]] = []
    for i, record in enumerate(records):
        for err in sorted(validator.iter_errors(record), key=lambda e: list(e.path)):
            errors.append(
                {
                    "index": i,
                    "path": list(err.path),
                    "message": err.message,
                }
            )
            if len(errors) >= max_errors:
                return errors
    return errors


def validate_sample(
    pipeline: str,
    *,
    limit: int = DEFAULT_SAMPLE_LIMIT,
    max_errors: int = 20,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_sample_limit(limit)
    err_cap = max(1, min(int(max_errors), 100))
    run = resolve_raw_run(
        config,
        root=base,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    load_plugins()
    run_dir = _raw_run_dir(run, base)
    manifest = read_raw_manifest(run_dir)
    source = get_source(config.source.type)
    effective = merge_source_config(source.defaults(), config.source.overrides)
    schema = load_json_schema(resolve_path(base, config.schema_path))

    coerce_errors: list[dict[str, Any]] = []
    schema_errors: list[dict[str, Any]] = []
    checked = 0
    valid_rows: list[dict[str, Any]] = []
    truncated = False

    for source_row in source.records_from_raw(
        config=effective, raw_dir=run_dir, manifest=manifest
    ):
        if checked >= capped:
            truncated = True
            break
        idx = checked
        checked += 1
        try:
            named = apply_naming(source_row.data, config.bronze.naming)
        except Exception as exc:
            coerce_errors.append(
                {"index": idx, "path": [], "message": f"naming: {exc}"}
            )
            if len(coerce_errors) + len(schema_errors) >= err_cap:
                break
            continue
        try:
            typed = coerce_record(named, schema)
        except CoerceError as exc:
            coerce_errors.append({"index": idx, "path": [], "message": str(exc)})
            if len(coerce_errors) + len(schema_errors) >= err_cap:
                break
            continue
        row_errs = collect_validation_errors([typed], schema, max_errors=err_cap)
        for e in row_errs:
            schema_errors.append(
                {
                    "index": idx,
                    "path": e["path"],
                    "message": e["message"],
                }
            )
        if not row_errs:
            valid_rows.append(typed)
        if len(coerce_errors) + len(schema_errors) >= err_cap:
            break

    ok = checked > 0 and not coerce_errors and not schema_errors
    return {
        "pipeline": config.name,
        "limit": capped,
        "max_errors": err_cap,
        "run": run,
        "run_path": run["path"],
        "rows_checked": checked,
        "ok": ok,
        "coerce_errors": coerce_errors[:err_cap],
        "schema_errors": schema_errors[:err_cap],
        "truncated": truncated,
    }


def _resolve_bronze_fs_run(
    config: PipelineConfig,
    *,
    root: Path,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
) -> dict[str, Any]:
    if run_path is not None:
        run_dir = _resolve_lake_run_path(run_path, root=root)
        if not run_dir.is_dir():
            raise FileNotFoundError(f"run path is not a directory: {run_dir}")
        _assert_under_bronze(run_dir, root=root)
        return {"path": _rel(run_dir, root)}

    runs = walk_hive_runs(
        bronze_dataset_dir(config, root),
        root=root,
        limit=DEFAULT_LIST_LIMIT,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
    )
    if extract_run_datetime is not None:
        want = to_interval_or_partition(extract_run_datetime)
        runs = [r for r in runs if r["extract_run_datetime"] == want]
    if not runs:
        raise FileNotFoundError(f"no bronze runs found for pipeline {config.name}")
    return max(runs, key=lambda r: r["extract_run_datetime"])


def _sample_bronze_filesystem(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    run_path: str | None,
    interval_start: str | None,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> dict[str, Any]:
    run = _resolve_bronze_fs_run(
        config,
        root=root,
        run_path=run_path,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    run_dir = _resolve_lake_run_path(str(run["path"]), root=root)
    jsonl = run_dir / "data.jsonl"
    rows: list[dict[str, Any]] = []
    truncated = False
    if not jsonl.is_file():
        return {
            "pipeline": config.name,
            "destination_type": "filesystem",
            "limit": limit,
            "run": run,
            "run_path": run["path"],
            "rows": [],
            "errors": [{"message": f"data.jsonl not found under {run['path']}"}],
            "truncated": False,
            "note": "Bronze samples are for inspection only; rebuild via det migrate from raw.",
        }
    with jsonl.open(encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= limit:
                truncated = True
                break
            line = line.strip()
            if not line:
                continue
            rows.append({"index": i, "data": json.loads(line)})
    return {
        "pipeline": config.name,
        "destination_type": "filesystem",
        "limit": limit,
        "run": run,
        "run_path": run["path"],
        "rows": rows,
        "errors": [],
        "truncated": truncated,
        "note": "Bronze samples are for inspection only; rebuild via det migrate from raw.",
    }


def _sql_sample_filters(
    *,
    interval_start: str | None,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)
        clauses.append("__interval_start_datetime >= ?")
        params.append(window[0])
        clauses.append("__interval_start_datetime < ?")
        params.append(window[1])
    if extract_run_datetime is not None:
        clauses.append("__extract_run_datetime = ?")
        params.append(to_interval_or_partition(extract_run_datetime))
    where = (" where " + " and ".join(clauses)) if clauses else ""
    return where, params


def _sample_bronze_duckdb(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> dict[str, Any]:
    from det.runtime.ids import sql_names_for_config

    db_path = duckdb_connection_path(config.destination, root)
    schema, table = sql_names_for_config(config)
    qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    note = "Bronze samples are for inspection only; rebuild via det migrate from raw."
    base_out: dict[str, Any] = {
        "pipeline": config.name,
        "destination_type": "duckdb",
        "limit": limit,
        "schema": schema,
        "table": table,
        "connection": _rel(db_path, root),
        "note": note,
    }
    if not db_path.exists():
        return {
            **base_out,
            "rows": [],
            "errors": [{"message": "DuckDB file not found"}],
            "truncated": False,
        }

    where, params = _sql_sample_filters(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    # DuckDB uses ? placeholders; rebuild where for duckdb (already ?).
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
            return {
                **base_out,
                "rows": [],
                "errors": [{"message": f"table not found: {schema}.{table}"}],
                "truncated": False,
            }
        sql = (
            f"select * from {qualified}{where} "
            f"order by __extract_run_datetime "
            f"limit ?"
        )
        result = con.execute(sql, [*params, limit + 1])
        cols = [d[0] for d in result.description]
        fetched = result.fetchall()
    finally:
        con.close()

    truncated = len(fetched) > limit
    rows = [
        {"index": i, "data": dict(zip(cols, row, strict=True))}
        for i, row in enumerate(fetched[:limit])
    ]
    return {**base_out, "rows": rows, "errors": [], "truncated": truncated}


def _sample_bronze_postgres(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> dict[str, Any]:
    from det.runtime.ids import sql_names_for_config

    _ = root
    schema, table = sql_names_for_config(config)
    qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    note = "Bronze samples are for inspection only; rebuild via det migrate from raw."
    base_out: dict[str, Any] = {
        "pipeline": config.name,
        "destination_type": "postgres",
        "limit": limit,
        "schema": schema,
        "table": table,
        "note": note,
    }
    try:
        import psycopg
    except ImportError:
        return {
            **base_out,
            "rows": [],
            "errors": [
                {
                    "message": (
                        'Postgres inspect requires the optional extra: '
                        'pip install -e ".[postgres]"'
                    )
                }
            ],
            "truncated": False,
        }
    try:
        dsn = postgres_dsn(config.destination, backend="env")
    except (SecretError, ValueError) as exc:
        return {
            **base_out,
            "rows": [],
            "errors": [{"message": sanitize_detail(exc)}],
            "truncated": False,
        }

    where_duck, params = _sql_sample_filters(
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    where = where_duck.replace("?", "%s")
    sql = (
        f"select * from {qualified}{where} "
        f"order by __extract_run_datetime "
        f"limit %s"
    )
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
                return {
                    **base_out,
                    "rows": [],
                    "errors": [{"message": f"table not found: {schema}.{table}"}],
                    "truncated": False,
                }
            cur.execute(sql, [*params, limit + 1])
            cols = [d.name for d in cur.description] if cur.description else []
            fetched = cur.fetchall()

    truncated = len(fetched) > limit
    rows = [
        {"index": i, "data": dict(zip(cols, row, strict=True))}
        for i, row in enumerate(fetched[:limit])
    ]
    return {**base_out, "rows": rows, "errors": [], "truncated": truncated}


def _sample_bronze_iceberg(
    config: PipelineConfig,
    *,
    root: Path,
    limit: int,
    interval_start: str | None,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> dict[str, Any]:
    from det.destinations.models import lake_root
    from det.ingestion.iceberg_writer import load_iceberg_table, scan_iceberg_rows
    from det.runtime.ids import sql_names_for_config

    schema, table = sql_names_for_config(config)
    location = bronze_dataset_dir(config, root)
    note = "Bronze samples are for inspection only; rebuild via det migrate from raw."
    base_out: dict[str, Any] = {
        "pipeline": config.name,
        "destination_type": "iceberg",
        "limit": limit,
        "schema": schema,
        "table": table,
        "location": _rel(location, root),
        "note": note,
    }
    try:
        ice = load_iceberg_table(
            lake=lake_root(config.destination, root),
            namespace=schema,
            table=table,
            table_location=location,
        )
    except ImportError as exc:
        return {
            **base_out,
            "rows": [],
            "errors": [{"message": str(exc)}],
            "truncated": False,
        }
    if ice is None:
        return {
            **base_out,
            "rows": [],
            "errors": [{"message": f"Iceberg table not found: {schema}.{table}"}],
            "truncated": False,
        }
    fetched = scan_iceberg_rows(
        ice,
        limit=limit + 1,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    truncated = len(fetched) > limit
    rows = [{"index": i, "data": row} for i, row in enumerate(fetched[:limit])]
    return {**base_out, "rows": rows, "errors": [], "truncated": truncated}


def sample_bronze(
    pipeline: str,
    *,
    limit: int = DEFAULT_SAMPLE_LIMIT,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Sample landed bronze rows. Inspection only — rebuild from raw via migrate."""
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_sample_limit(limit)
    dest = config.destination
    if dest.type == "filesystem":
        return _sample_bronze_filesystem(
            config,
            root=base,
            limit=capped,
            run_path=run_path,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )
    if dest.type == "duckdb":
        if run_path is not None:
            return {
                "pipeline": config.name,
                "destination_type": "duckdb",
                "limit": capped,
                "rows": [],
                "errors": [
                    {
                        "message": (
                            "run_path applies to filesystem bronze only; "
                            "use interval_start / extract_run_datetime filters"
                        )
                    }
                ],
                "truncated": False,
                "note": "Bronze samples are for inspection only; rebuild via det migrate from raw.",
            }
        return _sample_bronze_duckdb(
            config,
            root=base,
            limit=capped,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )
    if dest.type == "postgres":
        if run_path is not None:
            return {
                "pipeline": config.name,
                "destination_type": "postgres",
                "limit": capped,
                "rows": [],
                "errors": [
                    {
                        "message": (
                            "run_path applies to filesystem bronze only; "
                            "use interval_start / extract_run_datetime filters"
                        )
                    }
                ],
                "truncated": False,
                "note": "Bronze samples are for inspection only; rebuild via det migrate from raw.",
            }
        return _sample_bronze_postgres(
            config,
            root=base,
            limit=capped,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )
    if dest.type == "iceberg":
        if run_path is not None:
            return {
                "pipeline": config.name,
                "destination_type": "iceberg",
                "limit": capped,
                "rows": [],
                "errors": [
                    {
                        "message": (
                            "run_path applies to filesystem bronze only; "
                            "use interval_start / extract_run_datetime filters"
                        )
                    }
                ],
                "truncated": False,
                "note": "Bronze samples are for inspection only; rebuild via det migrate from raw.",
            }
        return _sample_bronze_iceberg(
            config,
            root=base,
            limit=capped,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )
    raise ValueError(f"unsupported destination.type={dest.type!r}")
