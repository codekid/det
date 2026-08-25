"""Read-only lake inspect helpers for DET MCP (Lane A)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from jsonschema import Draft202012Validator

from det.destinations.models import (
    bronze_dataset_dir,
    duckdb_connection_path,
    postgres_dsn,
    raw_dataset_dir,
)
from det.mcp.context import PathSandboxError, project_root, resolve_under_root
from det.mcp.errors import sanitize_detail
from det.optional_deps import require_duckdb
from det.plugins import load_plugins
from det.runtime.coerce import CoerceError, coerce_record
from det.runtime.config import PipelineConfig, load_pipeline_config, resolve_path
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import LakeRef, is_lake_uri, open_lake
from det.runtime.lake import relpath as lake_relpath
from det.runtime.manifest import is_committed_raw_dir
from det.runtime.manifest import read_manifest as read_raw_manifest
from det.runtime.meta import (
    from_partition_value,
    identity_iso,
    resolve_interval,
    to_interval_datetime,
)
from det.runtime.naming import apply_naming
from det.runtime.pipelines import resolve_pipeline_ref
from det.runtime.registry import get_source
from det.runtime.secrets import SecretError
from det.sources.base import merge_source_config
from det.validation.jsonschema_validator import load_json_schema

DEFAULT_LIST_LIMIT = 200
DEFAULT_SAMPLE_LIMIT = 5
MAX_SAMPLE_LIMIT = 50
MAX_WIRE_CHARS = 4000

SampleStage = Literal["wire", "rows", "named", "coerced"]
RUN_KEY_FIELDS = ("interval_start", "interval_end", "extract_run_datetime")


def clamp_sample_limit(limit: int | None = None) -> int:
    """Caller-controlled sample size; default 5, clamped to 1..50."""
    if limit is None:
        return DEFAULT_SAMPLE_LIMIT
    return max(1, min(int(limit), MAX_SAMPLE_LIMIT))


def clamp_list_limit(limit: int | None = None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    return max(1, min(int(limit), DEFAULT_LIST_LIMIT))


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _load_pipeline(pipeline: str, root: Path) -> tuple[PipelineConfig, Path]:
    resolved = resolve_pipeline_ref(pipeline, project_root=root)
    return load_pipeline_config(resolved.path), resolved.path


def _rel(path: Path | LakeRef, root: Path) -> str:
    return lake_relpath(path, root)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _parse_hive_key(dirname: str, prefix: str) -> str | None:
    if not dirname.startswith(prefix):
        return None
    return dirname[len(prefix) :]


def _run_dict(
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


def _run_key(run: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(run["interval_start"]),
        str(run["interval_end"]),
        str(run["extract_run_datetime"]),
    )


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
                    _run_dict(
                        interval_start=start_val,
                        interval_end=end_val,
                        extract_run_datetime=run_val,
                        path=_rel(run_dir, root),
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
                    cur.execute(
                        f"""
                        select distinct
                            __interval_start_datetime,
                            __interval_end_datetime,
                            __extract_run_datetime
                        from {qualified}
                        order by 1, 2, 3
                        limit %s
                        """,
                        (limit,),
                    )
                else:
                    cur.execute(
                        f"""
                        select distinct
                            __interval_start_datetime,
                            __interval_end_datetime,
                            __extract_run_datetime
                        from {qualified}
                        where __interval_start_datetime >= %s
                          and __interval_start_datetime < %s
                        order by 1, 2, 3
                        limit %s
                        """,
                        (window[0], window[1], limit),
                    )
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


def _assert_under_raw(run_dir: Path | LakeRef, *, root: Path) -> None:
    parts = _posix_parts(run_dir)
    if "raw" not in parts:
        raise PathSandboxError(
            f"run path must be under a lake raw/ tree: {_rel(run_dir, root)}"
        )


def _assert_under_bronze(run_dir: Path | LakeRef, *, root: Path) -> None:
    parts = _posix_parts(run_dir)
    if "bronze" not in parts:
        raise PathSandboxError(
            f"run path must be under a lake bronze/ tree: {_rel(run_dir, root)}"
        )


def _posix_parts(path: Path | LakeRef) -> tuple[str, ...]:
    if isinstance(path, LakeRef):
        return tuple(str(path).replace("\\", "/").split("/"))
    return path.resolve().parts


def _resolve_lake_run_path(run_path: str, *, root: Path) -> Path | LakeRef:
    if is_lake_uri(run_path):
        return open_lake(run_path, root)
    return resolve_under_root(run_path, root=root)


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
        except Exception:
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


def to_interval_or_partition(value: str) -> str:
    """Normalize a caller interval/run value to ISO UTC."""
    compact = value.strip()
    # Hive compact form: 20260801T000000Z
    if len(compact) == 16 and compact.endswith("Z") and ":" not in compact:
        try:
            return from_partition_value(compact)
        except Exception:
            pass
    return to_interval_datetime(compact)


def _raw_run_dir(run: dict[str, Any], root: Path) -> Path | LakeRef:
    path = run.get("path")
    if not path:
        raise FileNotFoundError("resolved run has no path")
    return _resolve_lake_run_path(str(path), root=root)


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
                    "error": str(exc),
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


def diagnose_pipeline(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Composite inspect: coverage diff + optional validate on latest only_raw run."""
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_sample_limit(sample_limit)

    diff = diff_partitions(
        pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        root=base,
    )
    findings: list[dict[str, Any]] = []
    suggested: list[str] = []
    evidence: dict[str, Any] = {"diff": diff}
    validation: dict[str, Any] | None = None

    only_raw = list(diff.get("only_raw") or [])
    only_bronze = list(diff.get("only_bronze") or [])
    both_count = int(diff.get("both_count") or 0)
    raw_total = int(diff.get("only_raw_count") or 0) + both_count
    bronze_total = int(diff.get("only_bronze_count") or 0) + both_count

    if raw_total == 0 and bronze_total == 0:
        findings.append(
            {
                "severity": "error",
                "code": "empty_lake",
                "detail": (
                    "No raw or bronze runs found"
                    + (f" for window starting {interval_start}" if interval_start else "")
                    + f" under {_rel(raw_dataset_dir(config, base), base)}"
                ),
            }
        )
        suggested.append(
            f"det extract -p {config.name} -s <interval_start>"
            if not interval_start
            else f"det extract -p {config.name} -s {interval_start[:10]}"
        )
    else:
        if only_raw:
            findings.append(
                {
                    "severity": "warning",
                    "code": "raw_without_bronze",
                    "detail": (
                        f"{diff['only_raw_count']} raw run(s) have no matching bronze "
                        f"(showing up to {len(only_raw)})"
                    ),
                }
            )
            latest = max(only_raw, key=lambda r: r["extract_run_datetime"])
            start_flag = (latest.get("interval_start") or "")[:10] or (
                interval_start[:10] if interval_start else "<interval_start>"
            )
            suggested.append(f"det load -p {config.name} -s {start_flag}")
            try:
                validation = validate_sample(
                    pipeline,
                    limit=capped,
                    run_path=latest.get("path"),
                    root=base,
                )
                evidence["validation"] = validation
                if not validation.get("ok"):
                    findings.append(
                        {
                            "severity": "error",
                            "code": "schema_invalid",
                            "detail": (
                                f"validate_sample failed on latest only_raw run "
                                f"{latest.get('path')}: "
                                f"{len(validation.get('coerce_errors') or [])} coerce, "
                                f"{len(validation.get('schema_errors') or [])} schema"
                            ),
                        }
                    )
            except Exception as exc:
                findings.append(
                    {
                        "severity": "error",
                        "code": "schema_invalid",
                        "detail": f"validate_sample error on {latest.get('path')}: {exc}",
                    }
                )
            try:
                if latest.get("path"):
                    mdir = resolve_under_root(str(latest["path"]), root=base)
                    evidence["manifest"] = {
                        "path": _rel(mdir / "meta" / "manifest.json", base),
                        "manifest": read_raw_manifest(mdir),
                    }
            except Exception as exc:
                evidence["manifest_error"] = str(exc)

        if only_bronze:
            findings.append(
                {
                    "severity": "warning",
                    "code": "bronze_without_raw",
                    "detail": (
                        f"{diff['only_bronze_count']} bronze run(s) have no matching raw "
                        "(orphan, wrong lake, or raw pruned externally — "
                        "migrate rebuilds from raw only)"
                    ),
                }
            )

        if not only_raw and not only_bronze and both_count > 0:
            findings.append(
                {
                    "severity": "info",
                    "code": "ok",
                    "detail": f"raw and bronze coverage match ({both_count} run(s))",
                }
            )

    # Prefer a single summary line.
    codes = [f["code"] for f in findings]
    if "empty_lake" in codes:
        summary = "empty lake — no raw or bronze runs"
    elif "schema_invalid" in codes:
        summary = "raw ahead of bronze; sample failed schema/coerce validation"
    elif "raw_without_bronze" in codes:
        summary = f"raw ahead of bronze by {diff['only_raw_count']} run(s)"
    elif "bronze_without_raw" in codes:
        summary = f"bronze ahead of raw by {diff['only_bronze_count']} run(s)"
    elif "ok" in codes:
        summary = "raw and bronze coverage match"
    else:
        summary = "diagnose complete"

    return {
        "pipeline": config.name,
        "destination_type": config.destination.type,
        "sample_limit": capped,
        "summary": summary,
        "findings": findings,
        "evidence": evidence,
        "suggested_commands": suggested,
    }
