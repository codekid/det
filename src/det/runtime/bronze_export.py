"""Read existing bronze rows for ``det export-bq`` (filesystem or Iceberg)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import det.ingestion.iceberg_writer as iceberg_writer
from det.destinations.models import bronze_dataset_dir, lake_root
from det.ingestion.sql_replace import require_bronze_run_identity
from det.runtime.bronze_runs import walk_hive_runs
from det.runtime.config import PipelineConfig
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import is_lake_uri, open_lake
from det.runtime.limits import DEFAULT_LIST_LIMIT
from det.runtime.meta import identity_iso
from det.runtime.settings import DetSettings, get_active_settings

# Practical upper bound for one export scan (Iceberg sample API requires a limit).
_EXPORT_ICEBERG_LIMIT = 50_000_000

__all__ = [
    "iter_bronze_export_batches",
    "load_bronze_export_rows",
]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _resolve_run_dir(run_path: str, *, project_root: Path) -> Path:
    text = str(run_path).strip()
    if is_lake_uri(text):
        ref = open_lake(text, project_root)
        if not ref.is_local:
            raise ValueError(
                f"det export-bq filesystem source requires a local bronze path, got {text!r}"
            )
        return ref.to_path()
    candidate = Path(text)
    if candidate.is_absolute():
        return candidate
    return (project_root / candidate).resolve()


def _load_filesystem_batches(
    config: PipelineConfig,
    *,
    project_root: Path,
    interval_start: str,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> list[tuple[tuple[str, str, str], list[dict[str, Any]]]]:
    runs = walk_hive_runs(
        bronze_dataset_dir(config, project_root),
        root=project_root,
        limit=DEFAULT_LIST_LIMIT,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
        require_committed=True,
    )
    if extract_run_datetime is not None:
        want = identity_iso(extract_run_datetime)
        runs = [r for r in runs if identity_iso(r["extract_run_datetime"]) == want]
    batches: list[tuple[tuple[str, str, str], list[dict[str, Any]]]] = []
    for run in runs:
        run_dir = _resolve_run_dir(str(run["path"]), project_root=project_root)
        jsonl = run_dir / "data.jsonl"
        if not jsonl.is_file():
            continue
        rows = _read_jsonl(jsonl)
        if not rows:
            continue
        # Prefer row meta over hive keys — partition values are second-precision
        # and can disagree with microsecond __extract_run_datetime on the rows.
        identity = require_bronze_run_identity(rows)
        batches.append((identity, rows))
    return batches


def _load_iceberg_batches(
    config: PipelineConfig,
    *,
    project_root: Path,
    interval_start: str,
    interval_end: str | None,
    extract_run_datetime: str | None,
) -> list[tuple[tuple[str, str, str], list[dict[str, Any]]]]:
    schema, table = sql_names_for_config(config)
    location = bronze_dataset_dir(config, project_root)
    ice = iceberg_writer.load_iceberg_table(
        lake=lake_root(config.destination, project_root),
        namespace=schema,
        table=table,
        table_location=location,
    )
    if ice is None:
        return []
    rows = iceberg_writer.scan_iceberg_rows(
        ice,
        limit=_EXPORT_ICEBERG_LIMIT,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    if not rows:
        return []
    # Fail closed like filesystem export — do not silently drop rows missing meta.
    require_bronze_run_identity(rows)
    by_run: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row["__interval_start_datetime"]),
            str(row["__interval_end_datetime"]),
            str(row["__extract_run_datetime"]),
        )
        by_run.setdefault(key, []).append(row)
    return list(by_run.items())


def load_bronze_export_rows(
    config: PipelineConfig,
    *,
    project_root: Path,
    interval_start: str,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    settings: DetSettings | None = None,
) -> list[tuple[tuple[str, str, str], list[dict[str, Any]]]]:
    """
    Load bronze rows grouped by run identity for export.

    Supports ``filesystem`` (hive ``data.jsonl``) and ``iceberg``. Other
    destination types raise ``ValueError``.
    """
    _ = settings if settings is not None else get_active_settings()
    dest = config.destination.type
    if dest == "filesystem":
        return _load_filesystem_batches(
            config,
            project_root=project_root,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )
    if dest == "iceberg":
        return _load_iceberg_batches(
            config,
            project_root=project_root,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )
    raise ValueError(
        f"det export-bq supports filesystem and iceberg bronze sources, "
        f"got destination.type={dest!r}"
    )


def iter_bronze_export_batches(
    config: PipelineConfig,
    *,
    project_root: Path,
    interval_start: str,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    settings: DetSettings | None = None,
) -> Iterator[tuple[tuple[str, str, str], list[dict[str, Any]]]]:
    yield from load_bronze_export_rows(
        config,
        project_root=project_root,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
        settings=settings,
    )
