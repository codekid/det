"""Read existing bronze rows for ``det export-bq`` (filesystem or Iceberg)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pendulum
from pendulum import DateTime

import det.ingestion.iceberg_writer as iceberg_writer
from det.destinations.models import bronze_dataset_dir, lake_root
from det.ingestion.sql_replace import require_bronze_run_identity
from det.runtime.bronze_runs import walk_hive_runs
from det.runtime.config import PipelineConfig
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import is_lake_uri, open_lake
from det.runtime.limits import DEFAULT_LIST_LIMIT
from det.runtime.meta import identity_iso
from det.runtime.settings import DetSettings

# Practical upper bound for one export scan (Iceberg sample API requires a limit).
_EXPORT_ICEBERG_LIMIT = 50_000_000

_START = "__interval_start_datetime"
_END = "__interval_end_datetime"
_RUN = "__extract_run_datetime"

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


def _second_precision_iso(value: object) -> str:
    """Normalize to UTC ISO with microsecond=0 (hive directory precision)."""
    text = identity_iso(value)
    parsed = pendulum.parse(text)
    if not isinstance(parsed, DateTime):
        raise ValueError(f"Not an extract-run datetime: {value!r}")
    return parsed.in_timezone("UTC").replace(microsecond=0).isoformat()


def _has_fractional_seconds(value: object) -> bool:
    text = identity_iso(value)
    parsed = pendulum.parse(text)
    if not isinstance(parsed, DateTime):
        return False
    return int(parsed.microsecond) != 0


def _hive_run_matches(got: str, want: str) -> bool:
    if got == want:
        return True
    # Hive dirs are second-precision; still find the partition for fractional wants.
    return _second_precision_iso(got) == _second_precision_iso(want)


def _assert_requested_run(identity: tuple[str, str, str], want: str) -> None:
    """Ensure a requested extract-run selects the row identity (or second-precision)."""
    row_run = identity_iso(identity[2])
    if row_run == want:
        return
    # Second-precision request may land on microsecond row meta — keep prior behavior.
    if (
        not _has_fractional_seconds(want)
        and _second_precision_iso(row_run) == _second_precision_iso(want)
    ):
        return
    raise ValueError(
        f"--extract-run-datetime {want!r} does not match bronze row identity "
        f"{row_run!r}"
    )


def _require_bronze_meta_present(rows: list[dict[str, Any]]) -> None:
    for i, row in enumerate(rows):
        for key in (_START, _END, _RUN):
            value = row.get(key)
            if value is None or not str(value).strip():
                raise ValueError(
                    f"record[{i}] missing bronze run meta {key} "
                    f"(required for det export-bq)"
                )


def _load_filesystem_batches(
    config: PipelineConfig,
    *,
    project_root: Path,
    interval_start: str,
    interval_end: str | None,
    extract_run_datetime: str | None,
    settings: DetSettings | None,
) -> list[tuple[tuple[str, str, str], list[dict[str, Any]]]]:
    # Probe one past the list cap so a partial window cannot silently omit runs.
    runs = walk_hive_runs(
        bronze_dataset_dir(config, project_root, settings=settings),
        root=project_root,
        limit=DEFAULT_LIST_LIMIT + 1,
        interval_start=interval_start,
        interval_end=interval_end,
        normalize_iso=True,
        require_committed=True,
    )
    if len(runs) > DEFAULT_LIST_LIMIT:
        raise ValueError(
            f"filesystem export matched more than {DEFAULT_LIST_LIMIT} committed "
            "runs in the window; refusing a partial export (narrow -s/-e or "
            "--extract-run-datetime)"
        )
    want: str | None = None
    if extract_run_datetime is not None:
        want = identity_iso(extract_run_datetime)
        runs = [
            r
            for r in runs
            if _hive_run_matches(identity_iso(r["extract_run_datetime"]), want)
        ]
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
        if want is not None:
            _assert_requested_run(identity, want)
        batches.append((identity, rows))
    return batches


def _load_iceberg_batches(
    config: PipelineConfig,
    *,
    project_root: Path,
    interval_start: str,
    interval_end: str | None,
    extract_run_datetime: str | None,
    settings: DetSettings | None,
) -> list[tuple[tuple[str, str, str], list[dict[str, Any]]]]:
    schema, table = sql_names_for_config(config)
    location = bronze_dataset_dir(config, project_root, settings=settings)
    ice = iceberg_writer.load_iceberg_table(
        lake=lake_root(config.destination, project_root, settings=settings),
        namespace=schema,
        table=table,
        table_location=location,
    )
    if ice is None:
        return []
    rows = iceberg_writer.scan_iceberg_rows(
        ice,
        limit=_EXPORT_ICEBERG_LIMIT + 1,
        interval_start=interval_start,
        interval_end=interval_end,
        extract_run_datetime=extract_run_datetime,
    )
    if len(rows) > _EXPORT_ICEBERG_LIMIT:
        raise ValueError(
            f"Iceberg export matched more than {_EXPORT_ICEBERG_LIMIT} rows; "
            "refusing a partial replace-by-run batch"
        )
    if not rows:
        return []
    # Validate meta on every row, group by run, then require a single identity
    # per group — mixed runs in one window are the documented default.
    _require_bronze_meta_present(rows)
    by_run: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (
            str(row[_START]),
            str(row[_END]),
            str(row[_RUN]),
        )
        by_run.setdefault(key, []).append(row)
    batches: list[tuple[tuple[str, str, str], list[dict[str, Any]]]] = []
    for group in by_run.values():
        identity = require_bronze_run_identity(group)
        batches.append((identity, group))
    return batches


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
    dest = config.destination.type
    if dest == "filesystem":
        return _load_filesystem_batches(
            config,
            project_root=project_root,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
            settings=settings,
        )
    if dest == "iceberg":
        return _load_iceberg_batches(
            config,
            project_root=project_root,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
            settings=settings,
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
