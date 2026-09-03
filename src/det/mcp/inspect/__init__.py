"""Read-only lake inspect helpers for DET MCP (Lane A).

Public re-exports preserve ``from det.mcp.inspect import X`` and
``import det.mcp.inspect as insp`` import paths used by callers.
"""

from __future__ import annotations

from det.runtime.bronze_runs import (
    _list_bronze_iceberg_runs,
    _list_bronze_sql_runs,
)
from det.runtime.manifest import read_manifest as read_raw_manifest

from ._common import (
    DEFAULT_LIST_LIMIT,
    DEFAULT_SAMPLE_LIMIT,
    MAX_SAMPLE_LIMIT,
    MAX_WIRE_CHARS,
    RUN_KEY_FIELDS,
    SampleStage,
    _assert_under_bronze,
    _assert_under_raw,
    _load_pipeline,
    _parse_hive_key,
    _posix_parts,
    _quote_ident,
    _rel,
    _resolve_lake_run_path,
    _root,
    _run_dict,
    _run_key,
    clamp_list_limit,
    clamp_sample_limit,
    walk_hive_runs,
)
from ._diagnose import diagnose_pipeline
from ._partitions import (
    _raw_run_dir,
    diff_partitions,
    list_bronze_runs,
    resolve_raw_run,
    to_interval_or_partition,
)
from ._sample import (
    _iter_load_rows,
    _resolve_bronze_fs_run,
    _sample_bronze_duckdb,
    _sample_bronze_filesystem,
    _sample_bronze_iceberg,
    _sample_bronze_postgres,
    _sample_wire,
    _sql_sample_filters,
    collect_validation_errors,
    sample_bronze,
    sample_raw,
    validate_sample,
)

__all__ = [
    # constants
    "DEFAULT_LIST_LIMIT",
    "DEFAULT_SAMPLE_LIMIT",
    "MAX_SAMPLE_LIMIT",
    "MAX_WIRE_CHARS",
    "RUN_KEY_FIELDS",
    "SampleStage",
    # public functions
    "clamp_list_limit",
    "clamp_sample_limit",
    "collect_validation_errors",
    "diagnose_pipeline",
    "diff_partitions",
    "list_bronze_runs",
    "resolve_raw_run",
    "sample_bronze",
    "sample_raw",
    "to_interval_or_partition",
    "validate_sample",
    "walk_hive_runs",
    # runtime re-exports used by tests via monkeypatch on inspect_mod
    "read_raw_manifest",
    # private helpers (used by tools.py, tests, or airflow_inspect)
    "_assert_under_bronze",
    "_assert_under_raw",
    "_iter_load_rows",
    "_list_bronze_iceberg_runs",
    "_list_bronze_sql_runs",
    "_load_pipeline",
    "_parse_hive_key",
    "_posix_parts",
    "_quote_ident",
    "_raw_run_dir",
    "_rel",
    "_resolve_bronze_fs_run",
    "_resolve_lake_run_path",
    "_root",
    "_run_dict",
    "_run_key",
    "_sample_bronze_duckdb",
    "_sample_bronze_filesystem",
    "_sample_bronze_iceberg",
    "_sample_bronze_postgres",
    "_sample_wire",
    "_sql_sample_filters",
]
