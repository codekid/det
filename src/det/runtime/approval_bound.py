"""Canonical mutating CLI params bound into approval plan digests.

Source of truth for the fail-closed approval backstop. CLI ``_BOUND_PARAMS``
imports this map; ``*_write_argv`` builders in ``det.runtime.approval`` must
encode the same flags. Typer params that change a write go here; harmless flags
stay in CLI ``_NEUTRAL_PARAMS``.
"""

from __future__ import annotations

# Typer names for lake roots (layout 2 parent or split).
LAKE_LAYER_PARAMS: frozenset[str] = frozenset(
    {"lake_path", "lake_path_raw", "lake_path_bronze", "lake_path_ops"}
)

# command → Typer param names that change what/where is written.
APPROVAL_BOUND_PARAMS: dict[str, frozenset[str]] = {
    "extract": frozenset(
        {"pipeline", "interval_start", "interval_end", *LAKE_LAYER_PARAMS, "set_"}
    ),
    "load": frozenset(
        {
            "pipeline",
            "interval_start",
            "interval_end",
            "extract_run_datetime",
            *LAKE_LAYER_PARAMS,
            "set_",
        }
    ),
    "run": frozenset(
        {"pipeline", "interval_start", "interval_end", *LAKE_LAYER_PARAMS, "set_"}
    ),
    "migrate": frozenset(
        {
            "pipeline",
            "to_bronze",
            "schema",
            "mapper",
            "interval_start",
            "interval_end",
            "from_raw",
            "wire_version",
            "recreate_iceberg",
            "all_raw",
            "all_raw_runs",
            "ingestion",
            *LAKE_LAYER_PARAMS,
            "set_",
        }
    ),
    "prune": frozenset(
        {
            "pipeline",
            "interval_start",
            "interval_end",
            "keep",
            "apply",
            "set_",
            *LAKE_LAYER_PARAMS,
        }
    ),
    "dbt": frozenset(
        {
            "pipeline",
            "select",
            "command",
            "full_refresh",
            "catchup",
            "catchup_manifest",
            "target",
            *LAKE_LAYER_PARAMS,
            "set_",
        }
    ),
    "scaffold-dbt": frozenset({"pipeline", "force", "set_"}),
    "scaffold-ops": frozenset({"force"}),
    "init-pipeline": frozenset(
        {
            "name",
            "source_type",
            "destination_type",
            "connection",
            "lake_path",
            "skip_dbt",
            "force",
        }
    ),
    "biglake-register": frozenset(
        {
            *LAKE_LAYER_PARAMS,
            "pipeline",
            "project",
            "location",
            "connection",
            "skip_ops",
            "apply",
        }
    ),
    "iceberg-register": frozenset({*LAKE_LAYER_PARAMS, "pipeline", "skip_ops", "apply"}),
    "lock-release": frozenset(
        {
            "pipeline",
            "interval_start",
            "interval_end",
            "dataset_id",
            "force",
            *LAKE_LAYER_PARAMS,
        }
    ),
    "silver-catchup-plan": frozenset(
        {
            "pipeline",
            "all_pipelines",
            "interval_start",
            "interval_end",
            "extract_lookback",
            "census",
            "limit",
            "apply",
            "manifest_id",
            "content_digest",
            *LAKE_LAYER_PARAMS,
        }
    ),
    "silver-catchup-cleanup": frozenset(
        {
            "manifest_id",
            "created_before",
            "apply",
        }
    ),
}

# Bound Typer names that are CLI-only (not kwargs on the matching *_write_argv).
# Builders still imply the write; argv may hardcode the flag (e.g. always --apply).
CLI_ONLY_BOUND_PARAMS: dict[str, frozenset[str]] = {
    "prune": frozenset({"apply"}),
    "biglake-register": frozenset({"apply"}),
    "iceberg-register": frozenset({"apply"}),
    "silver-catchup-plan": frozenset({"apply"}),
    "silver-catchup-cleanup": frozenset({"apply"}),
    "lock-release": frozenset({"force"}),
}

__all__ = [
    "APPROVAL_BOUND_PARAMS",
    "CLI_ONLY_BOUND_PARAMS",
    "LAKE_LAYER_PARAMS",
]
