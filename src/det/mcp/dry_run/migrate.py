"""MCP migrate dry-run."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import _helpers as h


def migrate_dry_run(
    pipeline: str,
    to_bronze: str,
    schema: str,
    mapper: str,
    interval_start: str | None = None,
    *,
    interval_end: str | None = None,
    from_raw: str | None = None,
    validate_limit: int = h.MAX_SAMPLE_LIMIT,
    confirm_full_validate: bool = False,
    wire_version: int | None = None,
    recreate_iceberg: bool = False,
    all_raw: bool = False,
    all_raw_runs: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview det migrate: parse/map/validate raw partitions; never writes bronze."""
    h.prepare_tool()
    from det.mcp.inspect._common import resolve_migrate_validate_limit
    from det.runtime.approval import migrate_write_argv
    from det.runtime.full_validate import assert_full_validate_allowed
    from det.runtime.migrate import (
        DEFAULT_MIGRATE_VALIDATE_MAX_ROWS,
        BronzeMigrator,
        MigratePlan,
    )
    from det.runtime.pipelines import resolve_pipeline_ref

    if all_raw:
        if interval_start is not None or interval_end is not None:
            raise ValueError("--all-raw cannot be combined with interval_start/end")
        if not recreate_iceberg:
            raise ValueError("--all-raw requires recreate_iceberg")
    elif interval_start is None:
        raise ValueError("interval_start is required unless all_raw")

    base = h.root(root)
    resolved_limit = resolve_migrate_validate_limit(validate_limit)
    validate_max_rows: int | None = None
    if resolved_limit is None:
        assert_full_validate_allowed(confirm=confirm_full_validate)
        validate_max_rows = DEFAULT_MIGRATE_VALIDATE_MAX_ROWS
    resolved = resolve_pipeline_ref(pipeline, project_root=base)
    schema_path = Path(schema)
    if not schema_path.is_absolute():
        schema_path = base / schema_path
    plan = BronzeMigrator(base).migrate(
        pipeline=resolved.path,
        to_bronze=to_bronze,
        schema_path=schema_path,
        mapper_name=mapper,
        interval_start=interval_start,
        interval_end=interval_end,
        from_raw=from_raw,
        dry_run=True,
        validate_limit=resolved_limit,
        validate_max_rows=validate_max_rows,
        wire_version=wire_version,
        recreate_iceberg=recreate_iceberg,
        all_raw=all_raw,
        all_raw_runs=all_raw_runs,
    )
    if not isinstance(plan, MigratePlan):
        raise TypeError(f"expected MigratePlan, got {type(plan).__name__}")
    out = plan.to_dict()
    out["validate_limit"] = validate_limit
    if confirm_full_validate:
        out["confirm_full_validate"] = True
    if validate_max_rows is not None:
        out["validate_max_rows"] = validate_max_rows
    out["pipeline"] = resolved.canonical_id
    out["approval_plan"] = h.approval_plan(
        "migrate",
        migrate_write_argv(
            resolved.canonical_id,
            to_bronze,
            schema,
            mapper,
            interval_start,
            interval_end=interval_end,
            from_raw=from_raw,
            wire_version=wire_version,
            recreate_iceberg=recreate_iceberg,
            all_raw=all_raw,
            all_raw_runs=all_raw_runs,
            **h.approval_lake_kwargs(project_root=base),
        ),
    )
    bits: list[str] = []
    if recreate_iceberg:
        bits.append(" --recreate-iceberg")
    if all_raw:
        bits.append(" --all-raw")
    if all_raw_runs:
        bits.append(" --all-raw-runs")
    flag_bit = "".join(bits)
    if all_raw:
        scope = ""
    else:
        scope = f" -s {interval_start}"
    out["note"] = (
        "Dry-run only — no bronze written. Apply with "
        f"`det migrate -p {resolved.canonical_id} --to-bronze {to_bronze} "
        f"--schema {schema} --mapper {mapper}{scope}{flag_bit}` after user confirms."
    )
    return out
