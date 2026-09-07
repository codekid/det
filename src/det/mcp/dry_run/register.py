"""MCP BigLake / Iceberg register dry-runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import _helpers as h


def biglake_register_dry_run(
    *,
    pipeline: str | None = None,
    lake_path: str | None = None,
    project: str | None = None,
    location: str | None = None,
    connection: str | None = None,
    skip_ops: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview BigLake registration plan (never creates BQ resources)."""
    h.prepare_tool()
    from det.runtime.biglake_register import (
        biglake_register_write_argv,
        build_biglake_register_plan,
        build_iam_hint,
    )

    base = h.root(root)
    pipe_path = None
    pipe_id = None
    if pipeline:
        _, pipe_path = h.load_pipeline(pipeline, base)
        pipe_id = h.canonical_id(pipeline, base)
    argv = biglake_register_write_argv(
        lake_path=lake_path,
        pipeline=pipe_id,
        project=project,
        location=location,
        connection=connection,
        skip_ops=skip_ops,
    )
    plan = build_biglake_register_plan(
        project_root=base,
        lake_path=lake_path,
        pipeline=pipe_path,
        project=project,
        location=location,
        connection=connection,
        include_ops=not skip_ops and pipeline is None,
    )
    return {
        **plan.to_dict(),
        "iam_hint": build_iam_hint(plan),
        "approval_plan": h.approval_plan("biglake-register", argv),
        "note": (
            "Dry-run only — no BigLake tables created. Operator: det approve --plan "
            "<approval_plan> --approved-by <id>. Agent: det biglake-register --apply "
            "--approval <id> in a later turn."
        ),
    }


def iceberg_register_dry_run(
    *,
    pipeline: str | None = None,
    lake_path: str | None = None,
    skip_ops: bool = False,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview Iceberg REST/Glue registration plan (never mutates the catalog)."""
    h.prepare_tool()
    from det.runtime.iceberg_register import (
        build_iceberg_register_plan,
        iceberg_register_write_argv,
        with_catalog_target_argv,
    )

    base = h.root(root)
    pipe_path = None
    pipe_id = None
    if pipeline:
        _, pipe_path = h.load_pipeline(pipeline, base)
        pipe_id = h.canonical_id(pipeline, base)
    plan = build_iceberg_register_plan(
        project_root=base,
        lake_path=lake_path,
        pipeline=pipe_path,
        include_ops=not skip_ops and pipeline is None,
    )
    argv = with_catalog_target_argv(
        iceberg_register_write_argv(
            lake_path=lake_path,
            pipeline=pipe_id,
            skip_ops=skip_ops,
        ),
        plan,
    )
    return {
        **plan.to_dict(),
        "approval_plan": h.approval_plan("iceberg-register", argv),
        "note": (
            "Dry-run only — no catalog register. Operator: det approve --plan "
            "<approval_plan> --approved-by <id>. Agent: det iceberg-register --apply "
            "--approval <id> in a later turn."
        ),
    }
