"""MCP ops/observability tools: receipts, approvals, check, Cube, analytics query."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.mcp import _helpers as h
from det.mcp.context import resolve_under_root

_RECEIPT_SECRET_KEYS = frozenset(
    {
        "connection",
        "password",
        "dsn",
        "secret",
        "token",
        "api_key",
        "apikey",
    }
)
_RECEIPT_NOTE = (
    "Receipts are observability for extract/load attempts. "
    "meta/manifest.json is the authority for landed partitions."
)


def _runs_lake(pipeline: str | None, root: Path):
    from det.destinations.models import lake_root
    from det.logging import sanitize_lake_uri
    from det.runtime.config import load_pipeline_config
    from det.runtime.lake import open_lake, pick_lake_spec
    from det.runtime.pipelines import resolve_pipeline_ref

    if pipeline:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        resolve_under_root(resolved.path, root=root)
        config = load_pipeline_config(resolved.path)
        lake = lake_root(config.destination, root)
        return lake, config.name, sanitize_lake_uri(str(lake))
    spec = pick_lake_spec(destination_path=None)
    lake = open_lake(spec, root)
    return lake, None, sanitize_lake_uri(str(lake))


def _public_receipt(row: dict[str, Any], *, root: Path) -> dict[str, Any]:
    from det.logging import sanitize_lake_uri

    out = {
        key: value
        for key, value in row.items()
        if key.lower() not in _RECEIPT_SECRET_KEYS
        and not key.lower().endswith(("_password", "_secret", "_token", "_dsn", "_connection"))
    }
    path = out.get("path")
    if isinstance(path, str):
        if "://" in path:
            out["path"] = sanitize_lake_uri(path)
        else:
            try:
                out["path"] = h.rel(Path(path), root)
            except Exception:
                out["path"] = path
    return out


def list_runs(
    pipeline: str | None = None,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    command: str | None = None,
    limit: int = h.DEFAULT_LIST_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    List extract/load run receipts (observability).

    Manifest remains the authority for landed partitions. Never returns a
    destination connection.
    """
    h.prepare_tool()
    from det.mcp.inspect import clamp_list_limit
    from det.runtime.receipts import list_receipts

    base = h.root(root)
    lake, pipe_id, lake_display = _runs_lake(pipeline, base)
    capped = clamp_list_limit(limit)
    rows = list_receipts(
        lake,
        pipeline=pipe_id,
        since=since,
        until=until,
        status=status,
        command=command,
        limit=capped,
    )
    public = [_public_receipt(row, root=base) for row in rows]
    return {
        "pipeline": pipe_id,
        "lake": lake_display,
        "limit": capped,
        "truncated": len(rows) >= capped,
        "note": _RECEIPT_NOTE,
        "runs": public,
    }


def summarize_runs(
    pipeline: str | None = None,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    command: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Summarize extract/load run receipts (counts, error codes, p50/p95).

    Numbers only — no SLO thresholds. Manifest remains the data authority.
    """
    h.prepare_tool()
    from det.runtime.receipts import summarize_receipts

    base = h.root(root)
    lake, pipe_id, lake_display = _runs_lake(pipeline, base)
    payload = summarize_receipts(
        lake,
        pipeline=pipe_id,
        since=since,
        until=until,
        status=status,
        command=command,
    )
    payload["pipeline"] = pipe_id
    payload["lake"] = lake_display
    payload["note"] = _RECEIPT_NOTE
    return payload


def list_models(*, root: Path | None = None) -> dict[str, Any]:
    """List dbt models (stg/silver/gold/ops) from dbt/models YAML + SQL."""
    h.prepare_tool()
    from det.mcp.catalog import list_dbt_models

    return list_dbt_models(root=h.root(root))


def describe_model(name: str, *, root: Path | None = None) -> dict[str, Any]:
    """Describe one dbt model: schema, grain, columns from YAML."""
    h.prepare_tool()
    from det.mcp.catalog import describe_dbt_model

    return describe_dbt_model(name, root=h.root(root))


def query_analytics(
    sql: str,
    *,
    warehouse: str = "analytics",
    limit: int = h.DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Capped read-only SELECT on analytics or ops DuckDB (not certified metrics)."""
    h.prepare_tool()
    from det.mcp.query_sql import Warehouse
    from det.mcp.query_sql import query_analytics as run_query

    if warehouse not in {"analytics", "ops"}:
        return {
            "ok": False,
            "error": "invalid_warehouse",
            "detail": "warehouse must be analytics or ops",
            "rows": [],
        }
    wh: Warehouse = "ops" if warehouse == "ops" else "analytics"
    return run_query(sql, warehouse=wh, limit=limit, root=h.root(root))


def cube_meta(*, root: Path | None = None) -> dict[str, Any]:
    """Cube Core meta (cubes/measures/dimensions). Start Cube with make cube-up."""
    h.prepare_tool()
    from det.mcp.cube_client import cube_meta as fetch_meta

    return fetch_meta(root=h.root(root))


def cube_load(
    measures: list[str],
    *,
    dimensions: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    limit: int = h.DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Run a Cube REST load query (certified gold/ops metrics)."""
    h.prepare_tool()
    from det.mcp.cube_client import cube_load as run_load

    return run_load(
        measures=measures,
        dimensions=dimensions,
        filters=filters,
        limit=limit,
        root=h.root(root),
    )


def check(
    pipeline: str | None = None,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Pipeline structure check (schema file, source plugin, optional dbt models).

    Same payload as ``det check --json``. Never writes; not a substitute for
    extract/load.
    """
    h.prepare_tool()
    from det.runtime.check import findings_payload
    from det.scaffold.check_dbt import check_project_with_dbt

    base = h.root(root)
    findings = check_project_with_dbt(base, pipeline=pipeline)
    return findings_payload(findings)


def list_approvals(
    status: str | None = None,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Approval records (MCP never creates these).

    Defaults to unused, unexpired. Pass ``status="claimed"`` to find an approval
    left stuck by a crashed run — claimed records never expire, so they do not
    appear in the default listing. Records live on the lake ops root (or
    Postgres when ``DET_APPROVAL_BACKEND=postgres``).
    """
    h.prepare_tool()
    from det.runtime.approval import list_approval_records

    valid = {"unused", "claimed", "consumed", "expired", "all"}
    wanted = (status or "unused").strip().lower()
    if wanted not in valid:
        raise ValueError(f"status must be one of {sorted(valid)}, got {status!r}")

    base = h.root(root)
    statuses = None if wanted == "all" else (wanted,)
    return {
        "project_root": str(base),
        "status": wanted,
        "approvals": list_approval_records(base, statuses=statuses),
    }


def describe_approval(approval_id: str, *, root: Path | None = None) -> dict[str, Any]:
    """Load one approval record; expired + heartbeat triage derived at read time."""
    h.prepare_tool()
    from det.runtime.approval import ApprovalError, describe_approval_record

    base = h.root(root)
    try:
        return describe_approval_record(base, approval_id)
    except ApprovalError as exc:
        if exc.code == "approval_not_found":
            raise FileNotFoundError(str(exc)) from exc
        raise
