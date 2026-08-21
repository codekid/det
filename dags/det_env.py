"""Shared env helpers for DET Airflow DAGs (not a DAG module)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path


def project_root() -> Path:
    return Path(os.environ.get("DET_PROJECT_ROOT", "/opt/det"))


def pipeline_ref() -> str:
    return os.environ.get("DET_PIPELINE_CONFIG", "noaa.storm_events")


def lock_ttl_sec_from_conf(conf: Mapping[str, object] | None) -> int | None:
    if not conf:
        return None
    raw = conf.get("lock_ttl_sec")
    if raw is None or str(raw).strip() == "":
        return None
    value = int(raw)
    if value < 1:
        raise ValueError("lock_ttl_sec must be >= 1")
    return value


def set_lock_owner(*, dag_id: str, run_id: str) -> None:
    os.environ["DET_LOCK_OWNER"] = f"airflow:{dag_id}:{run_id}"


def pipeline_path() -> Path:
    from det.runtime.pipelines import resolve_pipeline_ref

    return resolve_pipeline_ref(pipeline_ref(), project_root=project_root()).path


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def pipeline_overrides() -> list[str]:
    """Parse DET_PIPELINE_OVERRIDES as comma/newline-separated `key=value` (CLI --set)."""
    raw = os.environ.get("DET_PIPELINE_OVERRIDES", "")
    if not raw.strip():
        return []
    return [part.strip() for part in raw.replace("\n", ",").split(",") if part.strip()]


def dbt_select_for_pipeline() -> list[str]:
    """
    Pipeline-scoped select (``stg_{slug}+``), same as ``det dbt -p …``.

    Prefer :func:`dbt_select` for the nightly dbt DAG (full project by default).
    """
    from det.runtime.config import load_pipeline_config
    from det.runtime.dbt_runner import default_select_for_pipeline

    config = load_pipeline_config(
        pipeline_path(), overrides=pipeline_overrides() or None
    )
    return default_select_for_pipeline(config)


def dbt_select() -> list[str] | None:
    """
    Optional dbt ``--select`` from ``DET_DBT_SELECT``.

    Unset or empty → ``None`` (run the entire dbt project). Otherwise space- or
    comma-separated selectors, e.g. ``stg_noaa__storm_events+ stg_noaa__fatalities+``.
    """
    raw = os.environ.get("DET_DBT_SELECT", "").strip()
    if not raw:
        return None
    return [part.strip() for part in raw.replace(",", " ").split() if part.strip()]


def dbt_env_for_pipeline() -> dict[str, str]:
    """Env dbt needs to read DET bronze (lake path + SQL schema identity)."""
    from det.destinations.models import lake_root
    from det.runtime.config import load_pipeline_config
    from det.runtime.ids import sql_names_for_config

    root = project_root()
    config = load_pipeline_config(
        pipeline_path(), overrides=pipeline_overrides() or None
    )
    sql_schema, _ = sql_names_for_config(config)
    lake = os.environ.get("DET_LAKE_PATH")
    if not lake:
        lake = str(lake_root(config.destination, root))
    if config.destination.type == "duckdb":
        bronze_source = "duckdb"
    elif config.destination.type == "iceberg":
        bronze_source = "iceberg"
    else:
        bronze_source = "filesystem"
    analytics_db = os.environ.get("DET_ANALYTICS_DUCKDB")
    if not analytics_db:
        analytics_db = str((root / "data" / "analytics.duckdb").resolve())
    return {
        "DET_LAKE_PATH": lake,
        "DET_BRONZE_SOURCE": os.environ.get("DET_BRONZE_SOURCE", bronze_source),
        "DET_BRONZE_SCHEMA": os.environ.get("DET_BRONZE_SCHEMA", sql_schema),
        # Prefer absolute so profiles.yml does not depend on task cwd.
        "DET_ANALYTICS_DUCKDB": analytics_db,
    }


def ops_dbt_env() -> dict[str, str]:
    """Env for ops dbt target (receipts Iceberg → DET_OPS_DUCKDB)."""
    root = project_root()
    lake = os.environ.get("DET_LAKE_PATH")
    if not lake:
        lake = str((root / "data" / "lake").resolve())
    ops_db = os.environ.get("DET_OPS_DUCKDB")
    if not ops_db:
        ops_db = str((root / "data" / "det_ops.duckdb").resolve())
    return {
        "DET_LAKE_PATH": lake,
        "DET_OPS_DUCKDB": ops_db,
    }


def daily_logical_dates_for_interval(
    interval_start: str, interval_end: str
) -> list[datetime]:
    """Map DET ``[interval_start, interval_end)`` to Airflow ``@daily`` logical dates.

    For each day ``D`` in the half-open range, the daily timetable uses
    ``logical_date = D + 1 day`` so ``data_interval`` is ``[D, D+1)``.
    """
    start = date.fromisoformat(interval_start.strip()[:10])
    end = date.fromisoformat(interval_end.strip()[:10])
    if end <= start:
        raise ValueError(
            f"interval_end ({end.isoformat()}) must be after "
            f"interval_start ({start.isoformat()})"
        )
    out: list[datetime] = []
    day = start
    while day < end:
        out.append(
            datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)
        )
        day += timedelta(days=1)
    return out


def merge_dag_conf(
    conf: Mapping[str, object] | None = None,
    params: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Params then dag_run.conf (conf wins), same order as backfill."""
    merged: dict[str, object] = {}
    if params:
        merged.update(dict(params))
    if conf:
        merged.update(dict(conf))
    return merged


def approval_id_from_conf(conf: Mapping[str, object] | None) -> str | None:
    """Read ``approval`` or ``approval_id`` from DagRun conf / params."""
    if not conf:
        return None
    for key in ("approval", "approval_id"):
        raw = conf.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return None


def gate_prune_apply_approval(
    project_root: Path,
    *,
    pipeline: str,
    interval_start: str,
    interval_end: str | None,
    keep: int,
    approval_id: str | None,
) -> None:
    """
    Require a valid unused approval matching ``det prune … --apply`` argv.

    Always ``require=True`` — prune-apply in Airflow never runs without an id.
    """
    from det.runtime.approval import (
        ApprovalError,
        check_approval,
        prune_write_argv,
    )

    argv = prune_write_argv(
        pipeline,
        interval_start,
        interval_end=interval_end,
        keep=keep,
    )
    try:
        check_approval(
            project_root,
            "prune",
            argv,
            approval_id,
            require=True,
        )
    except ApprovalError as exc:
        raise ValueError(f"{exc.code}: {exc}") from exc


def consume_prune_approval(project_root: Path, approval_id: str) -> None:
    """Mark prune-apply approval consumed after a successful apply."""
    from det.runtime.approval import ApprovalError, consume_approval

    try:
        consume_approval(project_root, approval_id)
    except ApprovalError as exc:
        raise ValueError(f"{exc.code}: {exc}") from exc


def gate_backfill_approval(
    project_root: Path,
    *,
    interval_start: str,
    interval_end: str,
    approval_id: str | None,
) -> None:
    """
    Require a valid unused approval matching the backfill window argv.

    Always ``require=True`` — manual backfill never opens without an id.
    Child ``det_extract_bronze`` runs stay approval-free.
    """
    from det.runtime.approval import (
        ApprovalError,
        backfill_write_argv,
        check_approval,
    )

    argv = backfill_write_argv(interval_start, interval_end)
    try:
        check_approval(
            project_root,
            "backfill",
            argv,
            approval_id,
            require=True,
        )
    except ApprovalError as exc:
        raise ValueError(f"{exc.code}: {exc}") from exc


def consume_backfill_approval(project_root: Path, approval_id: str) -> None:
    """Mark backfill-window approval consumed after trigger specs are built."""
    from det.runtime.approval import ApprovalError, consume_approval

    try:
        consume_approval(project_root, approval_id)
    except ApprovalError as exc:
        raise ValueError(f"{exc.code}: {exc}") from exc
