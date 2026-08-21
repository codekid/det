"""
Airflow DAG: extract → load → optional bronze prune.

Does not trigger dbt — silver/gold runs on ``det_dbt_silver_gold`` (separate schedule).

Requires `det` installed in the Airflow environment.

Env:
  DET_PROJECT_ROOT, DET_PIPELINE_CONFIG (canonical id or YAML path; default noaa.storm_events)
  DET_PIPELINE_OVERRIDES — comma-separated dotted.key=value (same as `det --set`)
  DET_PRUNE=1              — run prune after load (default off)
  DET_PRUNE_APPLY=1        — actually delete (otherwise dry-run plan only)
  DET_PRUNE_KEEP=1         — newest extract runs to keep per interval

Prune **apply** requires DagRun conf ``{"approval": "apr_…"}`` matching the same
argv as ``det prune … --apply`` / MCP ``prune_dry_run`` ``approval_plan`` (after
``det approve``). Scheduled extract/load never need approvals. Do not set
``DET_REQUIRE_APPROVAL=1`` on Compose for the scheduler.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task
from det_env import (
    approval_id_from_conf,
    consume_prune_approval,
    env_flag,
    gate_prune_apply_approval,
    lock_ttl_sec_from_conf,
    merge_dag_conf,
    pipeline_overrides,
    pipeline_path,
    project_root,
    set_lock_owner,
)

PROJECT_ROOT = project_root()


@dag(
    dag_id="det_extract_bronze",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["det", "raw", "bronze"],
)
def det_extract_bronze():
    @task
    def extract_raw(data_interval_start=None, data_interval_end=None) -> dict:
        from airflow.operators.python import get_current_context

        from det.runtime.runner import PipelineRunner

        context = get_current_context()
        dag_run = context.get("dag_run")
        run_id = getattr(dag_run, "run_id", "unknown")
        set_lock_owner(dag_id="det_extract_bronze", run_id=str(run_id))
        ttl = lock_ttl_sec_from_conf(getattr(dag_run, "conf", None) or {})

        if data_interval_start:
            start = data_interval_start.isoformat()
        else:
            start = datetime.utcnow().date().isoformat()
        end = data_interval_end.isoformat() if data_interval_end else None
        result = PipelineRunner(PROJECT_ROOT).extract(
            pipeline_path(),
            interval_start=start,
            interval_end=end,
            overrides=pipeline_overrides() or None,
            lock_ttl_sec=ttl,
        )
        return {
            "pipeline": result.pipeline,
            "raw_dir": str(result.raw_dir),
            "extract_run_datetime": result.extract_run_datetime,
            "interval_start": result.interval_start,
            "interval_end": result.interval_end,
            "artifacts": result.artifacts,
        }

    @task
    def load_bronze(extract_info: dict) -> dict:
        from airflow.operators.python import get_current_context

        from det.runtime.runner import PipelineRunner

        context = get_current_context()
        dag_run = context.get("dag_run")
        run_id = getattr(dag_run, "run_id", "unknown")
        set_lock_owner(dag_id="det_extract_bronze", run_id=str(run_id))
        ttl = lock_ttl_sec_from_conf(getattr(dag_run, "conf", None) or {})

        result = PipelineRunner(PROJECT_ROOT).load(
            pipeline_path(),
            interval_start=extract_info["interval_start"],
            interval_end=extract_info["interval_end"],
            extract_run_datetime=extract_info["extract_run_datetime"],
            overrides=pipeline_overrides() or None,
            lock_ttl_sec=ttl,
        )
        return {
            "pipeline": result.pipeline,
            "rows": result.rows,
            "partition": str(result.partition_dir),
            "data_interval_date": result.data_interval_date,
            "raw_dir": str(result.raw_dir) if result.raw_dir else None,
            "interval_start": extract_info["interval_start"],
            "interval_end": extract_info["interval_end"],
        }

    @task
    def prune_bronze(load_info: dict) -> dict:
        """Optional bronze retention. Disabled unless DET_PRUNE=1."""
        if not env_flag("DET_PRUNE"):
            return {"skipped": True, "reason": "DET_PRUNE not set"}

        from airflow.operators.python import get_current_context

        from det.runtime.config import load_pipeline_config
        from det.runtime.prune import BronzePruner

        keep = int(os.environ.get("DET_PRUNE_KEEP", "1"))
        apply = env_flag("DET_PRUNE_APPLY")
        config = load_pipeline_config(pipeline_path(), overrides=pipeline_overrides() or None)
        pruner = BronzePruner(PROJECT_ROOT)
        plan = pruner.plan(
            config,
            interval_start=load_info["interval_start"],
            interval_end=load_info["interval_end"],
            keep=keep,
        )
        if not apply:
            return {
                "skipped": False,
                "apply": False,
                "keep": keep,
                "would_remove": plan.remove_count,
                "removed": 0,
            }

        context = get_current_context()
        dag_run = context.get("dag_run")
        merged = merge_dag_conf(
            getattr(dag_run, "conf", None) if dag_run else None,
            context.get("params"),
        )
        approval_id = approval_id_from_conf(merged)
        gate_prune_apply_approval(
            PROJECT_ROOT,
            pipeline=config.name,
            interval_start=load_info["interval_start"],
            interval_end=load_info["interval_end"],
            keep=keep,
            approval_id=approval_id,
        )
        removed = pruner.apply(
            config,
            plan,
            interval_start=load_info["interval_start"],
            interval_end=load_info["interval_end"],
        )
        assert approval_id is not None  # gate_prune_apply_approval require=True
        consume_prune_approval(PROJECT_ROOT, approval_id)
        return {
            "skipped": False,
            "apply": True,
            "keep": keep,
            "would_remove": plan.remove_count,
            "removed": removed,
            "approval": approval_id,
        }

    extracted = extract_raw()
    loaded = load_bronze(extracted)
    prune_bronze(loaded)


det_extract_bronze()
