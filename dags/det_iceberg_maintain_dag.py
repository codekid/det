"""
Airflow DAG: plan Iceberg maintain, then map submit one table at a time.

Calls ``iter_iceberg_maintain_plans`` (SemVer). Does **not** run Spark/Athena
itself. Optional ``DET_ICEBERG_MAINTAIN_SUBMIT`` is ``module:function`` that
receives **one** actionable plan dict per mapped task.

Concurrency: mapped submits use ``max_active_tis_per_dag`` (default 4 via
``DET_ICEBERG_MAINTAIN_MAX_ACTIVE``) and optional Airflow pool
``DET_ICEBERG_MAINTAIN_POOL`` (default ``default_pool``; create a dedicated
``iceberg_maintain`` pool in prod).

Without a submit hook, only ``build_plans`` runs (logs and succeeds).
Hadoop / unset catalog → plans with ``actionable=False`` (not mapped).
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from det_env import project_root

PROJECT_ROOT = project_root()
SCHEDULE = os.environ.get("DET_ICEBERG_MAINTAIN_SCHEDULE", "@weekly")
SUBMIT_SPEC = os.environ.get("DET_ICEBERG_MAINTAIN_SUBMIT", "").strip()
POOL = os.environ.get("DET_ICEBERG_MAINTAIN_POOL", "default_pool").strip() or "default_pool"
try:
    MAX_ACTIVE = int(os.environ.get("DET_ICEBERG_MAINTAIN_MAX_ACTIVE", "4"))
except ValueError as exc:
    raise ValueError(
        "DET_ICEBERG_MAINTAIN_MAX_ACTIVE must be an integer >= 1"
    ) from exc
if MAX_ACTIVE < 1:
    raise ValueError(
        f"DET_ICEBERG_MAINTAIN_MAX_ACTIVE must be an integer >= 1, got {MAX_ACTIVE}"
    )


def _load_submit() -> Any:
    if not SUBMIT_SPEC:
        raise RuntimeError(
            "DET_ICEBERG_MAINTAIN_SUBMIT is unset; mapped submit tasks should not run"
        )
    if ":" not in SUBMIT_SPEC:
        raise ValueError(
            "DET_ICEBERG_MAINTAIN_SUBMIT must be module:function, "
            f"got {SUBMIT_SPEC!r}"
        )
    mod_name, _, func_name = SUBMIT_SPEC.partition(":")
    mod = importlib.import_module(mod_name.strip())
    fn = getattr(mod, func_name.strip())
    if not callable(fn):
        raise TypeError(f"{SUBMIT_SPEC} is not callable")
    return fn


@dag(
    dag_id="det_iceberg_maintain",
    start_date=datetime(2024, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    tags=["det", "iceberg"],
    max_active_tasks=MAX_ACTIVE + 1,  # build_plans + up to MAX_ACTIVE submits
)
def det_iceberg_maintain():
    @task
    def build_plans() -> list[dict[str, Any]]:
        from det import iter_iceberg_maintain_plans

        plans = list(iter_iceberg_maintain_plans(PROJECT_ROOT))
        payload = [p.to_dict() for p in plans]
        actionable = [p for p in payload if p.get("actionable")]
        skipped = [p for p in payload if not p.get("actionable")]
        print(
            json.dumps(
                {
                    "pipeline_count": len(payload),
                    "actionable_count": len(actionable),
                    "skipped_count": len(skipped),
                    "submit_configured": bool(SUBMIT_SPEC),
                    "max_active_tis_per_dag": MAX_ACTIVE,
                    "pool": POOL,
                    "plans": payload,
                },
                indent=2,
                default=str,
            )
        )
        if not SUBMIT_SPEC:
            print(
                "DET_ICEBERG_MAINTAIN_SUBMIT unset — plan-only run "
                "(no mapped submit tasks)"
            )
            return []
        return actionable

    @task(
        max_active_tis_per_dag=MAX_ACTIVE,
        pool=POOL,
        pool_slots=1,
    )
    def submit_one(plan: dict[str, Any]) -> dict[str, Any]:
        submit = _load_submit()
        submit(plan)
        return {
            "pipeline": plan.get("pipeline"),
            "sql_relation": plan.get("sql_relation"),
            "submitted": True,
        }

    actionable = build_plans()
    submit_one.expand(plan=actionable)


det_iceberg_maintain()
