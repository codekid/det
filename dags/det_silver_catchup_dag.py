"""
Airflow reference DAG: Mode A silver catch-up (detect holes → map heal).

Tier 1: calls SemVer ``iter_silver_catchup_holes`` / ``run_silver_catchup_heal``.
Embedders should copy/adapt this file into their Airflow; DET does not ship a
``det.airflow`` package module.

Detecting holes is not a failure. Mapped heals fail only if apply/build/verify
fails. No approval gate — do not set ``DET_REQUIRE_APPROVAL=1`` on Compose for
this DAG (same as scheduled extract/dbt).

Concurrency: ``max_active_runs=1`` (one fleet run). Mapped heals use
``max_active_tis_per_dag`` (default **1** via ``DET_SILVER_CATCHUP_MAX_ACTIVE``)
so DuckDB analytics stays single-writer; raise only for BigQuery/multi-writer
warehouses. Optional pool ``DET_SILVER_CATCHUP_POOL``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from det_env import (
    project_root,
    silver_catchup_lookback,
    silver_catchup_pipeline_allowlist,
)

PROJECT_ROOT = project_root()
SCHEDULE = os.environ.get("DET_SILVER_CATCHUP_SCHEDULE", "@daily")
POOL = os.environ.get("DET_SILVER_CATCHUP_POOL", "default_pool").strip() or "default_pool"
try:
    MAX_ACTIVE = int(os.environ.get("DET_SILVER_CATCHUP_MAX_ACTIVE", "1"))
except ValueError as exc:
    raise ValueError(
        "DET_SILVER_CATCHUP_MAX_ACTIVE must be an integer >= 1"
    ) from exc
if MAX_ACTIVE < 1:
    raise ValueError(
        f"DET_SILVER_CATCHUP_MAX_ACTIVE must be an integer >= 1, got {MAX_ACTIVE}"
    )


@dag(
    dag_id="det_silver_catchup",
    start_date=datetime(2024, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["det", "silver", "catchup"],
    max_active_tasks=MAX_ACTIVE + 1,  # detect + up to MAX_ACTIVE heals
)
def det_silver_catchup():
    @task
    def detect_holes() -> list[dict[str, Any]]:
        from det import iter_silver_catchup_holes

        lookback = silver_catchup_lookback()
        allowlist = silver_catchup_pipeline_allowlist()
        holes = list(
            iter_silver_catchup_holes(
                PROJECT_ROOT,
                pipelines=allowlist,
                extract_lookback=lookback,
            )
        )
        payload = [h.to_dict() for h in holes if h.actionable]
        print(
            json.dumps(
                {
                    "lookback": lookback,
                    "allowlist": allowlist,
                    "hole_pipeline_count": len(payload),
                    "catchup_count": sum(int(p.get("catchup_count") or 0) for p in payload),
                    "max_active_tis_per_dag": MAX_ACTIVE,
                    "pool": POOL,
                    "holes": payload,
                },
                indent=2,
                default=str,
            )
        )
        return payload

    @task(
        max_active_tis_per_dag=MAX_ACTIVE,
        pool=POOL,
        pool_slots=1,
    )
    def heal_one(item: dict[str, Any]) -> dict[str, Any]:
        from det import run_silver_catchup_heal

        pipeline = str(item.get("pipeline") or "").strip()
        if not pipeline:
            raise ValueError("heal_one requires item['pipeline']")
        lookback = str(item.get("extract_lookback") or silver_catchup_lookback())
        return run_silver_catchup_heal(
            PROJECT_ROOT,
            pipeline=pipeline,
            extract_lookback=lookback,
        )

    heal_one.expand(item=detect_holes())


det_silver_catchup()
