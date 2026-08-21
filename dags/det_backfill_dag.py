"""
Backfill driver: trigger ``det_extract_bronze`` once per day.

Manual only. Trigger with conf (or UI params), DET half-open interval ``[start, end)``
plus a single-use approval for the **window** (child extract runs stay ungated):

.. code-block:: json

   {
     "interval_start": "2026-08-01",
     "interval_end": "2026-08-08",
     "approval": "apr_…"
   }

Approve first via MCP ``preview_backfill_conf`` → ``det approve --plan`` (command
``backfill``) or ``det approve --command backfill --argv-json '[...]'``.

Each calendar day ``D`` in that range becomes one DagRun of ``det_extract_bronze``.
Airflow ``@daily`` maps ``logical_date = D + 1 day`` → data interval ``[D, D+1)``.

CLI example::

   airflow dags trigger det_backfill_extract_bronze --conf '{
     "interval_start": "2026-08-01",
     "interval_end": "2026-08-08",
     "approval": "apr_…"
   }'
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from det_env import (
    approval_id_from_conf,
    consume_backfill_approval,
    daily_logical_dates_for_interval,
    gate_backfill_approval,
    merge_dag_conf,
    project_root,
)

PROJECT_ROOT = project_root()


@dag(
    dag_id="det_backfill_extract_bronze",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["det", "backfill", "raw", "bronze"],
    params={
        "interval_start": Param(
            "",
            type="string",
            title="Interval start",
            description="Inclusive start (YYYY-MM-DD), DET [start, end)",
        ),
        "interval_end": Param(
            "",
            type="string",
            title="Interval end",
            description="Exclusive end (YYYY-MM-DD), DET [start, end)",
        ),
        "approval": Param(
            "",
            type="string",
            title="Approval id",
            description="apr_… from det approve for this backfill window",
        ),
    },
)
def det_backfill_extract_bronze():
    @task
    def build_trigger_kwargs(**context) -> list[dict]:
        conf = merge_dag_conf(
            context["dag_run"].conf or {},
            context.get("params"),
        )
        start = (conf.get("interval_start") or "").strip()
        end = (conf.get("interval_end") or "").strip()
        if not start or not end:
            raise ValueError(
                "Provide interval_start and interval_end "
                '(e.g. conf {"interval_start":"2026-08-01",'
                '"interval_end":"2026-08-08","approval":"apr_…"})'
            )

        approval_id = approval_id_from_conf(conf)
        gate_backfill_approval(
            PROJECT_ROOT,
            interval_start=start,
            interval_end=end,
            approval_id=approval_id,
        )

        run_id = context["dag_run"].run_id
        specs: list[dict] = []
        for logical in daily_logical_dates_for_interval(start, end):
            specs.append(
                {
                    "logical_date": logical,
                    "trigger_run_id": (
                        f"backfill__{run_id}__{logical.strftime('%Y%m%dT%H%M%S')}"
                    ),
                }
            )
        assert approval_id is not None
        consume_backfill_approval(PROJECT_ROOT, approval_id)
        return specs

    TriggerDagRunOperator.partial(
        task_id="trigger_extract_bronze",
        trigger_dag_id="det_extract_bronze",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    ).expand_kwargs(build_trigger_kwargs())


det_backfill_extract_bronze()
