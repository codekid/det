"""
Backfill driver: trigger ``det_extract_bronze`` once per day.

Manual only. Trigger with conf (or UI params), DET half-open interval ``[start, end)``:

.. code-block:: json

   {
     "interval_start": "2026-08-01",
     "interval_end": "2026-08-08"
   }

Each calendar day ``D`` in that range becomes one DagRun of ``det_extract_bronze``.
Airflow ``@daily`` maps ``logical_date = D + 1 day`` → data interval ``[D, D+1)``.

CLI example::

   airflow dags trigger det_backfill_extract_bronze --conf '{
     "interval_start": "2026-08-01",
     "interval_end": "2026-08-08"
   }'
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from det_env import daily_logical_dates_for_interval


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
    },
)
def det_backfill_extract_bronze():
    @task
    def build_trigger_kwargs(**context) -> list[dict]:
        conf = dict(context.get("params") or {})
        conf.update(context["dag_run"].conf or {})
        start = (conf.get("interval_start") or "").strip()
        end = (conf.get("interval_end") or "").strip()
        if not start or not end:
            raise ValueError(
                "Provide interval_start and interval_end "
                '(e.g. conf {"interval_start":"2026-08-01","interval_end":"2026-08-08"})'
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
        return specs

    TriggerDagRunOperator.partial(
        task_id="trigger_extract_bronze",
        trigger_dag_id="det_extract_bronze",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    ).expand_kwargs(build_trigger_kwargs())


det_backfill_extract_bronze()
