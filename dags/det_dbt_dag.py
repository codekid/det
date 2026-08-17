"""
Airflow DAG: dbt silver + gold in a single process.

Scheduled independently of extract — DET owns bronze validation; dbt owns
transforms. By default runs the **entire** dbt project. Narrow with
``DET_DBT_SELECT`` if needed.

One ``dbt build`` (not Cosmos model tasks): DuckDB file destinations only allow
a single writer, so parallel per-model Airflow tasks contend on
``analytics.duckdb``. This matches local ``det dbt``.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from det_env import dbt_env_for_pipeline, dbt_select, project_root

PROJECT_ROOT = project_root()
DBT_PROJECT = Path(os.environ.get("DET_DBT_PROJECT", str(PROJECT_ROOT / "dbt")))


@dag(
    dag_id="det_dbt_silver_gold",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["det", "dbt", "silver", "gold"],
)
def det_dbt_silver_gold():
    @task
    def dbt_build() -> dict:
        from det.runtime.dbt_runner import DbtNotInstalledError, run_dbt

        for key, value in dbt_env_for_pipeline().items():
            os.environ[key] = value

        select = dbt_select()
        try:
            result = run_dbt(
                project_root=PROJECT_ROOT,
                command="build",
                project_dir=DBT_PROJECT,
                select=select,
                exclude=_analytics_exclude(select),
            )
        except DbtNotInstalledError:
            raise

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt build failed with exit code {result.returncode}"
            )
        return {
            "command": result.command,
            "returncode": result.returncode,
            "select": list(result.select),
            "lake_path": result.lake_path,
            "bronze_source": result.bronze_source,
        }

    dbt_build()


def _analytics_exclude(select: list[str] | None) -> list[str] | None:
    from det.runtime.dbt_runner import analytics_exclude

    return analytics_exclude(select)


det_dbt_silver_gold()
