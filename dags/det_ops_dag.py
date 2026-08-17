"""
Airflow DAG: materialize run receipts → Iceberg, then dbt ops models.

Standalone from ``det_dbt_silver_gold`` (analytics) and ``det_extract_bronze``.
Uses ``DET_OPS_DUCKDB`` and ``--target ops``; never writes analytics.duckdb.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from det_env import ops_dbt_env, project_root

PROJECT_ROOT = project_root()
DBT_PROJECT = Path(os.environ.get("DET_DBT_PROJECT", str(PROJECT_ROOT / "dbt")))


@dag(
    dag_id="det_ops_receipts",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["det", "ops"],
)
def det_ops_receipts():
    @task
    def materialize_runs(data_interval_start=None, data_interval_end=None) -> dict:
        from det.runtime.lake import open_lake, pick_lake_spec
        from det.runtime.receipts_materialize import materialize_receipts

        for key, value in ops_dbt_env().items():
            os.environ[key] = value

        lake = open_lake(pick_lake_spec(cli_lake_path=None, destination_path=None), PROJECT_ROOT)
        since = None
        until = None
        if data_interval_start is not None:
            since = data_interval_start.date().isoformat()
        if data_interval_end is not None:
            until = data_interval_end.date().isoformat()
        stats = materialize_receipts(lake, since=since, until=until)
        return {
            "since": stats.since.isoformat(),
            "until": stats.until.isoformat(),
            "days_touched": stats.days_touched,
            "rows_written": stats.rows_written,
            "skipped": stats.skipped,
            "table_location": stats.table_location,
        }

    @task
    def dbt_ops_build(_materialize_info: dict) -> dict:
        from det.runtime.dbt_runner import DbtNotInstalledError, run_dbt

        for key, value in ops_dbt_env().items():
            os.environ[key] = value

        try:
            result = run_dbt(
                project_root=PROJECT_ROOT,
                command="build",
                project_dir=DBT_PROJECT,
                select=["tag:ops"],
                target="ops",
            )
        except DbtNotInstalledError:
            raise

        if result.returncode != 0:
            raise RuntimeError(
                f"dbt ops build failed with exit code {result.returncode}"
            )
        return {
            "command": result.command,
            "returncode": result.returncode,
            "select": list(result.select),
            "lake_path": result.lake_path,
        }

    dbt_ops_build(materialize_runs())


det_ops_receipts()
