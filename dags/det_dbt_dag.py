"""
Airflow DAG: dbt silver + gold via Astronomer Cosmos (model-level tasks).

Scheduled independently of extract — DET owns bronze validation; dbt owns transforms.
Trigger manually after a backfill if you need silver sooner than the next schedule.

Requires ``astronomer-cosmos`` and the ``[dbt]`` extra in the Airflow image.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode
from det_env import dbt_env_for_pipeline, dbt_select_for_pipeline, project_root

PROJECT_ROOT = project_root()
DBT_PROJECT = Path(os.environ.get("DET_DBT_PROJECT", str(PROJECT_ROOT / "dbt")))

_select = dbt_select_for_pipeline()
_env = dbt_env_for_pipeline()

det_dbt_silver_gold = DbtDag(
    dag_id="det_dbt_silver_gold",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT,
        env_vars=_env,
        install_dbt_deps=False,
    ),
    profile_config=ProfileConfig(
        profile_name="disaster_analytics",
        target_name="duckdb",
        profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
    ),
    render_config=RenderConfig(
        select=_select,
        load_method=LoadMode.DBT_LS,
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        invocation_mode=InvocationMode.SUBPROCESS,
    ),
    operator_args={
        "install_deps": False,
        "append_env": True,
    },
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["det", "dbt", "silver", "gold", "cosmos"],
)
