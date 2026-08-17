"""
Manual DAG: show or force-clear a DET lake lease.

Does not kill the worker. Only delete the lock after the DagRun/CLI is dead.

Trigger with conf::

   {
     "pipeline": "noaa.storm_events",
     "interval_start": "2026-08-15",
     "interval_end": "2026-08-16",
     "force": true
   }
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from det_env import pipeline_ref, project_root

PROJECT_ROOT = project_root()


@dag(
    dag_id="det_clear_lock",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["det", "ops", "lock"],
    params={
        "pipeline": Param(
            "",
            type="string",
            title="Pipeline",
            description="Canonical id (default DET_PIPELINE_CONFIG)",
        ),
        "interval_start": Param(
            "",
            type="string",
            title="Interval start",
        ),
        "interval_end": Param(
            "",
            type="string",
            title="Interval end (optional; default start+1 day)",
        ),
        "force": Param(
            False,
            type="boolean",
            title="Force delete",
            description="Must be true to delete. Worker must already be dead.",
        ),
    },
)
def det_clear_lock():
    @task
    def clear_lock(**context) -> dict:
        import shutil
        import subprocess
        import sys

        det_bin = shutil.which("det")
        if det_bin is None:
            candidate = Path(sys.executable).parent / "det"
            det_bin = str(candidate) if candidate.is_file() else None
        if det_bin is None:
            raise RuntimeError("det CLI not found on PATH")

        conf = dict(context.get("params") or {})
        conf.update(context["dag_run"].conf or {})
        pipeline = (conf.get("pipeline") or pipeline_ref()).strip()
        start = (conf.get("interval_start") or "").strip()
        end = (conf.get("interval_end") or "").strip() or None
        force = conf.get("force") in {True, "true", "True", "1", 1}
        if not start:
            raise ValueError("interval_start is required")

        show_cmd = [
            det_bin,
            "lock-show",
            "-p",
            pipeline,
            "-s",
            start,
            "--project-root",
            str(PROJECT_ROOT),
        ]
        if end:
            show_cmd.extend(["-e", end])
        shown = subprocess.run(show_cmd, check=False, capture_output=True, text=True)
        out = {
            "pipeline": pipeline,
            "interval_start": start,
            "interval_end": end,
            "show_stdout": shown.stdout,
            "show_stderr": shown.stderr,
            "force": force,
            "released": False,
        }
        if shown.returncode != 0:
            raise RuntimeError(shown.stderr or shown.stdout or "lock-show failed")
        if not force:
            return out
        rel_cmd = [
            det_bin,
            "lock-release",
            "-p",
            pipeline,
            "-s",
            start,
            "--force",
            "--project-root",
            str(PROJECT_ROOT),
        ]
        if end:
            rel_cmd.extend(["-e", end])
        released = subprocess.run(rel_cmd, check=False, capture_output=True, text=True)
        out["release_stdout"] = released.stdout
        out["release_stderr"] = released.stderr
        if released.returncode != 0:
            raise RuntimeError(released.stderr or released.stdout or "lock-release failed")
        out["released"] = True
        return out

    clear_lock()


det_clear_lock()
