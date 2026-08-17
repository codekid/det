"""dbt parse of the committed project (skipped without the dbt extra)."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.integration
def test_dbt_parse_committed_project(project_root: Path, tmp_path: Path):
    if shutil.which("dbt") is None:
        pytest.skip("dbt extra not installed")
    target = tmp_path / "dbt-target"
    env = os.environ.copy()
    env.setdefault("DET_LAKE_PATH", str(tmp_path / "lake"))
    env.setdefault("DET_ANALYTICS_DUCKDB", str(tmp_path / "analytics.duckdb"))
    completed = subprocess.run(
        [
            "dbt",
            "parse",
            "--project-dir",
            str(project_root / "dbt"),
            "--profiles-dir",
            str(project_root / "dbt"),
            "--target-path",
            str(target),
        ],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (target / "manifest.json").is_file()
