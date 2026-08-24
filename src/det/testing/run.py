"""Filesystem extract→load smoke helper."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from det.runtime.runner import RunResult
from det.runtime.settings import SecretLookup
from det.testing.project import TestProject


def run_extract_load(
    project: TestProject | Path,
    pipeline: str,
    *,
    interval_start: str = "2026-08-06",
    interval_end: str | None = None,
    secrets: Mapping[str, str | None] | SecretLookup | None = None,
    lock: bool = False,
    overrides: Sequence[str] | None = None,
) -> RunResult:
    """
    Run extract+load on a filesystem lake under *project*.

    *pipeline* is a canonical id (``provider.source``), slash form, or path
    under ``configs/pipelines`` (same resolution as ``PipelineRunner``).
    """
    proj = project if isinstance(project, TestProject) else TestProject(project)
    runner = proj.runner(secrets=secrets, lock=lock)
    return runner.run(
        pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        overrides=overrides,
    )
