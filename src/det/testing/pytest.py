"""Optional pytest fixtures for DET plugin tests.

Enable in your ``conftest.py``::

    pytest_plugins = ["det.testing.pytest"]

Core helpers in ``det.testing`` stay framework-neutral; this module imports
pytest and is skipped if pytest is not installed.
"""

from __future__ import annotations

from pathlib import Path

import pytest  # type: ignore[import-not-found]

from det.testing.project import TestProject
from det.testing.registry import isolated_registries

pytest_plugins: list[str] = []


@pytest.fixture
def test_project(tmp_path: Path) -> TestProject:
    """Fresh ``TestProject`` rooted at pytest's ``tmp_path``."""
    return TestProject(tmp_path)


@pytest.fixture(autouse=True)
def _det_testing_isolate_registries():
    """Autouse registry + secret-cache isolation for plugin author suites."""
    with isolated_registries():
        yield
