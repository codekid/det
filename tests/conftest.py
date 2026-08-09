from __future__ import annotations

from pathlib import Path

import pytest

from det.plugins import load_plugins


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _plugins():
    load_plugins()
