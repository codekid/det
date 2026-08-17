from __future__ import annotations

from pathlib import Path

import pytest

from det.logging import clear_secret_values
from det.plugins import load_plugins
from det.runtime.secrets import clear_secret_cache


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _plugins():
    load_plugins()


@pytest.fixture(autouse=True)
def _isolate_secrets():
    """Resolved secrets are process-cached; never leak one across tests."""
    clear_secret_cache()
    clear_secret_values()
    yield
    clear_secret_cache()
    clear_secret_values()
