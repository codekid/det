from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest

from det.logging import clear_secret_values
from det.plugins import load_plugins
from det.runtime.secrets import clear_secret_cache


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _snapshot_plugin_modules() -> dict[str, ModuleType]:
    from det.runtime.discovery import is_in_tree_plugin_module

    return {n: sys.modules[n] for n in list(sys.modules) if is_in_tree_plugin_module(n)}


def _restore_plugin_snapshot(snapshot: dict[str, ModuleType]) -> None:
    from det.runtime import registry as reg
    from det.runtime.discovery import (
        bind_in_tree_plugin_modules,
        evict_in_tree_plugin_modules,
    )

    evict_in_tree_plugin_modules()
    bind_in_tree_plugin_modules(snapshot)
    reg._SOURCE_REGISTRY.clear()
    identity = reg._MAPPER_REGISTRY.get(("identity", reg._GLOBAL_ROOT_KEY))
    reg._MAPPER_REGISTRY.clear()
    if identity is not None:
        reg._MAPPER_REGISTRY[("identity", reg._GLOBAL_ROOT_KEY)] = identity
    reg._MAPPERS_SCANNED.clear()


@pytest.fixture(scope="session")
def _plugin_module_snapshot() -> dict[str, ModuleType]:
    """In-tree plugin modules after collection (the classes tests patch)."""
    import importlib

    from det.runtime.discovery import iter_in_tree_source_specs

    for _plugin_id, module_name in iter_in_tree_source_specs():
        importlib.import_module(module_name)
    return _snapshot_plugin_modules()


@pytest.fixture(autouse=True)
def _plugins():
    load_plugins()


@pytest.fixture(autouse=True)
def _restore_plugin_modules(_plugin_module_snapshot, _plugins):
    """Keep SourcePlugin class identity stable after MCP refresh / eviction tests."""
    _restore_plugin_snapshot(_plugin_module_snapshot)
    yield
    _restore_plugin_snapshot(_plugin_module_snapshot)


@pytest.fixture(autouse=True)
def _isolate_approval_policy(monkeypatch: pytest.MonkeyPatch):
    """Approval enforcement must be opted into per test, not inherited.

    CI sets ``DET_REQUIRE_APPROVAL=1`` for the job, so without this the gating
    tests would silently depend on ambient env and behave differently locally.
    """
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)


@pytest.fixture(autouse=True)
def _isolate_secrets():
    """Resolved secrets are process-cached; never leak one across tests."""
    clear_secret_cache()
    clear_secret_values()
    yield
    clear_secret_cache()
    clear_secret_values()
