"""Refresh cached det modules for long-lived MCP server processes.

Cursor keeps the DET MCP stdio process alive across edits. Without a refresh,
imports against a stale ``sys.modules`` entry can miss new symbols even when
the file on disk is fine.
"""

from __future__ import annotations

import importlib
import sys
from typing import Final

# Reload only modules MCP tools commonly need fresh, without replacing pydantic
# model class identity (which breaks ``PipelineConfig(medallion=existing)``).
_RELOAD_MODULES: Final[tuple[str, ...]] = (
    "det.runtime.manifest",
)


def refresh_det_runtime() -> None:
    """
    Evict discovered source modules, clear registries, reload hot modules,
    and re-register builtins.

    Safe to call at the start of each MCP tool. Keeps registry / plugins /
    config class identity stable for in-process callers.
    """
    from det.runtime.discovery import evict_in_tree_plugin_modules
    from det.runtime.registry import clear_registries

    evict_in_tree_plugin_modules()
    clear_registries()

    plugs = sys.modules.get("det.plugins")
    if plugs is not None:
        plugs._LOADED = False  # pyright: ignore[reportAttributeAccessIssue]

    for name in _RELOAD_MODULES:
        mod = sys.modules.get(name)
        if mod is None:
            continue
        try:
            importlib.reload(mod)
        except Exception:
            del sys.modules[name]

    from det.plugins import load_plugins

    load_plugins()
