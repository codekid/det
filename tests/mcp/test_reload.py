from __future__ import annotations

import sys

from det.mcp.reload import refresh_det_runtime
from det.runtime.registry import list_sources


def test_refresh_reloads_manifest_and_reregisters_plugins():
    """Simulate long-lived MCP: cached manifest without a new symbol, then refresh."""
    import det.runtime.manifest as manifest_mod

    # Pretend the process loaded an older manifest (no stamp helper).
    if hasattr(manifest_mod, "stamp_validation_success"):
        delattr(manifest_mod, "stamp_validation_success")
    assert not hasattr(sys.modules["det.runtime.manifest"], "stamp_validation_success")

    refresh_det_runtime()

    from det.runtime.manifest import stamp_validation_success
    from det.runtime.migrate import BronzeMigrator

    assert callable(stamp_validation_success)
    assert BronzeMigrator is not None
    assert "example_api.events" in list_sources()
