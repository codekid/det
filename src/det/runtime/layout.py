"""DET lake layout version — hive / SQL skeleton, not a dataset era.

``wire_version`` is payload + lake id (``{name}_vN``). ``LAKE_LAYOUT`` is the
path and naming contract every era hangs on. Additive sibling prefixes and extra
manifest keys do not bump this; renaming hive keys or SQL names does.

Published contract and changelog: ``docs/lake-layout.md``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# Max layout this install can write/read. Not package semver.
# Layout 2 only: split/derived roots (DET_LAKE_PATH or DET_LAKE_PATH_*).
LAKE_LAYOUT = 2


def lake_layout_of(payload: Mapping[str, Any] | None) -> int:
    """Layout from a manifest or receipt body. Missing or invalid ⇒ layout 2."""
    if not payload:
        return 2
    raw = payload.get("lake_layout")
    if raw is None or raw == "":
        return 2
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 2
    return value if value >= 1 else 2
