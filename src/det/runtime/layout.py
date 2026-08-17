"""DET lake layout version — hive / SQL skeleton, not a dataset era.

``wire_version`` is payload + lake id (``{name}_vN``). ``LAKE_LAYOUT`` is the
path and naming contract every era hangs on. Additive sibling prefixes and extra
manifest keys do not bump this; renaming hive keys or SQL names does.

Published contract and changelog: ``docs/lake-layout.md``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# Current on-disk contract (raw hive, SQL names, sibling prefixes). Not package semver.
LAKE_LAYOUT = 1


def lake_layout_of(payload: Mapping[str, Any] | None) -> int:
    """Layout from a manifest or receipt body. Missing or invalid ⇒ layout 1."""
    if not payload:
        return LAKE_LAYOUT
    raw = payload.get("lake_layout")
    if raw is None or raw == "":
        return LAKE_LAYOUT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return LAKE_LAYOUT
    return value if value >= 1 else LAKE_LAYOUT
