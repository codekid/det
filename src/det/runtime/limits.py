"""Shared list caps for partition / run enumeration."""

from __future__ import annotations

DEFAULT_LIST_LIMIT = 200


def clamp_list_limit(limit: int | None = None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    return max(1, min(int(limit), DEFAULT_LIST_LIMIT))
