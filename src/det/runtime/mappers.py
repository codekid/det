from __future__ import annotations

from typing import Any


def identity_mapper(row: dict[str, Any]) -> dict[str, Any]:
    """
    Named bronze row is already the target shape (pass-through).

    Use when migrate only needs schema re-check / meta refresh. Fails validation
    when the named row is not yet in the target contract (use a rename mapper).
    """
    return dict(row)
