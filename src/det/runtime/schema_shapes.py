"""JSON Schema property shape classifiers (object / array / scalar).

Kernel helpers shared by bronze DDL (``sql_types``) and dbt.stg flatten planning.
Not a SemVer export — import from ``det.runtime.schema_shapes`` when needed.
"""

from __future__ import annotations

from typing import Any


def allowed_types(prop: dict[str, Any]) -> set[str]:
    """Return the set of JSON Schema ``type`` strings on a property node."""
    raw = prop.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {t for t in raw if isinstance(t, str)}
    return set()


def is_object_prop(prop: dict[str, Any]) -> bool:
    allowed = allowed_types(prop)
    if "object" in allowed:
        return True
    if "properties" in prop and "array" not in allowed:
        return True
    return False


def is_array_prop(prop: dict[str, Any]) -> bool:
    allowed = allowed_types(prop)
    if "array" in allowed:
        return True
    return "items" in prop and "object" not in allowed


def is_scalar_prop(prop: dict[str, Any]) -> bool:
    if is_array_prop(prop) or is_object_prop(prop):
        return False
    allowed = allowed_types(prop)
    if not allowed:
        return "properties" not in prop and "items" not in prop
    return bool(allowed & {"string", "integer", "number", "boolean", "null"})
