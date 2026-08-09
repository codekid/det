from __future__ import annotations

from typing import Any


class CoerceError(ValueError):
    """Raised when a value cannot be coerced to the schema type."""


def _allowed_types(prop: dict[str, Any]) -> set[str]:
    raw = prop.get("type")
    if raw is None:
        return set()
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {t for t in raw if isinstance(t, str)}
    return set()


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def coerce_value(value: Any, prop: dict[str, Any], *, field: str) -> Any:
    """
    Coerce a single value toward the JSON Schema property type.

    Blank strings become null when null is allowed. Does not invent defaults for
    required non-null fields — validation catches those.
    """
    allowed = _allowed_types(prop)
    if not allowed:
        return value

    if _is_blank(value):
        if "null" in allowed:
            return None
        if "string" in allowed and value is not None:
            return "" if isinstance(value, str) else value
        raise CoerceError(f"{field}: empty value is not valid for types {sorted(allowed)}")

    # Prefer integer before number when both appear (unusual).
    if "integer" in allowed:
        if isinstance(value, bool):
            raise CoerceError(f"{field}: boolean is not a valid integer")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            text = value.strip()
            try:
                return int(text)
            except ValueError:
                pass
            try:
                as_float = float(text)
            except ValueError as exc:
                raise CoerceError(f"{field}: cannot coerce {value!r} to integer") from exc
            if as_float.is_integer():
                return int(as_float)
            raise CoerceError(f"{field}: cannot coerce {value!r} to integer")

    if "number" in allowed:
        if isinstance(value, bool):
            raise CoerceError(f"{field}: boolean is not a valid number")
        if isinstance(value, (int, float)):
            return float(value) if not isinstance(value, int) else value
        if isinstance(value, str):
            text = value.strip()
            try:
                # Preserve ints when the text is integral.
                if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
                    return int(text)
                return float(text)
            except ValueError as exc:
                raise CoerceError(f"{field}: cannot coerce {value!r} to number") from exc

    if "boolean" in allowed:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes"}:
                return True
            if lowered in {"false", "0", "no"}:
                return False
        raise CoerceError(f"{field}: cannot coerce {value!r} to boolean")

    if "string" in allowed:
        if isinstance(value, str):
            return value
        return str(value)

    if "null" in allowed and value is None:
        return None

    return value


def coerce_record(row: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    """Apply schema property types to a named row (before JSON Schema validate)."""
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        return dict(row)
    out: dict[str, Any] = {}
    for key, value in row.items():
        prop = props.get(key)
        if not isinstance(prop, dict):
            out[key] = value
            continue
        out[key] = coerce_value(value, prop, field=key)
    return out


def coerce_records(
    rows: list[dict[str, Any]], schema: dict[str, Any]
) -> list[dict[str, Any]]:
    return [coerce_record(row, schema) for row in rows]
