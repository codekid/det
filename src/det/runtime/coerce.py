from __future__ import annotations

from typing import Any

from det.errors import DetContractError


class CoerceError(DetContractError, ValueError):
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


def _schema_fragment_from_prop(prop: dict[str, Any]) -> dict[str, Any]:
    """Treat a property schema as a record schema when it describes an object."""
    return {
        "type": "object",
        "properties": prop.get("properties") or {},
    }


def coerce_value(value: Any, prop: dict[str, Any], *, field: str) -> Any:
    """
    Coerce a single value toward the JSON Schema property type.

    Recurses into ``object`` / ``array`` using ``properties`` / ``items`` only
    (no ``$ref`` / ``allOf`` / ``oneOf`` resolution). Blank strings become null
    when null is allowed. Does not invent defaults for required non-null fields
    — validation catches those.
    """
    # Composition / $ref: leave as-is at this node (validate may still enforce).
    if any(k in prop for k in ("$ref", "allOf", "oneOf", "anyOf", "patternProperties")):
        return value

    allowed = _allowed_types(prop)

    if isinstance(value, dict) and (
        not allowed or "object" in allowed or "properties" in prop
    ):
        if "properties" in prop or "object" in allowed or not allowed:
            return coerce_record(value, _schema_fragment_from_prop(prop), path=field)

    if isinstance(value, list) and (not allowed or "array" in allowed or "items" in prop):
        items = prop.get("items")
        if isinstance(items, dict):
            out: list[Any] = []
            for i, item in enumerate(value):
                item_field = f"{field}[{i}]"
                item_allowed = _allowed_types(items)
                if isinstance(item, dict) and (
                    "properties" in items
                    or "object" in item_allowed
                    or not item_allowed
                ):
                    out.append(
                        coerce_record(
                            item, _schema_fragment_from_prop(items), path=item_field
                        )
                    )
                else:
                    out.append(coerce_value(item, items, field=item_field))
            return out
        return list(value)

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


def coerce_record(
    row: dict[str, Any], schema: dict[str, Any], *, path: str = ""
) -> dict[str, Any]:
    """Apply schema property types to a named row (before JSON Schema validate)."""
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        return dict(row)
    out: dict[str, Any] = {}
    for key, value in row.items():
        field = f"{path}.{key}" if path else str(key)
        prop = props.get(key)
        if not isinstance(prop, dict):
            # Unknown key: pass through; still walk nested structure without schema.
            if isinstance(value, dict):
                out[key] = coerce_record(
                    value, {"type": "object", "properties": {}}, path=field
                )
            elif isinstance(value, list):
                out[key] = [
                    coerce_record(
                        item,
                        {"type": "object", "properties": {}},
                        path=f"{field}[{i}]",
                    )
                    if isinstance(item, dict)
                    else item
                    for i, item in enumerate(value)
                ]
            else:
                out[key] = value
            continue
        out[key] = coerce_value(value, prop, field=field)
    return out


def coerce_records(
    rows: list[dict[str, Any]], schema: dict[str, Any]
) -> list[dict[str, Any]]:
    return [coerce_record(row, schema) for row in rows]
