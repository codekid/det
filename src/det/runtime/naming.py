from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class BronzeNamingConfig(BaseModel):
    """How business-column keys are normalized before bronze validation."""

    style: Literal["snake_case", "identity"] = "snake_case"


class BronzeConfig(BaseModel):
    naming: BronzeNamingConfig = Field(default_factory=BronzeNamingConfig)


def to_snake_case(name: str) -> str:
    """
    Structural key rename: camelCase / kebab / spaces / UPPER_SNAKE → snake_case.

    Leading double-underscores (runtime meta) are preserved. Does not coerce values.
    """
    if name.startswith("__"):
        return name
    text = str(name).replace('"', "").strip()
    text = _CAMEL_BOUNDARY.sub("_", text)
    text = text.replace("-", "_").replace(" ", "_")
    text = text.lower()
    text = _NON_ALNUM.sub("_", text)
    return text.strip("_")


def _path_label(path: str) -> str:
    return path if path else "<root>"


def _rename_value(value: Any, naming: BronzeNamingConfig, *, path: str) -> Any:
    if isinstance(value, dict):
        return apply_naming(value, naming, path=path)
    if isinstance(value, list):
        out: list[Any] = []
        for i, item in enumerate(value):
            item_path = f"{path}[{i}]" if path else f"[{i}]"
            if isinstance(item, dict):
                out.append(apply_naming(item, naming, path=item_path))
            else:
                out.append(item)
        return out
    return value


def apply_naming(
    row: dict[str, Any],
    naming: BronzeNamingConfig,
    *,
    path: str = "",
) -> dict[str, Any]:
    """
    Apply configured key naming recursively through nested objects and arrays
    of objects. Raises ValueError if two keys in the same object collide.
    """
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key is None:
            continue
        raw_key = str(key)
        new_key = raw_key if naming.style == "identity" else to_snake_case(raw_key)
        if new_key in out:
            loc = _path_label(path)
            raise ValueError(
                f"Naming collision at {loc}: multiple keys map to {new_key!r} "
                f"(duplicate includes {raw_key!r})"
            )
        child_path = f"{path}.{new_key}" if path else new_key
        out[new_key] = _rename_value(value, naming, path=child_path)
    return out
