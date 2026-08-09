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


def apply_naming(row: dict[str, Any], naming: BronzeNamingConfig) -> dict[str, Any]:
    """Apply configured key naming. Raises ValueError if two source keys collide."""
    if naming.style == "identity":
        return dict(row)
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key is None:
            continue
        new_key = to_snake_case(str(key))
        if new_key in out:
            raise ValueError(
                f"Naming collision: multiple keys map to {new_key!r} "
                f"(duplicate includes {key!r})"
            )
        out[new_key] = value
    return out
