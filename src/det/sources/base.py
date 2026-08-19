from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

from det.runtime.lake import LakeRef

MAPPER_ATTR = "__det_mapper__"

_F = TypeVar("_F", bound=Callable[..., Any])


@dataclass
class Interval:
    """Normalized extraction window: ISO 8601 UTC, end exclusive."""

    start: str
    end: str

    @property
    def date(self) -> str:
        return self.start[:10]


@dataclass
class SourceRow:
    """One source-native row prior to naming / schema validation / meta."""

    data: dict[str, Any]
    filename: str | None = None


class SourcePlugin(Protocol):
    name: str

    def defaults(self) -> dict[str, Any]:
        """Code-first defaults (url, auth env names, filters, etc.)."""
        ...

    def extract_to_raw(
        self,
        *,
        config: dict[str, Any],
        interval: Interval,
        data_dir: Path | LakeRef,
    ) -> list[dict[str, Any]]:
        """
        Fetch or copy source bytes into data_dir, run a format check, return artifact
        descriptors for the raw manifest (paths relative to the raw partition root).
        """
        ...

    def records_from_raw(
        self,
        *,
        config: dict[str, Any],
        raw_dir: Path | LakeRef,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        """Parse data/ artifacts into source-native rows (no naming; runtime coerces)."""
        ...


def mapper(name: str) -> Callable[[_F], _F]:
    """Mark a migrate function for discovery (same module as the source plugin)."""
    if not name or not isinstance(name, str):
        raise ValueError(f"mapper name must be a non-empty string, got {name!r}")

    def deco(fn: _F) -> _F:
        setattr(fn, MAPPER_ATTR, name)
        return fn

    return deco


def merge_source_config(defaults: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """YAML overrides win on key conflict (shallow merge)."""
    merged = dict(defaults)
    merged.update(overrides or {})
    return merged
