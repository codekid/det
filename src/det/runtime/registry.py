from __future__ import annotations

from collections.abc import Callable
from typing import Any

from det.ingestion.base import IngestionBackend
from det.sources.base import SourcePlugin

_SOURCE_REGISTRY: dict[str, Callable[[], SourcePlugin]] = {}
_INGESTION_REGISTRY: dict[str, Callable[[], IngestionBackend]] = {}
_MAPPER_REGISTRY: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}


def register_source(name: str, factory: Callable[[], SourcePlugin]) -> None:
    _SOURCE_REGISTRY[name] = factory


def register_ingestion(name: str, factory: Callable[[], IngestionBackend]) -> None:
    _INGESTION_REGISTRY[name] = factory


def register_mapper(name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
    _MAPPER_REGISTRY[name] = fn


def get_source(name: str) -> SourcePlugin:
    try:
        return _SOURCE_REGISTRY[name]()
    except KeyError as exc:
        raise KeyError(
            f"Unknown source '{name}'. Available: {sorted(_SOURCE_REGISTRY)}"
        ) from exc


def get_ingestion(name: str) -> IngestionBackend:
    try:
        return _INGESTION_REGISTRY[name]()
    except KeyError as exc:
        raise KeyError(
            f"Unknown ingestion library '{name}'. Available: {sorted(_INGESTION_REGISTRY)}"
        ) from exc


def get_mapper(name: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    try:
        return _MAPPER_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown mapper '{name}'. Available: {sorted(_MAPPER_REGISTRY)}"
        ) from exc


def list_sources() -> list[str]:
    return sorted(_SOURCE_REGISTRY)


def list_mappers() -> list[str]:
    return sorted(_MAPPER_REGISTRY)


def describe_mappers() -> list[tuple[str, str]]:
    """Registered mappers paired with the first line of their docstring."""
    described: list[tuple[str, str]] = []
    for name in sorted(_MAPPER_REGISTRY):
        doc = (_MAPPER_REGISTRY[name].__doc__ or "").strip()
        described.append((name, doc.splitlines()[0].strip() if doc else ""))
    return described
