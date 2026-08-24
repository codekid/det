from __future__ import annotations

from collections.abc import Callable
from typing import Any

from det.errors import DetNotFoundError, DetPluginError
from det.ingestion.base import IngestionBackend
from det.runtime.discovery import (
    PluginLoadError,
    discovered_source_ids,
    iter_discovered_mappers,
    load_source,
)
from det.sources.base import SourcePlugin

_SOURCE_REGISTRY: dict[str, Callable[[], SourcePlugin]] = {}
_INGESTION_REGISTRY: dict[str, Callable[[], IngestionBackend]] = {}
_MAPPER_REGISTRY: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}
_MAPPERS_SCANNED = False


def clear_registries() -> None:
    """Empty plugin registries (MCP refresh / tests). Source list remains path-derived."""
    global _MAPPERS_SCANNED
    _SOURCE_REGISTRY.clear()
    _INGESTION_REGISTRY.clear()
    _MAPPER_REGISTRY.clear()
    _MAPPERS_SCANNED = False


def register_source(name: str, factory: Callable[[], SourcePlugin]) -> None:
    existing = _SOURCE_REGISTRY.get(name)
    if existing is not None and existing is not factory:
        raise PluginLoadError(f"duplicate source {name!r}")
    _SOURCE_REGISTRY[name] = factory


def register_ingestion(name: str, factory: Callable[[], IngestionBackend]) -> None:
    existing = _INGESTION_REGISTRY.get(name)
    if existing is not None and existing is not factory:
        raise PluginLoadError(f"duplicate ingestion library {name!r}")
    _INGESTION_REGISTRY[name] = factory


def register_mapper(name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
    existing = _MAPPER_REGISTRY.get(name)
    if existing is not None and existing is not fn:
        raise PluginLoadError(f"duplicate mapper {name!r}")
    _MAPPER_REGISTRY[name] = fn


def _ensure_source_factory(name: str) -> Callable[[], SourcePlugin]:
    cached = _SOURCE_REGISTRY.get(name)
    if cached is not None:
        return cached
    try:
        factory = load_source(name)
    except KeyError as exc:
        raise DetNotFoundError(
            f"Unknown source '{name}'. Available: {discovered_source_ids()}"
        ) from exc
    except PluginLoadError:
        raise
    except Exception as exc:
        raise DetPluginError(
            f"failed to load source {name!r}: {exc}", plugin=name
        ) from exc
    register_source(name, factory)
    return factory


def get_source(name: str) -> SourcePlugin:
    return _ensure_source_factory(name)()


def get_ingestion(name: str) -> IngestionBackend:
    try:
        return _INGESTION_REGISTRY[name]()
    except KeyError as exc:
        raise DetNotFoundError(
            f"Unknown ingestion library '{name}'. Available: {sorted(_INGESTION_REGISTRY)}"
        ) from exc


def _ensure_mappers() -> None:
    global _MAPPERS_SCANNED
    if _MAPPERS_SCANNED:
        return
    if "identity" not in _MAPPER_REGISTRY:
        from det.runtime.mappers import identity_mapper

        register_mapper("identity", identity_mapper)
    for mapper_name, fn in iter_discovered_mappers():
        register_mapper(mapper_name, fn)
    _MAPPERS_SCANNED = True


def get_mapper(name: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    _ensure_mappers()
    try:
        return _MAPPER_REGISTRY[name]
    except KeyError as exc:
        raise DetNotFoundError(
            f"Unknown mapper '{name}'. Available: {sorted(_MAPPER_REGISTRY)}"
        ) from exc


def list_sources() -> list[str]:
    return discovered_source_ids()


def list_mappers() -> list[str]:
    _ensure_mappers()
    return sorted(_MAPPER_REGISTRY)


def describe_mappers() -> list[tuple[str, str]]:
    """Registered mappers paired with the first line of their docstring."""
    _ensure_mappers()
    described: list[tuple[str, str]] = []
    for name in sorted(_MAPPER_REGISTRY):
        doc = (_MAPPER_REGISTRY[name].__doc__ or "").strip()
        described.append((name, doc.splitlines()[0].strip() if doc else ""))
    return described
