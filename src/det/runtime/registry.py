from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from det.errors import DetNotFoundError, DetPluginError
from det.ingestion.base import IngestionBackend
from det.runtime.discovery import (
    PluginLoadError,
    discovered_source_ids,
    iter_discovered_mappers,
    load_source,
    resolve_discovery_root,
)
from det.sources.base import SourcePlugin

# Test/process-wide injection via register_source/register_mapper (not project-local).
_GLOBAL_ROOT_KEY = ""
_SOURCE_REGISTRY: dict[tuple[str, str], Callable[[], SourcePlugin]] = {}
_INGESTION_REGISTRY: dict[str, Callable[[], IngestionBackend]] = {}
_MAPPER_REGISTRY: dict[tuple[str, str], Callable[[dict[str, Any]], dict[str, Any]]] = {}
_MAPPERS_SCANNED: set[str] = set()


def _root_key(project_root: Path | None) -> str:
    return str(resolve_discovery_root(project_root))


def _mapper_key(name: str, root_key: str) -> tuple[str, str]:
    return (name, root_key)


def _resolve_mapper(
    name: str,
    root_key: str,
) -> Callable[[dict[str, Any]], dict[str, Any]] | None:
    fn = _MAPPER_REGISTRY.get(_mapper_key(name, root_key))
    if fn is not None:
        return fn
    return _MAPPER_REGISTRY.get(_mapper_key(name, _GLOBAL_ROOT_KEY))


def _mapper_names_for_root(root_key: str) -> list[str]:
    names: set[str] = set()
    for mapper_name, mapper_root in _MAPPER_REGISTRY:
        if mapper_root == root_key or mapper_root == _GLOBAL_ROOT_KEY:
            names.add(mapper_name)
    return sorted(names)


def clear_registries() -> None:
    """Empty plugin registries (MCP refresh / tests). Source list remains path-derived."""
    _SOURCE_REGISTRY.clear()
    _INGESTION_REGISTRY.clear()
    _MAPPER_REGISTRY.clear()
    _MAPPERS_SCANNED.clear()
    import det.plugins as plugs

    plugs._LOADED = False


def register_source(name: str, factory: Callable[[], SourcePlugin]) -> None:
    key = (name, _GLOBAL_ROOT_KEY)
    existing = _SOURCE_REGISTRY.get(key)
    if existing is not None and existing is not factory:
        raise PluginLoadError(f"duplicate source {name!r}")
    _SOURCE_REGISTRY[key] = factory


def register_ingestion(name: str, factory: Callable[[], IngestionBackend]) -> None:
    existing = _INGESTION_REGISTRY.get(name)
    if existing is not None and existing is not factory:
        raise PluginLoadError(f"duplicate ingestion library {name!r}")
    _INGESTION_REGISTRY[name] = factory


def register_mapper(
    name: str,
    fn: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    project_root: Path | None = None,
) -> None:
    """Register a mapper globally (``project_root`` omitted) or for one project root."""
    root_key = _root_key(project_root) if project_root is not None else _GLOBAL_ROOT_KEY
    key = _mapper_key(name, root_key)
    existing = _MAPPER_REGISTRY.get(key)
    if existing is not None and existing is not fn:
        raise PluginLoadError(f"duplicate mapper {name!r}")
    _MAPPER_REGISTRY[key] = fn


def _ensure_source_factory(
    name: str,
    *,
    project_root: Path | None = None,
) -> Callable[[], SourcePlugin]:
    root_key = _root_key(project_root)
    key = (name, root_key)
    cached = _SOURCE_REGISTRY.get(key)
    if cached is not None:
        return cached
    global_cached = _SOURCE_REGISTRY.get((name, _GLOBAL_ROOT_KEY))
    if global_cached is not None:
        return global_cached
    try:
        factory = load_source(name, project_root=project_root)
    except KeyError as exc:
        raise DetNotFoundError(
            f"Unknown source '{name}'. Available: "
            f"{discovered_source_ids(project_root=project_root)}"
        ) from exc
    except PluginLoadError:
        raise
    except Exception as exc:
        raise DetPluginError(
            f"failed to load source {name!r}: {exc}", plugin=name
        ) from exc
    _SOURCE_REGISTRY[key] = factory
    return factory


def get_source(name: str, *, project_root: Path | None = None) -> SourcePlugin:
    return _ensure_source_factory(name, project_root=project_root)()


def get_ingestion(name: str) -> IngestionBackend:
    try:
        return _INGESTION_REGISTRY[name]()
    except KeyError as exc:
        raise DetNotFoundError(
            f"Unknown ingestion library '{name}'. Available: {sorted(_INGESTION_REGISTRY)}"
        ) from exc


def _ensure_mappers(*, project_root: Path | None = None) -> None:
    root_key = _root_key(project_root)
    if root_key in _MAPPERS_SCANNED:
        return
    if _resolve_mapper("identity", root_key) is None:
        from det.runtime.mappers import identity_mapper

        register_mapper("identity", identity_mapper)
    for mapper_name, fn in iter_discovered_mappers(project_root=project_root):
        register_mapper(mapper_name, fn, project_root=project_root)
    _MAPPERS_SCANNED.add(root_key)


def get_mapper(
    name: str,
    *,
    project_root: Path | None = None,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    _ensure_mappers(project_root=project_root)
    root_key = _root_key(project_root)
    fn = _resolve_mapper(name, root_key)
    if fn is None:
        raise DetNotFoundError(
            f"Unknown mapper '{name}'. Available: {_mapper_names_for_root(root_key)}"
        )
    return fn


def list_sources(*, project_root: Path | None = None) -> list[str]:
    return discovered_source_ids(project_root=project_root)


def list_mappers(*, project_root: Path | None = None) -> list[str]:
    _ensure_mappers(project_root=project_root)
    return _mapper_names_for_root(_root_key(project_root))


def describe_mappers(*, project_root: Path | None = None) -> list[tuple[str, str]]:
    """Registered mappers paired with the first line of their docstring."""
    _ensure_mappers(project_root=project_root)
    root_key = _root_key(project_root)
    described: list[tuple[str, str]] = []
    for name in _mapper_names_for_root(root_key):
        fn = _resolve_mapper(name, root_key)
        if fn is None:
            continue
        doc = (fn.__doc__ or "").strip()
        described.append((name, doc.splitlines()[0].strip() if doc else ""))
    return described
