"""Registry helpers for out-of-tree plugin tests."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager

from det.logging import clear_secret_values
from det.plugins import load_plugins
from det.runtime.registry import clear_registries, register_source
from det.runtime.secrets import clear_secret_cache
from det.sources.base import SourcePlugin


def register_source_for_tests(
    name: str,
    source: type[SourcePlugin] | Callable[[], SourcePlugin],
) -> None:
    """
    Register a source factory for the current process (tests / notebooks).

    Prefer project-local ``sources/`` or entry points for production discovery.
    Pair with :func:`isolated_registries` so registrations do not leak.
    """
    if isinstance(source, type):
        factory: Callable[[], SourcePlugin] = source  # type: ignore[assignment]
    else:
        factory = source
    register_source(name, factory)


@contextmanager
def isolated_registries() -> Iterator[None]:
    """
    Snapshot plugin registries + secret caches; restore on exit.

    Clears process secret caches before and after the block. Reloads built-in
    ingestion backends after restore when the snapshot was empty.
    """
    from det import plugins as plugs
    from det.runtime import registry as reg

    src_snap = dict(reg._SOURCE_REGISTRY)
    ing_snap = dict(reg._INGESTION_REGISTRY)
    map_snap = dict(reg._MAPPER_REGISTRY)
    scanned = set(reg._MAPPERS_SCANNED)
    loaded = plugs._LOADED

    clear_secret_cache()
    clear_secret_values()
    try:
        yield
    finally:
        reg._SOURCE_REGISTRY.clear()
        reg._SOURCE_REGISTRY.update(src_snap)
        reg._INGESTION_REGISTRY.clear()
        reg._INGESTION_REGISTRY.update(ing_snap)
        reg._MAPPER_REGISTRY.clear()
        reg._MAPPER_REGISTRY.update(map_snap)
        reg._MAPPERS_SCANNED.clear()
        reg._MAPPERS_SCANNED.update(scanned)
        plugs._LOADED = loaded
        if not reg._INGESTION_REGISTRY:
            clear_registries()
            load_plugins()
        clear_secret_cache()
        clear_secret_values()
