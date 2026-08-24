"""Path-convention source/mapper discovery: in-tree, project-local, entry points.

In-tree: ``det.sources.<provider>.<source>`` (package modules).
Project-local: ``{project_root}/sources/<provider>/<source>.py``.
Helpers at the ``det.sources`` package root (``http``, ``http_json``, ``base``)
are not candidates. Leaf names starting with ``_`` are skipped.

``cls.name`` must equal ``{provider}.{source}``. Out-of-tree packages may register
via entry points ``det.sources`` / ``det.mappers``; those names must not collide
with in-tree or project-local ids.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import pkgutil
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import det.sources as sources_pkg
from det.errors import DetPluginError
from det.sources.base import MAPPER_ATTR, SourcePlugin

SOURCES_GROUP = "det.sources"
MAPPERS_GROUP = "det.mappers"
_PLUGIN_METHODS = ("defaults", "extract_to_raw", "records_from_raw")
_PROJECT_MODULE_PREFIX = "_det_project_sources"


class PluginLoadError(DetPluginError):
    """A discovered plugin module exists but failed name, protocol, or import checks."""

    def __init__(self, message: str, *, module: str | None = None) -> None:
        super().__init__(message, plugin=module)
        self.module = module


def _entry_points(group: str) -> importlib.metadata.EntryPoints:
    return importlib.metadata.entry_points(group=group)


def resolve_discovery_root(project_root: Path | None = None) -> Path:
    """Explicit root > active ``DetSettings`` > ``DET_PROJECT_ROOT`` / cwd."""
    if project_root is not None:
        return Path(project_root).expanduser().resolve()
    from det.runtime.settings import get_active_settings

    active = get_active_settings()
    if active is not None:
        return active.project_root
    from det.runtime.pipelines import resolve_project_root

    return resolve_project_root(None)


def project_sources_dir(project_root: Path) -> Path:
    return Path(project_root).resolve() / "sources"


def iter_in_tree_source_specs() -> Iterator[tuple[str, str]]:
    """Yield ``(plugin_id, module_name)`` for ``det.sources.<provider>.<leaf>`` modules."""
    prefix = sources_pkg.__name__
    for _finder, provider, ispkg in pkgutil.iter_modules(sources_pkg.__path__):
        if not ispkg or provider.startswith("_"):
            continue
        search_paths = [
            str(Path(base) / provider)
            for base in sources_pkg.__path__
            if (Path(base) / provider).is_dir()
        ]
        if not search_paths:
            continue
        for _leaf_finder, leaf, leaf_ispkg in pkgutil.iter_modules(search_paths):
            if leaf_ispkg or leaf.startswith("_"):
                continue
            yield f"{provider}.{leaf}", f"{prefix}.{provider}.{leaf}"


def in_tree_source_map() -> dict[str, str]:
    """plugin_id → module_name for in-tree plugins."""
    return dict(iter_in_tree_source_specs())


def iter_project_source_specs(
    project_root: Path | None = None,
) -> Iterator[tuple[str, Path]]:
    """Yield ``(plugin_id, path)`` for ``{project_root}/sources/<provider>/<source>.py``."""
    root = project_sources_dir(resolve_discovery_root(project_root))
    if not root.is_dir():
        return
    for provider_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        provider = provider_dir.name
        if provider.startswith("_"):
            continue
        for path in sorted(provider_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            yield f"{provider}.{path.stem}", path


def project_source_map(project_root: Path | None = None) -> dict[str, Path]:
    return dict(iter_project_source_specs(project_root))


def is_in_tree_plugin_module(module_name: str) -> bool:
    """True for ``det.sources.<provider>.<leaf>`` plugin modules (not helpers)."""
    parts = module_name.split(".")
    return (
        len(parts) == 4
        and parts[0] == "det"
        and parts[1] == "sources"
        and not parts[2].startswith("_")
        and not parts[3].startswith("_")
    )


def is_project_plugin_module(module_name: str) -> bool:
    return module_name.startswith(f"{_PROJECT_MODULE_PREFIX}.")


def evict_in_tree_plugin_modules() -> None:
    """Drop discovered plugin modules from ``sys.modules`` (not helpers or packages)."""
    for name in [n for n in sys.modules if is_in_tree_plugin_module(n)]:
        _unbind_submodule(name)
        del sys.modules[name]
    for name in [n for n in sys.modules if is_project_plugin_module(n)]:
        del sys.modules[name]


def bind_in_tree_plugin_modules(modules: dict[str, ModuleType]) -> None:
    """Put plugin modules back in ``sys.modules`` and on their parent packages."""
    sys.modules.update(modules)
    for name, module in modules.items():
        _bind_submodule(name, module)


def _unbind_submodule(module_name: str) -> None:
    parent_name, _, leaf = module_name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is None:
        return
    attr = getattr(parent, leaf, None)
    if isinstance(attr, ModuleType) and attr.__name__ == module_name:
        delattr(parent, leaf)


def _bind_submodule(module_name: str, module: ModuleType) -> None:
    parent_name, _, leaf = module_name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None:
        setattr(parent, leaf, module)


def discovered_source_ids(project_root: Path | None = None) -> list[str]:
    """In-tree + project-local + entry-point ids. Does not import plugin modules."""
    root = resolve_discovery_root(project_root)
    in_tree = in_tree_source_map()
    project = project_source_map(root)
    ids = set(in_tree)
    for plugin_id, path in project.items():
        if plugin_id in in_tree:
            raise PluginLoadError(
                f"project source {plugin_id!r} at {path} collides with in-tree "
                f"{in_tree[plugin_id]}",
                module=str(path),
            )
        ids.add(plugin_id)
    for ep in _entry_points(SOURCES_GROUP):
        if ep.name in in_tree:
            raise PluginLoadError(
                f"entry point source {ep.name!r} collides with in-tree {in_tree[ep.name]}",
                module=in_tree[ep.name],
            )
        if ep.name in project:
            raise PluginLoadError(
                f"entry point source {ep.name!r} collides with project-local "
                f"{project[ep.name]}",
                module=str(project[ep.name]),
            )
        ids.add(ep.name)
    return sorted(ids)


def is_source_plugin_class(obj: object) -> bool:
    if not isinstance(obj, type):
        return False
    name = getattr(obj, "name", None)
    if not isinstance(name, str) or not name.strip():
        return False
    return all(callable(getattr(obj, meth, None)) for meth in _PLUGIN_METHODS)


def source_class_from_module(module: ModuleType, *, expected_id: str) -> type[SourcePlugin]:
    """Exactly one plugin class defined in *module*; ``cls.name`` must equal *expected_id*."""
    defined = [
        obj
        for obj in module.__dict__.values()
        if is_source_plugin_class(obj) and getattr(obj, "__module__", None) == module.__name__
    ]
    if not defined:
        raise PluginLoadError(
            f"no SourcePlugin class in {module.__name__} (expected name {expected_id!r})",
            module=module.__name__,
        )
    if len(defined) > 1:
        names = sorted({getattr(cls, "name", "?") for cls in defined})
        raise PluginLoadError(
            f"{module.__name__} defines {len(defined)} source plugins {names}; "
            "expected exactly one",
            module=module.__name__,
        )
    cls = defined[0]
    name = cls.name
    if name != expected_id:
        raise PluginLoadError(
            f"{module.__name__} plugin name {name!r} must equal {expected_id!r} "
            f"(path sources/<provider>/<source>.py or det.sources.<provider>.<source>)",
            module=module.__name__,
        )
    return cls


def collect_mappers(module: ModuleType) -> dict[str, Callable[[dict[str, Any]], dict[str, Any]]]:
    """``@mapper``-decorated callables defined on *module*."""
    found: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {}
    for obj in module.__dict__.values():
        mapper_name = getattr(obj, MAPPER_ATTR, None)
        if mapper_name is None or not callable(obj):
            continue
        if getattr(obj, "__module__", None) != module.__name__:
            continue
        if mapper_name in found:
            raise PluginLoadError(
                f"duplicate mapper {mapper_name!r} in {module.__name__}",
                module=module.__name__,
            )
        found[str(mapper_name)] = obj
    return found


def _project_module_name(plugin_id: str) -> str:
    return f"{_PROJECT_MODULE_PREFIX}.{plugin_id.replace('.', '_')}"


def _import_project_source(plugin_id: str, path: Path) -> ModuleType:
    module_name = _project_module_name(plugin_id)
    existing = sys.modules.get(module_name)
    if existing is not None and getattr(existing, "__file__", None) == str(path):
        return existing
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PluginLoadError(
            f"failed to create import spec for project source {plugin_id!r} at {path}",
            module=str(path),
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        sys.modules.pop(module_name, None)
        raise PluginLoadError(
            f"failed to import project source {plugin_id!r} from {path}: {exc}",
            module=str(path),
        ) from exc
    return module


def load_source(
    plugin_id: str,
    *,
    project_root: Path | None = None,
) -> Callable[[], SourcePlugin]:
    """Import the plugin module (project, in-tree, or entry point) and return factory."""
    root = resolve_discovery_root(project_root)
    in_tree = in_tree_source_map()
    project = project_source_map(root)
    ep_by_name = {ep.name: ep for ep in _entry_points(SOURCES_GROUP)}

    if plugin_id in in_tree and plugin_id in project:
        raise PluginLoadError(
            f"project source {plugin_id!r} collides with in-tree {in_tree[plugin_id]}",
            module=str(project[plugin_id]),
        )
    if plugin_id in ep_by_name and plugin_id in in_tree:
        raise PluginLoadError(
            f"entry point source {plugin_id!r} collides with in-tree {in_tree[plugin_id]}",
            module=in_tree[plugin_id],
        )
    if plugin_id in ep_by_name and plugin_id in project:
        raise PluginLoadError(
            f"entry point source {plugin_id!r} collides with project-local "
            f"{project[plugin_id]}",
            module=str(project[plugin_id]),
        )

    if plugin_id in project:
        module = _import_project_source(plugin_id, project[plugin_id])
        return source_class_from_module(module, expected_id=plugin_id)

    if plugin_id in in_tree:
        module_name = in_tree[plugin_id]
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            raise PluginLoadError(
                f"failed to import source {plugin_id!r} from {module_name}: {exc}",
                module=module_name,
            ) from exc
        return source_class_from_module(module, expected_id=plugin_id)

    if plugin_id in ep_by_name:
        ep = ep_by_name[plugin_id]
        try:
            loaded = ep.load()
        except Exception as exc:
            raise PluginLoadError(
                f"failed to load entry point source {plugin_id!r}: {exc}",
            ) from exc
        if not is_source_plugin_class(loaded):
            raise PluginLoadError(
                f"entry point {plugin_id!r} did not load a SourcePlugin class",
            )
        if loaded.name != plugin_id:
            raise PluginLoadError(
                f"entry point source {plugin_id!r} class name {loaded.name!r} mismatch",
                module=getattr(loaded, "__module__", None),
            )
        return loaded

    raise KeyError(plugin_id)


def iter_discovered_mappers(
    project_root: Path | None = None,
) -> Iterator[tuple[str, Callable[[dict[str, Any]], dict[str, Any]]]]:
    """Import every in-tree + project plugin module plus mapper entry points."""
    root = resolve_discovery_root(project_root)
    for _plugin_id, module_name in iter_in_tree_source_specs():
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            raise PluginLoadError(
                f"failed to import {module_name} while collecting mappers: {exc}",
                module=module_name,
            ) from exc
        yield from collect_mappers(module).items()

    for plugin_id, path in iter_project_source_specs(root):
        module = _import_project_source(plugin_id, path)
        yield from collect_mappers(module).items()

    for ep in _entry_points(MAPPERS_GROUP):
        try:
            fn = ep.load()
        except Exception as exc:
            raise PluginLoadError(f"failed to load entry point mapper {ep.name!r}: {exc}") from exc
        if not callable(fn):
            raise PluginLoadError(f"entry point mapper {ep.name!r} is not callable")
        attr = getattr(fn, MAPPER_ATTR, None)
        if attr is not None and attr != ep.name:
            raise PluginLoadError(
                f"entry point mapper {ep.name!r} does not match @mapper({attr!r})",
                module=getattr(fn, "__module__", None),
            )
        yield ep.name, fn


def probe_source_load_errors(
    project_root: Path | None = None,
) -> list[dict[str, str]]:
    """Import each discovered source; return ``{id, detail}`` for failures (not unknown)."""
    errors: list[dict[str, str]] = []
    for plugin_id in discovered_source_ids(project_root=project_root):
        try:
            load_source(plugin_id, project_root=project_root)
        except PluginLoadError as exc:
            errors.append({"id": plugin_id, "detail": str(exc)})
        except Exception as exc:
            errors.append({"id": plugin_id, "detail": f"{type(exc).__name__}: {exc}"})
    return errors
