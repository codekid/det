from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from det.runtime.discovery import (
    PluginLoadError,
    collect_mappers,
    discovered_source_ids,
    in_tree_source_map,
    is_in_tree_plugin_module,
    source_class_from_module,
)
from det.runtime.registry import (
    get_source,
    register_mapper,
)
from det.sources.base import mapper


class _FakeEP:
    def __init__(self, name: str, loaded: object) -> None:
        self.name = name
        self._loaded = loaded

    def load(self) -> object:
        return self._loaded


def _plugin_class(module_name: str, plugin_id: str) -> type:
    class Plugin:
        name = plugin_id

        def defaults(self) -> dict[str, Any]:
            return {}

        def extract_to_raw(self, **kwargs: Any) -> list:
            return []

        def records_from_raw(self, **kwargs: Any):
            if False:
                yield None

    Plugin.__module__ = module_name
    Plugin.__name__ = "Plugin"
    return Plugin


def test_in_tree_specs_match_path_and_skip_helpers():
    specs = in_tree_source_map()
    assert specs["noaa.storm_events"] == "det.sources.noaa.storm_events"
    assert specs["noaa.fatalities"] == "det.sources.noaa.fatalities"
    assert specs["example_api.orders"] == "det.sources.example_api.orders"
    assert specs["openlibrary.subjects"] == "det.sources.openlibrary.subjects"
    for module_name in specs.values():
        parts = module_name.split(".")
        assert len(parts) == 4
        assert parts[1] == "sources"
        assert parts[2] not in {"http", "http_json", "base"}
        assert not parts[3].startswith("_")
    assert is_in_tree_plugin_module("det.sources.noaa.storm_events")
    assert not is_in_tree_plugin_module("det.sources.http")
    assert not is_in_tree_plugin_module("det.sources.http_json")
    assert not is_in_tree_plugin_module("det.sources.base")
    assert not is_in_tree_plugin_module("det.sources.noaa._private")


def test_source_class_name_mismatch():
    module_name = "det.sources.fake.plugin"
    mod = types.ModuleType(module_name)
    cls = _plugin_class(module_name, "wrong.id")
    mod.Plugin = cls
    with pytest.raises(PluginLoadError, match="must equal 'fake.plugin'"):
        source_class_from_module(mod, expected_id="fake.plugin")


def test_source_class_missing_protocol_methods():
    module_name = "det.sources.fake.plugin"
    mod = types.ModuleType(module_name)

    class Incomplete:
        name = "fake.plugin"

        def defaults(self) -> dict[str, Any]:
            return {}

    Incomplete.__module__ = module_name
    mod.Incomplete = Incomplete
    with pytest.raises(PluginLoadError, match="no SourcePlugin class"):
        source_class_from_module(mod, expected_id="fake.plugin")


def test_collect_mappers_and_duplicate_in_module():
    module_name = "det.sources.fake.plugin"
    mod = types.ModuleType(module_name)

    @mapper("one")
    def one(row: dict[str, Any]) -> dict[str, Any]:
        return row

    one.__module__ = module_name
    mod.one = one
    assert collect_mappers(mod) == {"one": one}

    @mapper("one")
    def also(row: dict[str, Any]) -> dict[str, Any]:
        return dict(row)

    also.__module__ = module_name
    mod.also = also
    with pytest.raises(PluginLoadError, match="duplicate mapper"):
        collect_mappers(mod)


def test_register_mapper_rejects_different_function():
    from det.runtime import registry as reg

    def a(row: dict[str, Any]) -> dict[str, Any]:
        return row

    def b(row: dict[str, Any]) -> dict[str, Any]:
        return dict(row)

    try:
        register_mapper("_test_dup", a)
        with pytest.raises(PluginLoadError, match="duplicate mapper"):
            register_mapper("_test_dup", b)
        register_mapper("_test_dup", a)
    finally:
        reg._MAPPER_REGISTRY.pop(("_test_dup", reg._GLOBAL_ROOT_KEY), None)


def test_entry_point_source_collides_with_in_tree(monkeypatch: pytest.MonkeyPatch):
    def fake_eps(group: str):
        if group == "det.sources":
            return [_FakeEP("noaa.storm_events", object())]
        return []

    monkeypatch.setattr("det.runtime.discovery._entry_points", fake_eps)
    with pytest.raises(PluginLoadError, match="collides with in-tree"):
        discovered_source_ids()


def test_get_source_does_not_import_other_providers():
    from det.runtime import registry as reg
    from det.runtime.discovery import evict_in_tree_plugin_modules

    evict_in_tree_plugin_modules()
    reg._SOURCE_REGISTRY.clear()
    assert "det.sources.noaa.storm_events" not in sys.modules
    source = get_source("example_api.orders")
    assert source.name == "example_api.orders"
    assert "det.sources.example_api.orders" in sys.modules
    assert "det.sources.noaa.storm_events" not in sys.modules


def test_noaa_storm_events_mapper_in_tree():
    from det.runtime.registry import list_mappers

    assert "noaa_storm_events_episode_id_str" in list_mappers()
