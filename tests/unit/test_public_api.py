"""Public ``det.__all__`` surface stays importable."""

from __future__ import annotations

import det
from det.runtime import lease as lease_mod


def test_all_exports_are_defined() -> None:
    assert det.__all__
    for name in det.__all__:
        assert hasattr(det, name), f"missing export {name!r}"
        assert getattr(det, name) is not None or name == "__version__"


def test_lock_aliases_match_low_level() -> None:
    assert det.inspect_lease is lease_mod.read_lock
    assert det.release_lock is lease_mod.force_release_lock
    assert det.inspect_lease is lease_mod.inspect_lease
    assert det.release_lock is lease_mod.release_lock


def test_version_and_lake_layout() -> None:
    assert isinstance(det.__version__, str) and det.__version__
    assert det.LAKE_LAYOUT == 1


def test_load_pipeline_exported() -> None:
    assert "load_pipeline" in det.__all__
    assert callable(det.load_pipeline)


def test_http_json_submodule_all() -> None:
    from det.sources import http_json

    assert set(http_json.__all__) <= set(dir(http_json))
    for name in http_json.__all__:
        assert hasattr(http_json, name)


def test_http_submodule_all() -> None:
    from det.sources import http

    assert set(http.__all__) <= set(dir(http))
    for name in http.__all__:
        assert hasattr(http, name)
