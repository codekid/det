"""Optional dependency guards raise actionable ImportErrors."""

from __future__ import annotations

import builtins
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from det import optional_deps


@pytest.fixture
def hide_modules(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Force ImportError for selected third-party modules."""

    blocked = {"duckdb", "jinja2", "dlt", "bs4"}
    real_import = builtins.__import__

    def _import(
        name: str,
        globals: Any = None,
        locals: Any = None,
        fromlist: Any = (),
        level: int = 0,
    ):
        root = name.split(".", 1)[0]
        if root in blocked:
            raise ImportError(f"hidden for test: {name}")
        return real_import(name, globals, locals, fromlist, level)

    for name in list(sys.modules):
        if name.split(".", 1)[0] in blocked:
            monkeypatch.delitem(sys.modules, name, raising=False)

    monkeypatch.setattr(builtins, "__import__", _import)
    yield


def test_require_duckdb_hint(hide_modules: None) -> None:
    with pytest.raises(ImportError, match=r"det\[duckdb\]"):
        optional_deps.require_duckdb()


def test_require_jinja2_hint(hide_modules: None) -> None:
    with pytest.raises(ImportError, match=r"det\[scaffold\]"):
        optional_deps.require_jinja2()


def test_require_dlt_rest_hint(hide_modules: None) -> None:
    with pytest.raises(ImportError, match=r"det\[examples\]"):
        optional_deps.require_dlt_rest()


def test_require_beautifulsoup_hint(hide_modules: None) -> None:
    with pytest.raises(ImportError, match=r"det\[examples\]"):
        optional_deps.require_beautifulsoup()
