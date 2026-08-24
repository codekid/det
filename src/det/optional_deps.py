"""Optional dependency guards for slim ``det`` extras."""

from __future__ import annotations

from types import ModuleType


def require_duckdb() -> ModuleType:
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError(
            "duckdb is required for this operation; install with: "
            "pip install 'det[duckdb]' (or uv sync --extra duckdb)"
        ) from exc
    return duckdb


def require_jinja2() -> ModuleType:
    try:
        import jinja2
    except ImportError as exc:
        raise ImportError(
            "jinja2 is required for scaffolding; install with: "
            "pip install 'det[scaffold]' (or uv sync --extra scaffold)"
        ) from exc
    return jinja2


def require_dlt_rest() -> tuple[ModuleType, ModuleType, ModuleType]:
    """Return ``(RESTClient, BearerTokenAuth, paginators_module)`` helpers."""
    try:
        from dlt.sources.helpers.rest_client import RESTClient
        from dlt.sources.helpers.rest_client import auth as rest_auth
        from dlt.sources.helpers.rest_client import paginators as rest_paginators
    except ImportError as exc:
        raise ImportError(
            "dlt is required for this HTTP source; install with: "
            "pip install 'det[examples]' (or uv sync --extra examples)"
        ) from exc
    return RESTClient, rest_auth, rest_paginators


def require_beautifulsoup() -> ModuleType:
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise ImportError(
            "beautifulsoup4 is required for NOAA HTML listing; install with: "
            "pip install 'det[examples]' (or uv sync --extra examples)"
        ) from exc
    return BeautifulSoup
