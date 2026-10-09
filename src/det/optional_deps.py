"""Optional dependency guards for slim ``det`` extras."""

from __future__ import annotations

from types import ModuleType
from typing import Any

# PyPI distribution name (import package remains ``det``).
DISTRIBUTION_NAME = "det-elt"


def pip_extra_hint(extra: str) -> str:
    return f"pip install '{DISTRIBUTION_NAME}[{extra}]'"


def require_duckdb() -> ModuleType:
    try:
        import duckdb  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "duckdb is required for this operation; install with: "
            f"{pip_extra_hint('duckdb')} (or uv sync --extra duckdb)"
        ) from exc
    return duckdb


def require_jinja2() -> ModuleType:
    try:
        import jinja2  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "jinja2 is required for scaffolding; install with: "
            f"{pip_extra_hint('scaffold')} (or uv sync --extra scaffold)"
        ) from exc
    return jinja2


def require_dlt_rest() -> tuple[Any, ModuleType, ModuleType]:
    """Return ``(RESTClient, auth_module, paginators_module)`` helpers."""
    try:
        from dlt.sources.helpers.rest_client import auth as rest_auth  # noqa: PLC0415
        from dlt.sources.helpers.rest_client import paginators as rest_paginators  # noqa: PLC0415
        from dlt.sources.helpers.rest_client.client import RESTClient  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "dlt is required for this HTTP source; install with: "
            f"{pip_extra_hint('examples')} (or uv sync --extra examples)"
        ) from exc
    return RESTClient, rest_auth, rest_paginators


def require_beautifulsoup() -> Any:
    """Return the ``BeautifulSoup`` class (callable), not the ``bs4`` module."""
    try:
        from bs4 import BeautifulSoup  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "beautifulsoup4 is required for NOAA HTML listing; install with: "
            f"{pip_extra_hint('examples')} (or uv sync --extra examples)"
        ) from exc
    return BeautifulSoup


def require_iceberg() -> None:
    """Ensure ``pyiceberg`` + ``pyarrow`` are importable (iceberg extra)."""
    try:
        import pyarrow  # noqa: PLC0415, F401
        import pyiceberg  # noqa: PLC0415, F401
    except ImportError as exc:
        raise ImportError(
            f"Iceberg bronze requires the optional extra: {pip_extra_hint('iceberg')}"
        ) from exc


def require_psycopg() -> ModuleType:
    try:
        import psycopg  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "psycopg is required for this operation; install with: "
            f"{pip_extra_hint('postgres')} (or uv sync --extra postgres)"
        ) from exc
    return psycopg


def try_import_bigquery() -> Any | None:
    """Return ``google.cloud.bigquery`` or ``None`` when the bigquery extra is absent."""
    try:
        from google.cloud import (  # noqa: PLC0415
            bigquery,  # pyright: ignore[reportAttributeAccessIssue]
        )
    except ImportError:
        return None
    return bigquery


def require_bigquery() -> Any:
    bigquery = try_import_bigquery()
    if bigquery is None:
        raise ImportError(
            "google-cloud-bigquery is required; install with: "
            f"{pip_extra_hint('bigquery')} (or uv sync --extra bigquery)"
        )
    return bigquery
