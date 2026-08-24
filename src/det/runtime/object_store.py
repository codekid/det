"""Shared S3/MinIO and GCS settings for DET lake I/O and Iceberg FileIO.

``AWS_ENDPOINT_URL`` is enough for s3fs/botocore, but PyIceberg/PyArrow FileIO
only honors catalog properties (``s3.endpoint``, …). Map the same env once so
raw and bronze stay aligned on MinIO or custom S3 APIs.

GCS mirrors that pattern: ``STORAGE_EMULATOR_HOST`` / ADC /
``GOOGLE_APPLICATION_CREDENTIALS`` for gcsfs, and ``gcs.*`` catalog props for
PyIceberg FileIO on ``gs://`` warehouses.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse


def _env(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def s3_endpoint_from_env(env: Mapping[str, str] | None = None) -> str | None:
    text = (_env(env).get("AWS_ENDPOINT_URL") or "").strip()
    return text or None


def s3_region_from_env(env: Mapping[str, str] | None = None) -> str | None:
    environ = _env(env)
    for key in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        text = (environ.get(key) or "").strip()
        if text:
            return text
    return None


def fsspec_s3_kwargs(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Kwargs for ``fsspec.filesystem(\"s3\", …)`` (explicit MinIO endpoint)."""
    environ = _env(env)
    kwargs: dict[str, Any] = {}
    endpoint = s3_endpoint_from_env(environ)
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    key = (environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    secret = (environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    if key:
        kwargs["key"] = key
    if secret:
        kwargs["secret"] = secret
    region = s3_region_from_env(environ)
    client_kwargs: dict[str, Any] = {}
    if region:
        client_kwargs["region_name"] = region
    if client_kwargs:
        kwargs["client_kwargs"] = client_kwargs
    # Path-style is required for most MinIO / custom endpoints.
    if endpoint:
        kwargs["config_kwargs"] = {"s3": {"addressing_style": "path"}}
    return kwargs


def iceberg_s3_properties(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """PyIceberg FileIO catalog properties for ``s3://`` warehouses."""
    environ = _env(env)
    props: dict[str, str] = {}
    endpoint = s3_endpoint_from_env(environ)
    if endpoint:
        props["s3.endpoint"] = endpoint
        # MinIO and path-style gateways break under virtual-hosted addressing.
        props["s3.force-virtual-addressing"] = "false"
        props["s3.resolve-region"] = "false"
    region = s3_region_from_env(environ)
    if region:
        props["s3.region"] = region
    elif endpoint:
        props["s3.region"] = "us-east-1"
    key = (environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    secret = (environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    if key:
        props["s3.access-key-id"] = key
    if secret:
        props["s3.secret-access-key"] = secret
    token = (environ.get("AWS_SESSION_TOKEN") or "").strip()
    if token:
        props["s3.session-token"] = token
    return props


def gcs_emulator_host_from_env(env: Mapping[str, str] | None = None) -> str | None:
    """
    Normalize ``STORAGE_EMULATOR_HOST`` to ``protocol://host:port`` (no path).

    gcsfs and PyIceberg ``gcs.service.host`` expect a URL; localgcp often sets
    ``localhost:4443`` without a scheme.
    """
    text = (_env(env).get("STORAGE_EMULATOR_HOST") or "").strip()
    if not text or text == "default":
        return None
    if not text.startswith(("http://", "https://")):
        text = f"http://{text}"
    parsed = urlparse(text)
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def gcs_project_from_env(env: Mapping[str, str] | None = None) -> str | None:
    environ = _env(env)
    for key in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"):
        text = (environ.get(key) or "").strip()
        if text:
            return text
    return None


def fsspec_gcs_kwargs(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Kwargs for ``fsspec.filesystem(\"gcs\", …)`` (ADC or storage emulator)."""
    environ = _env(env)
    kwargs: dict[str, Any] = {}
    endpoint = gcs_emulator_host_from_env(environ)
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        # Emulators reject real OAuth; anonymous is the usual localgcp / fake-gcs path.
        kwargs.setdefault("token", "anon")
    project = gcs_project_from_env(environ)
    if project:
        kwargs["project"] = project
    elif endpoint:
        kwargs.setdefault("project", "det-local")
    token = (environ.get("GCS_OAUTH_TOKEN") or "").strip()
    if token:
        kwargs["token"] = token
    # GOOGLE_APPLICATION_CREDENTIALS is read by google-auth when token is unset.
    return kwargs


def iceberg_gcs_properties(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """PyIceberg FileIO catalog properties for ``gs://`` warehouses."""
    environ = _env(env)
    props: dict[str, str] = {}
    endpoint = gcs_emulator_host_from_env(environ)
    if endpoint:
        props["gcs.service.host"] = endpoint
    project = gcs_project_from_env(environ)
    if project:
        props["gcs.project-id"] = project
    elif endpoint:
        props["gcs.project-id"] = "det-local"
    token = (environ.get("GCS_OAUTH_TOKEN") or "").strip()
    if token:
        props["gcs.oauth2.token"] = token
    return props


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def duckdb_s3_credentials_required(env: Mapping[str, str] | None = None) -> tuple[str, str]:
    """Return static S3 key/secret or raise when dbt/DuckDB cannot auth."""
    environ = _env(env)
    key = (environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    secret = (environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    if not key or not secret:
        raise ValueError(
            "object-lake dbt requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "(same env as DET extract/load)"
        )
    return key, secret


def duckdb_s3_endpoint_parts(
    env: Mapping[str, str] | None = None,
) -> tuple[str | None, bool]:
    """DuckDB S3 secret endpoint host (no scheme) and USE_SSL flag."""
    endpoint = s3_endpoint_from_env(env)
    if not endpoint:
        return None, False
    parsed = urlparse(endpoint)
    host = parsed.netloc or parsed.path
    return (host or None), parsed.scheme == "https"


def duckdb_s3_secret_params(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Parameters for DuckDB ``CREATE SECRET`` / dbt-duckdb profile secrets."""
    key, secret = duckdb_s3_credentials_required(env)
    endpoint_host, use_ssl = duckdb_s3_endpoint_parts(env)
    params: dict[str, Any] = {
        "key_id": key,
        "secret": secret,
        "region": s3_region_from_env(env) or "us-east-1",
        "url_style": "path",
        "use_ssl": use_ssl,
    }
    if endpoint_host:
        params["endpoint"] = endpoint_host
    return params


def duckdb_s3_profile_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Env exports for ``dbt/profiles.yml`` duckdb_s3 target (``DET_DUCKDB_S3_*``)."""
    duckdb_s3_credentials_required(env)
    endpoint_host, use_ssl = duckdb_s3_endpoint_parts(env)
    out: dict[str, str] = {
        "DET_DUCKDB_S3_USE_SSL": "true" if use_ssl else "false",
    }
    if endpoint_host:
        out["DET_DUCKDB_S3_ENDPOINT"] = endpoint_host
    return out


def configure_duckdb_s3(
    con: Any,
    env: Mapping[str, str] | None = None,
    *,
    secret_name: str = "det_s3",
) -> None:
    """Install httpfs/iceberg and register an S3 secret on a DuckDB connection."""
    params = duckdb_s3_secret_params(env)
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    parts = [
        "TYPE s3",
        f"KEY_ID '{_sql_literal(str(params['key_id']))}'",
        f"SECRET '{_sql_literal(str(params['secret']))}'",
        f"REGION '{_sql_literal(str(params['region']))}'",
        "URL_STYLE 'path'",
        f"USE_SSL {'true' if params['use_ssl'] else 'false'}",
    ]
    endpoint = params.get("endpoint")
    if endpoint:
        parts.append(f"ENDPOINT '{_sql_literal(str(endpoint))}'")
    body = ",\n    ".join(parts)
    con.execute(f"CREATE OR REPLACE SECRET {secret_name} (\n    {body}\n)")
