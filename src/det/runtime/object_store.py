"""Shared S3/MinIO settings for DET lake I/O and Iceberg FileIO.

``AWS_ENDPOINT_URL`` is enough for s3fs/botocore, but PyIceberg/PyArrow FileIO
only honors catalog properties (``s3.endpoint``, …). Map the same env once so
raw and bronze stay aligned on MinIO or custom S3 APIs.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


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
