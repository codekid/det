"""Resolve Iceberg catalog backend from lake-wide env (hadoop / rest / glue).

Default remains the DET Hadoop-style catalog (``version-hint.text``). Opt into
REST (GCP Lakehouse, Polaris, Glue Iceberg REST, …) or classic AWS Glue via
``DET_ICEBERG_CATALOG``. No per-pipeline YAML — see docs/iceberg-catalog.md.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Any, Literal
from urllib.parse import urlparse

from det.logging import get_logger
from det.optional_deps import pip_extra_hint
from det.runtime.lake import LakeRef
from det.runtime.object_store import gcs_project_from_env, iceberg_gcs_properties

logger = get_logger(__name__)

IcebergCatalogKind = Literal["hadoop", "rest", "glue"]

ENV_CATALOG = "DET_ICEBERG_CATALOG"
ENV_REST_URI = "DET_ICEBERG_REST_URI"
ENV_REST_WAREHOUSE = "DET_ICEBERG_REST_WAREHOUSE"
ENV_REST_CREDENTIAL = "DET_ICEBERG_REST_CREDENTIAL"
ENV_REST_SCOPE = "DET_ICEBERG_REST_SCOPE"
ENV_REST_REALM = "DET_ICEBERG_REST_REALM"
ENV_REST_ACCESS_DELEGATION = "DET_ICEBERG_REST_ACCESS_DELEGATION"
ENV_GLUE_ID = "DET_ICEBERG_GLUE_ID"

_BL_WAREHOUSE_PROJECT = re.compile(r"^bl://projects/([^/]+)/catalogs/")

_ICEBERG_HINT = pip_extra_hint("iceberg")
_KINDS = frozenset({"hadoop", "rest", "glue"})


def _env(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _require_iceberg() -> None:
    try:
        import pyarrow  # noqa: F401
        import pyiceberg  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            f"Iceberg bronze requires the optional extra: {_ICEBERG_HINT}"
        ) from exc


def lake_ref_uri(ref: LakeRef) -> str:
    """URI PyIceberg FileIO accepts (file://, s3://, gs://)."""
    if ref.is_local:
        return ref.to_path().resolve().as_uri()
    text = str(ref)
    if text.startswith("gcs://"):
        return "gs://" + text[len("gcs://") :]
    return text


def catalog_kind_from_env(env: Mapping[str, str] | None = None) -> IcebergCatalogKind:
    """Parse ``DET_ICEBERG_CATALOG``.

    Unset/empty → ``hadoop``, unless ``DET_ICEBERG_REST_URI`` is set (soft
    default ``rest``). Never auto-selects ``glue`` from ``s3://`` alone.
    """
    environ = _env(env)
    raw = (environ.get(ENV_CATALOG) or "").strip().lower()
    if raw:
        if raw not in _KINDS:
            raise ValueError(
                f"{ENV_CATALOG} must be one of hadoop|rest|glue, got {raw!r}"
            )
        return raw  # type: ignore[return-value]
    if (environ.get(ENV_REST_URI) or "").strip():
        return "rest"
    return "hadoop"


def maybe_bind_location(catalog: Any, identifier: Any, location: str) -> None:
    """Hadoop catalogs need bind_location; REST/Glue resolve via the metastore."""
    bind = getattr(catalog, "bind_location", None)
    if callable(bind):
        bind(identifier, location)


def object_store_root_uri(location: str) -> str | None:
    """Bucket root for object-store table URIs (``s3://b/…`` → ``s3://b/``).

    Polaris/REST namespaces pin ``allowedLocations``; using the bucket root lets
    many lake prefixes under one catalog share a namespace. Returns ``None`` for
    ``file://`` / other schemes (Hadoop ignores namespace location props).
    """
    for scheme in ("s3://", "gs://"):
        if not location.startswith(scheme):
            continue
        rest = location[len(scheme) :]
        bucket = rest.split("/", 1)[0]
        if not bucket:
            return None
        return f"{scheme}{bucket}/"
    if location.startswith("gcs://"):
        rest = location[len("gcs://") :]
        bucket = rest.split("/", 1)[0]
        if not bucket:
            return None
        return f"gs://{bucket}/"
    return None


def ensure_iceberg_namespace(
    catalog: Any, namespace: str, *, table_location: str | None = None
) -> None:
    """Idempotent ``create_namespace``; set object-store ``location`` when known."""
    props: dict[str, str] = {}
    if table_location:
        root = object_store_root_uri(table_location)
        if root:
            props["location"] = root
    try:
        if props:
            catalog.create_namespace(namespace, props)
        else:
            catalog.create_namespace(namespace)
    except Exception as exc:
        name = type(exc).__name__
        if "AlreadyExists" in name or "NamespaceAlreadyExists" in name:
            return
        if "already" in str(exc).lower():
            return
        raise


def _is_gcp_lakehouse_rest_uri(uri: str) -> bool:
    host = (urlparse(uri).hostname or "").lower()
    return host == "biglake.googleapis.com"


def _gcp_lakehouse_project(warehouse: str, env: Mapping[str, str]) -> str | None:
    match = _BL_WAREHOUSE_PROJECT.match(warehouse.strip())
    if match:
        return match.group(1)
    for key in ("DET_GCP_PROJECT", "GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"):
        text = (env.get(key) or "").strip()
        if text:
            return text
    return gcs_project_from_env(env)


def _gcp_lakehouse_rest_props(
    uri: str, warehouse: str, env: Mapping[str, str]
) -> dict[str, Any]:
    """PyIceberg REST props for GCP Lakehouse (ADC + billing project header)."""
    if not _is_gcp_lakehouse_rest_uri(uri):
        return {}
    project = _gcp_lakehouse_project(warehouse, env)
    if not project:
        return {}
    props: dict[str, Any] = {
        "auth": {"type": "google"},
        "header.x-goog-user-project": project,
    }
    delegation = env.get(ENV_REST_ACCESS_DELEGATION)
    if delegation is None and warehouse.startswith("bl://"):
        delegation = "vended-credentials"
    if delegation is not None:
        props["header.X-Iceberg-Access-Delegation"] = delegation.strip()
    creds_path = (env.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if creds_path:
        props["auth"] = {"type": "google", "google": {"credentials_path": creds_path}}
    return props


def _file_io_props(warehouse: str, env: Mapping[str, str] | None) -> dict[str, str]:
    from det.runtime.object_store import iceberg_s3_properties

    if warehouse.startswith("s3://"):
        return dict(iceberg_s3_properties(env))
    if warehouse.startswith("gs://"):
        return dict(iceberg_gcs_properties(env))
    return {}


def rest_uri_identity(uri: str) -> str:
    """Canonical non-secret REST endpoint identity for digests and logs.

    Includes scheme, host, explicit port, and path. Omits userinfo, query, and
    fragment so inline credentials in ``DET_ICEBERG_REST_URI`` never appear in
    approvals or structured logs.
    """
    raw = (uri or "").strip()
    parsed = urlparse(raw)
    hostname = parsed.hostname
    if not parsed.scheme or not hostname:
        # Opaque / unparseable: never echo netloc (may contain userinfo).
        return parsed.path or raw.split("@")[-1]

    if ":" in hostname and not hostname.startswith("["):
        host = f"[{hostname}]"
    else:
        host = hostname
    authority = f"{host}:{parsed.port}" if parsed.port is not None else host
    return f"{parsed.scheme}://{authority}{parsed.path or ''}"


def _rest_uri_host(uri: str) -> str:
    return rest_uri_identity(uri)


def _is_aws_glue_rest_uri(uri: str) -> bool:
    """True for Glue Iceberg REST hosts (``glue.<region>.amazonaws.com``)."""
    host = (urlparse(uri).hostname or "").lower()
    return host.startswith("glue.") and host.endswith(".amazonaws.com")


def _glue_rest_sigv4_props(
    uri: str, env: Mapping[str, str]
) -> dict[str, str]:
    """PyIceberg REST SigV4 props for AWS Glue Iceberg REST catalogs."""
    if not _is_aws_glue_rest_uri(uri):
        return {}
    from det.runtime.object_store import s3_region_from_env

    props: dict[str, str] = {
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "glue",
    }
    region = s3_region_from_env(env)
    if not region:
        host = (urlparse(uri).hostname or "").lower()
        # glue.us-east-1.amazonaws.com → us-east-1
        parts = host.split(".")
        if len(parts) >= 4 and parts[0] == "glue":
            region = parts[1]
    if region:
        props["rest.signing-region"] = region
    return props


def hadoop_catalog(lake: LakeRef, *, env: Mapping[str, str] | None = None) -> Any:
    """DET filesystem catalog (``version-hint.text`` on the table location)."""
    _require_iceberg()
    from det.ingestion.iceberg_catalog import LakeHadoopCatalog

    warehouse = lake_ref_uri(lake)
    props: dict[str, str] = {"warehouse": warehouse}
    props.update(_file_io_props(warehouse, env))
    return LakeHadoopCatalog("det", **props)


def rest_catalog_props(
    lake: LakeRef, *, env: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Build PyIceberg REST catalog properties (no network)."""
    environ = _env(env)
    uri = (environ.get(ENV_REST_URI) or "").strip()
    if not uri:
        raise ValueError(
            f"{ENV_CATALOG}=rest requires {ENV_REST_URI} "
            "(Iceberg REST catalog endpoint)"
        )
    explicit_warehouse = (environ.get(ENV_REST_WAREHOUSE) or "").strip()
    lake_uri = lake_ref_uri(lake)
    warehouse = explicit_warehouse or (
        lake_uri if not _is_aws_glue_rest_uri(uri) else ""
    )
    props: dict[str, Any] = {
        "type": "rest",
        "uri": uri,
    }
    if warehouse:
        props["warehouse"] = warehouse
    credential = (environ.get(ENV_REST_CREDENTIAL) or "").strip()
    if credential:
        props["credential"] = credential
    elif _is_gcp_lakehouse_rest_uri(uri):
        props.update(_gcp_lakehouse_rest_props(uri, warehouse, environ))
    scope = (environ.get(ENV_REST_SCOPE) or "").strip()
    if scope:
        props["scope"] = scope
    realm = (environ.get(ENV_REST_REALM) or "").strip()
    if realm:
        props["header.Polaris-Realm"] = realm
    props.update(_glue_rest_sigv4_props(uri, environ))
    props.update(_file_io_props(lake_uri, env))
    return props


def glue_catalog_props(
    lake: LakeRef, *, env: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Build PyIceberg Glue catalog properties (no network)."""
    from det.runtime.object_store import s3_region_from_env

    warehouse = lake_ref_uri(lake)
    if not warehouse.startswith("s3://"):
        raise ValueError(
            f"{ENV_CATALOG}=glue requires an s3:// lake "
            f"(got {warehouse!r}); use rest for GCS Lakehouse or keep hadoop"
        )
    environ = _env(env)
    props: dict[str, str] = {
        "type": "glue",
        "warehouse": warehouse,
    }
    glue_id = (environ.get(ENV_GLUE_ID) or "").strip()
    if glue_id:
        props["glue.id"] = glue_id
    region = s3_region_from_env(environ)
    if region:
        props["client.region"] = region
        props["glue.region"] = region
    props.update(_file_io_props(warehouse, env))
    return props


def resolve_iceberg_catalog(
    lake: LakeRef, *, env: Mapping[str, str] | None = None
) -> Any:
    """Return the PyIceberg catalog for bronze/ops writes."""
    _require_iceberg()
    kind = catalog_kind_from_env(env)
    if kind == "hadoop":
        logger.info("iceberg catalog", kind=kind, warehouse=lake_ref_uri(lake))
        return hadoop_catalog(lake, env=env)

    from pyiceberg.catalog import load_catalog

    if kind == "rest":
        props = rest_catalog_props(lake, env=env)
        logger.info(
            "iceberg catalog",
            kind=kind,
            uri_host=_rest_uri_host(props["uri"]),
            warehouse=props.get("warehouse"),
        )
        return load_catalog("det", **props)

    props = glue_catalog_props(lake, env=env)
    logger.info(
        "iceberg catalog",
        kind=kind,
        glue_id=props.get("glue.id"),
        warehouse=props.get("warehouse"),
        region=props.get("client.region") or props.get("glue.region"),
    )
    return load_catalog("det", **props)
