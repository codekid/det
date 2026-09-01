"""Resolve Iceberg catalog backend from lake-wide env (hadoop / rest / glue).

Default remains the DET Hadoop-style catalog (``version-hint.text``). Opt into
REST (GCP Lakehouse, Polaris, Glue Iceberg REST, …) or classic AWS Glue via
``DET_ICEBERG_CATALOG``. No per-pipeline YAML — see docs/iceberg-catalog.md.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any, Literal
from urllib.parse import urlparse

from det.logging import get_logger
from det.optional_deps import pip_extra_hint
from det.runtime.lake import LakeRef

logger = get_logger(__name__)

IcebergCatalogKind = Literal["hadoop", "rest", "glue"]

ENV_CATALOG = "DET_ICEBERG_CATALOG"
ENV_REST_URI = "DET_ICEBERG_REST_URI"
ENV_REST_WAREHOUSE = "DET_ICEBERG_REST_WAREHOUSE"
ENV_REST_CREDENTIAL = "DET_ICEBERG_REST_CREDENTIAL"
ENV_REST_SCOPE = "DET_ICEBERG_REST_SCOPE"
ENV_REST_REALM = "DET_ICEBERG_REST_REALM"
ENV_GLUE_ID = "DET_ICEBERG_GLUE_ID"

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


def _file_io_props(warehouse: str, env: Mapping[str, str] | None) -> dict[str, str]:
    from det.runtime.object_store import iceberg_gcs_properties, iceberg_s3_properties

    if warehouse.startswith("s3://"):
        return dict(iceberg_s3_properties(env))
    if warehouse.startswith("gs://"):
        return dict(iceberg_gcs_properties(env))
    return {}


def _rest_uri_host(uri: str) -> str:
    parsed = urlparse(uri)
    return parsed.netloc or uri


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
) -> dict[str, str]:
    """Build PyIceberg REST catalog properties (no network)."""
    environ = _env(env)
    uri = (environ.get(ENV_REST_URI) or "").strip()
    if not uri:
        raise ValueError(
            f"{ENV_CATALOG}=rest requires {ENV_REST_URI} "
            "(Iceberg REST catalog endpoint)"
        )
    warehouse = (environ.get(ENV_REST_WAREHOUSE) or "").strip() or lake_ref_uri(lake)
    props: dict[str, str] = {
        "type": "rest",
        "uri": uri,
        "warehouse": warehouse,
    }
    credential = (environ.get(ENV_REST_CREDENTIAL) or "").strip()
    if credential:
        props["credential"] = credential
    scope = (environ.get(ENV_REST_SCOPE) or "").strip()
    if scope:
        props["scope"] = scope
    realm = (environ.get(ENV_REST_REALM) or "").strip()
    if realm:
        props["header.Polaris-Realm"] = realm
    props.update(_file_io_props(lake_ref_uri(lake), env))
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
