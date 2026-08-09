"""
Canonical provider.source identity helpers.

Single resolution point for lake paths, SQL schema/table names, JSON schema
defaults, and dbt slugs. Call sites must not join these strings ad hoc.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from det.runtime.config import PipelineConfig

# At least provider.source (one or more dots).
CANONICAL_ID_RE = re.compile(
    r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$"
)


def parse_canonical_id(canonical_id: str) -> tuple[str, str]:
    """
    Split ``provider.source`` (or ``a.b.c``) into (provider, source_name).

    For multi-segment ids, provider is the first segment and source_name is the
    remainder joined by ``_`` (e.g. ``a.b.c`` → ``("a", "b_c")``).
    """
    cid = (canonical_id or "").strip()
    if not CANONICAL_ID_RE.match(cid):
        raise ValueError(
            f"canonical id must be provider.source (dotted), got {canonical_id!r}"
        )
    provider, _, rest = cid.partition(".")
    source_name = rest.replace(".", "_")
    return provider, source_name


def validate_canonical_id(canonical_id: str) -> str:
    cid = (canonical_id or "").strip()
    if not CANONICAL_ID_RE.match(cid):
        raise ValueError(
            f"canonical id must be provider.source (dotted), got {canonical_id!r}"
        )
    return cid


def fs_dataset_parts(canonical_id: str) -> tuple[str, ...]:
    """Filesystem segments under raw/ or bronze/ (e.g. ``("noaa", "storm_events")``)."""
    cid = validate_canonical_id(canonical_id)
    return tuple(cid.split("."))


def fs_dataset_relpath(canonical_id: str) -> str:
    """Relative lake dataset path using ``/`` (e.g. ``noaa/storm_events``)."""
    return "/".join(fs_dataset_parts(canonical_id))


def medallion_prefix(destination_dataset: str | None) -> str:
    """``destination.dataset`` is the medallion prefix only (default ``bronze``)."""
    raw = (destination_dataset or "bronze").strip()
    return raw or "bronze"


def sql_schema_name(medallion: str, provider: str) -> str:
    """SQL schema for DuckDB/Postgres: ``{medallion}_{provider}``."""
    return f"{medallion}_{provider}"


def sql_table_name(source_name: str) -> str:
    """SQL table leaf name (the source segment)."""
    return source_name


def qualified_sql_table(medallion: str, provider: str, source_name: str) -> str:
    return f"{sql_schema_name(medallion, provider)}.{sql_table_name(source_name)}"


def default_schema_path(canonical_id: str) -> str:
    """Default JSON Schema path: ``schemas/{provider}/{source}/{source}.schema.yaml``."""
    _, source_name = parse_canonical_id(canonical_id)
    parts = canonical_id.split(".")
    # schemas/noaa/storm_events/storm_events.schema.yaml
    return "/".join(("schemas", *parts, f"{source_name}.schema.yaml"))


def dbt_model_slug(canonical_id: str) -> str:
    """Safe dbt model stem: ``noaa.storm_events`` → ``noaa_storm_events``."""
    return validate_canonical_id(canonical_id).replace(".", "_")


def sql_names_for_config(config: PipelineConfig) -> tuple[str, str]:
    """Return ``(sql_schema, sql_table)`` for a pipeline's DuckDB/Postgres bronze."""
    provider, source_name = parse_canonical_id(config.canonical_id)
    schema = sql_schema_name(medallion_prefix(config.destination.dataset), provider)
    return schema, sql_table_name(source_name)
