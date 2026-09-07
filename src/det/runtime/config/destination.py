"""Destination and medallion config models."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from det.runtime.secrets import looks_like_secret_name

# Iceberg bronze partition profile (create-time only; not hive layout).
IcebergPartition = Literal["extract_run", "none"]

# Maintain duration fields: Nh / Nd (same family as catch-up lookback).
_ICEBERG_DURATION = re.compile(r"^(\d+)([HhDd])$")


def _require_iceberg_duration(value: str | None, *, where: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _ICEBERG_DURATION.fullmatch(text)
    if not match:
        raise ValueError(f"{where} must look like '48h' or '7d', got {value!r}")
    if int(match.group(1)) < 1:
        raise ValueError(f"{where} must be >= 1, got {value!r}")
    return text


class IcebergMaintainConfig(BaseModel):
    """Airflow/engine maintain knobs — DET validates only; never executes Spark."""

    model_config = ConfigDict(extra="forbid")

    # validate_default so YAML defaults stay checked without entering model_fields_set
    # (partial pipeline overrides merge on fields_set only — see resolve_maintain_config).
    expire_older_than: str | None = Field(default="7d", validate_default=True)
    expire: bool = True
    rewrite_data: bool = False
    rewrite_manifests: bool = False
    remove_orphans_older_than: str | None = Field(default="3d", validate_default=True)
    z_order: list[str] = Field(default_factory=list)

    @field_validator("expire_older_than", "remove_orphans_older_than", mode="before")
    @classmethod
    def _duration_or_none(cls, v: Any, info: ValidationInfo) -> Any:
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return _require_iceberg_duration(
            v, where=f"destination.iceberg.maintain.{info.field_name}"
        )


class IcebergDestinationConfig(BaseModel):
    """Iceberg-only destination knobs (create props + external maintain plan)."""

    model_config = ConfigDict(extra="forbid")

    # Applied on create_table only; Airflow reconciles existing tables.
    table_properties: dict[str, str] = Field(default_factory=dict)
    # Omit to inherit fleet ``DET_ICEBERG_MAINTAIN_*`` defaults in the plan API.
    maintain: IcebergMaintainConfig | None = None

    @field_validator("table_properties", mode="before")
    @classmethod
    def _stringify_props(cls, v: Any) -> Any:
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError("destination.iceberg.table_properties must be a mapping")
        out: dict[str, str] = {}
        for key, val in v.items():
            name = str(key).strip()
            if not name:
                raise ValueError("destination.iceberg.table_properties keys must be non-empty")
            out[name] = str(val)
        return out


class DestinationConfig(BaseModel):
    # Lake bronze default. ``filesystem`` is explicit JSONL (thin/dev).
    type: Literal["filesystem", "duckdb", "postgres", "iceberg"] = "iceberg"
    # Rare per-pipeline lake override. Omit in YAML; DET resolves
    # --lake-path > path > DET_LAKE_PATH > ./data/lake.
    path: str | None = None
    # Medallion prefix for SQL destinations (default bronze) → schema bronze_{provider}.
    # Not the lake dataset path and not the final SQL schema name.
    dataset: str | None = None
    # duckdb: database file path (required). postgres: DSN. iceberg: unused
    # (Hadoop catalog is the lake root).
    connection: str | None = None
    # postgres only: name of the env var holding the DSN, mirroring auth_env on a
    # source. Preferred over connection so credentials never live in committed YAML.
    connection_env: str | None = None
    # Iceberg only: identity on ``__extract_run_datetime`` (ETL default) or
    # unpartitioned. Applied on create_table; live mismatch hard-fails until
    # ``det migrate --recreate-iceberg`` or a manual wipe. Forbidden on other types.
    partition: IcebergPartition | None = None
    # Iceberg only: create-time table props + maintain plan knobs for external runners.
    iceberg: IcebergDestinationConfig | None = None

    @model_validator(mode="after")
    def connection_required_for_db_destinations(self) -> DestinationConfig:
        connection = (self.connection or "").strip()
        connection_env = (self.connection_env or "").strip()
        if connection_env:
            if self.type != "postgres":
                raise ValueError(
                    "destination.connection_env is only supported when "
                    f"destination.type is postgres, got {self.type}"
                )
            if connection:
                raise ValueError(
                    "set destination.connection or destination.connection_env, "
                    "not both (ambiguous which one holds the DSN)"
                )
            if not looks_like_secret_name(connection_env):
                raise ValueError(
                    "destination.connection_env must be an env var name like "
                    f"DET_POSTGRES_DSN, got {connection_env!r}"
                )
            return self
        if self.type in {"duckdb", "postgres"} and not connection:
            if self.type == "duckdb":
                raise ValueError(
                    "destination.connection is required when destination.type is "
                    "duckdb (DuckDB file path)"
                )
            raise ValueError(
                "destination.connection_env (env var name holding the DSN) is "
                "required when destination.type is postgres"
            )
        return self

    @model_validator(mode="after")
    def partition_iceberg_only(self) -> DestinationConfig:
        if self.partition is not None and self.type != "iceberg":
            raise ValueError(
                "destination.partition is only supported when destination.type is "
                f"iceberg, got {self.type}"
            )
        if self.type == "iceberg" and self.partition is None:
            self.partition = "extract_run"
        return self

    @model_validator(mode="after")
    def iceberg_block_iceberg_only(self) -> DestinationConfig:
        if self.iceberg is not None and self.type != "iceberg":
            raise ValueError(
                "destination.iceberg is only supported when destination.type is "
                f"iceberg, got {self.type}"
            )
        return self

    @property
    def iceberg_partition(self) -> IcebergPartition:
        """Resolved Iceberg partition profile (default extract_run)."""
        if self.type != "iceberg":
            raise ValueError(
                f"iceberg_partition requires destination.type iceberg, got {self.type}"
            )
        return self.partition or "extract_run"

    @property
    def iceberg_table_properties(self) -> dict[str, str]:
        """Create-time Iceberg table properties (empty when unset / non-iceberg)."""
        if self.type != "iceberg" or self.iceberg is None:
            return {}
        return dict(self.iceberg.table_properties)


class MedallionConfig(BaseModel):
    bronze_prefix: str = "bronze"
    raw_prefix: str = "raw"
