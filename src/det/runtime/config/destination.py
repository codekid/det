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

# DET replace-by-extract-run grain (Iceberg create-time profile + BigQuery default
# time partition). Not hive layout and not warehouse source-column layout.
RunGrainPartition = Literal["extract_run", "none"]
# Back-compat alias — Iceberg writers historically imported this name.
IcebergPartition = RunGrainPartition

# Payload / meta column ids for destination.bigquery.partition_by / cluster_by.
_BQ_COL_OR_META = re.compile(r"^(?:[a-z][a-z0-9_]*|__[a-z][a-z0-9_]*)$")

# Maintain duration fields: Nh / Nd (same family as catch-up lookback).
_ICEBERG_DURATION = re.compile(r"^(\d+)([HhDd])$")


def _require_bq_col_or_meta(name: str, *, where: str) -> str:
    text = str(name).strip()
    if not _BQ_COL_OR_META.match(text):
        raise ValueError(
            f"{where}: column id must be snake_case or DET meta (__*), got {name!r}"
        )
    return text


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


class BigQueryDestinationPartitionConfig(BaseModel):
    """BigQuery physical time partition (source/query grain — not DET run grain)."""

    model_config = ConfigDict(extra="forbid")

    field: str
    # Integer range partitioning needs start/end/interval; bronze supports time types.
    data_type: Literal["timestamp", "date", "datetime"] = "timestamp"
    granularity: Literal["hour", "day", "month", "year"] = "day"

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        if out.get("granularity") is None:
            out["granularity"] = "day"
        data_type = out.get("data_type", "timestamp")
        if data_type == "date" and out.get("granularity") == "hour":
            raise ValueError(
                "destination.bigquery.partition_by.granularity 'hour' is not valid "
                "when data_type is date"
            )
        return out

    @model_validator(mode="after")
    def _validate_field(self) -> BigQueryDestinationPartitionConfig:
        _require_bq_col_or_meta(
            self.field, where="destination.bigquery.partition_by.field"
        )
        return self


class BigQueryDestinationConfig(BaseModel):
    """BigQuery-only bronze layout (physical partition + clustering)."""

    model_config = ConfigDict(extra="forbid")

    # Source/query grain. When set, wins over destination.partition extract_run default.
    partition_by: BigQueryDestinationPartitionConfig | None = None
    cluster_by: list[str] = Field(default_factory=list)
    require_partition_filter: bool = False

    @field_validator("cluster_by", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            text = v.strip()
            return [text] if text else []
        if not isinstance(v, list):
            raise ValueError("destination.bigquery.cluster_by must be a list of column names")
        out: list[str] = []
        for item in v:
            name = str(item).strip()
            if name:
                out.append(name)
        return out

    @model_validator(mode="after")
    def _validate_layout(self) -> BigQueryDestinationConfig:
        if len(self.cluster_by) > 4:
            raise ValueError(
                "destination.bigquery.cluster_by supports at most 4 columns (BigQuery limit)"
            )
        for name in self.cluster_by:
            _require_bq_col_or_meta(name, where="destination.bigquery.cluster_by")
        if self.require_partition_filter and self.partition_by is None:
            # extract_run default still partitions; require_partition_filter needs an
            # explicit partition_by field so the filter column is unambiguous in YAML.
            raise ValueError(
                "destination.bigquery.require_partition_filter requires partition_by"
            )
        return self


class DestinationConfig(BaseModel):
    # Lake bronze default. ``filesystem`` is explicit JSONL (thin/dev).
    # ``bigquery`` lands bronze in BQ (project from DET_GCP_PROJECT; dataset
    # derives as {prefix}_{provider} unless bq_dataset is set).
    type: Literal["filesystem", "duckdb", "postgres", "iceberg", "bigquery"] = "iceberg"
    # Ignored for lake roots (layout 2). Kept so existing YAML still loads;
    # ``det check`` warns when set. Use DET_LAKE_PATH / --lake-path /
    # DET_LAKE_PATH_{RAW,BRONZE,OPS} — never destination.path.
    path: str | None = Field(
        default=None,
        description=(
            "Ignored: does not select or override the lake root. Use "
            "DET_LAKE_PATH / --lake-path or DET_LAKE_PATH_RAW/_BRONZE/_OPS."
        ),
    )
    # Medallion prefix for SQL destinations (default bronze) → schema bronze_{provider}.
    # Not the lake dataset path and not the final SQL schema name.
    dataset: str | None = None
    # duckdb: database file path (required). postgres: DSN. iceberg/bigquery: unused
    # (Hadoop catalog is the lake root; BQ project comes from DET_GCP_PROJECT).
    connection: str | None = None
    # postgres only: name of the env var holding the DSN, mirroring auth_env on a
    # source. Preferred over connection so credentials never live in committed YAML.
    connection_env: str | None = None
    # bigquery only: full dataset id override (e.g. my_exploration). When unset,
    # dataset derives as {DET_BQ_DATASET_PREFIX or bronze}_{provider}.
    # Project never lives in YAML — use DET_GCP_PROJECT.
    bq_dataset: str | None = None
    # DET run grain for iceberg + bigquery: ``extract_run`` (default) or ``none``.
    # Iceberg: create-time identity transforms. BigQuery: default time partition on
    # ``__extract_run_datetime`` unless ``bigquery.partition_by`` overrides.
    # Forbidden on filesystem/duckdb/postgres.
    partition: RunGrainPartition | None = None
    # Iceberg only: create-time table props + maintain plan knobs for external runners.
    iceberg: IcebergDestinationConfig | None = None
    # BigQuery only: physical layout (source partition_by / cluster_by).
    bigquery: BigQueryDestinationConfig | None = None

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
    def partition_run_grain_destinations(self) -> DestinationConfig:
        if self.partition is not None and self.type not in {"iceberg", "bigquery"}:
            raise ValueError(
                "destination.partition is only supported when destination.type is "
                f"iceberg or bigquery, got {self.type}"
            )
        if self.type in {"iceberg", "bigquery"} and self.partition is None:
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

    @model_validator(mode="after")
    def bigquery_block_bigquery_only(self) -> DestinationConfig:
        if self.bigquery is not None and self.type != "bigquery":
            raise ValueError(
                "destination.bigquery is only supported when destination.type is "
                f"bigquery, got {self.type}"
            )
        return self

    @model_validator(mode="after")
    def bq_dataset_bigquery_only(self) -> DestinationConfig:
        raw = (self.bq_dataset or "").strip()
        if raw and self.type != "bigquery":
            raise ValueError(
                "destination.bq_dataset is only supported when destination.type is "
                f"bigquery, got {self.type}"
            )
        if self.bq_dataset is not None:
            self.bq_dataset = raw or None
        return self

    @property
    def run_grain_partition(self) -> RunGrainPartition:
        """Resolved DET run grain (default extract_run) for iceberg/bigquery."""
        if self.type not in {"iceberg", "bigquery"}:
            raise ValueError(
                "run_grain_partition requires destination.type iceberg or bigquery, "
                f"got {self.type}"
            )
        return self.partition or "extract_run"

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
