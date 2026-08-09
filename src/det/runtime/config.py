from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from det.runtime.ids import (
    default_schema_path,
    fs_dataset_relpath,
    validate_canonical_id,
)
from det.runtime.naming import BronzeConfig


class SourceConfig(BaseModel):
    type: str
    overrides: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type")
    @classmethod
    def type_is_canonical(cls, v: str) -> str:
        return validate_canonical_id(v)


class ValidationConfig(BaseModel):
    engine: Literal["jsonschema"] = "jsonschema"


class IngestionConfig(BaseModel):
    library: Literal["dlt", "thin"] = "dlt"


class DestinationConfig(BaseModel):
    type: Literal["filesystem", "duckdb", "postgres"] = "filesystem"
    path: str = "./data/lake"
    # Medallion prefix for SQL destinations (default bronze) → schema bronze_{provider}.
    # Not the lake dataset path and not the final SQL schema name.
    dataset: str | None = None
    # duckdb: database file path (required). postgres: DSN when implemented.
    connection: str | None = None

    @model_validator(mode="after")
    def connection_required_for_db_destinations(self) -> DestinationConfig:
        if self.type in {"duckdb", "postgres"} and not (
            self.connection and str(self.connection).strip()
        ):
            kind = "DuckDB file path" if self.type == "duckdb" else "Postgres DSN"
            raise ValueError(
                f"destination.connection is required when destination.type is "
                f"{self.type} ({kind})"
            )
        return self


class MedallionConfig(BaseModel):
    bronze_prefix: str = "bronze"
    raw_prefix: str = "raw"


class DbtSilverConfig(BaseModel):
    """Knobs for `det scaffold-dbt` silver model generation."""

    materialized: Literal["table", "incremental", "view"] = "table"
    unique_key: list[str] = Field(default_factory=lambda: ["__row_hash"])
    order_by: list[str] = Field(
        default_factory=lambda: ["__extract_run_datetime desc"]
    )
    incremental_strategy: Literal["delete+insert", "append", "merge"] = "delete+insert"
    watermark: str = "__extract_run_datetime"
    lookback: str | None = None

    @field_validator("unique_key", "order_by", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        if isinstance(v, list):
            return [str(x) for x in v]
        raise TypeError("expected string or list of strings")

    @field_validator("unique_key")
    @classmethod
    def _unique_key_nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("dbt.silver.unique_key must be non-empty")
        return v

    @field_validator("order_by")
    @classmethod
    def _order_by_nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("dbt.silver.order_by must be non-empty")
        return v


class DbtConfig(BaseModel):
    silver: DbtSilverConfig = Field(default_factory=DbtSilverConfig)


class PipelineConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    source: SourceConfig
    # YAML key remains `schema:`; omit to use default_schema_path(name).
    schema_path: str = Field(alias="schema")
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    destination: DestinationConfig = Field(default_factory=DestinationConfig)
    medallion: MedallionConfig = Field(default_factory=MedallionConfig)
    bronze: BronzeConfig = Field(default_factory=BronzeConfig)
    dbt: DbtConfig = Field(default_factory=DbtConfig)
    # Optional override of the lake dataset id (defaults to name / source.type).
    dataset: str | None = None

    @model_validator(mode="before")
    @classmethod
    def fill_default_schema_path(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        name = data.get("name")
        schema = data.get("schema")
        if (schema is None or schema == "") and isinstance(name, str) and name.strip():
            data = {**data, "schema": default_schema_path(name)}
        return data

    @field_validator("name")
    @classmethod
    def name_is_canonical(cls, v: str) -> str:
        return validate_canonical_id(v)

    @field_validator("schema_path")
    @classmethod
    def schema_nonempty(cls, v: str) -> str:
        if not str(v).strip():
            raise ValueError("schema path must be non-empty")
        return v

    @model_validator(mode="after")
    def name_matches_source(self) -> PipelineConfig:
        if self.name != self.source.type:
            raise ValueError(
                f"pipeline name {self.name!r} must equal source.type "
                f"{self.source.type!r}"
            )
        if self.dataset is not None:
            validate_canonical_id(self.dataset)
        return self

    @property
    def canonical_id(self) -> str:
        return self.dataset or self.name

    def bronze_dataset(self) -> str:
        """Canonical lake dataset id (``provider.source``). Prefer path helpers."""
        return self.canonical_id

    def fs_dataset_relpath(self) -> str:
        return fs_dataset_relpath(self.canonical_id)


def apply_overrides(raw: dict[str, Any], assignments: Sequence[str]) -> dict[str, Any]:
    """
    Apply `dotted.key=value` assignments over a parsed pipeline mapping.

    Values are parsed as YAML so ints, bools, and null work as expected.
    """
    out = deepcopy(raw)
    for assignment in assignments:
        key, sep, value = assignment.partition("=")
        if not sep:
            raise ValueError(f"Override must be dotted.key=value, got: {assignment!r}")
        parts = [p for p in key.strip().split(".") if p]
        if not parts:
            raise ValueError(f"Override key must be non-empty: {assignment!r}")
        cursor: dict[str, Any] = out
        for part in parts[:-1]:
            existing = cursor.get(part)
            if not isinstance(existing, dict):
                existing = {}
                cursor[part] = existing
            cursor = existing
        cursor[parts[-1]] = yaml.safe_load(value)
    return out


def load_pipeline_config(
    path: Path | str,
    overrides: Sequence[str] | None = None,
) -> PipelineConfig:
    p = Path(path)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Pipeline config must be a mapping: {p}")
    if overrides:
        raw = apply_overrides(raw, overrides)
    return PipelineConfig.model_validate(raw)


def resolve_path(base: Path, maybe_relative: str) -> Path:
    path = Path(maybe_relative)
    if path.is_absolute():
        return path
    return (base / path).resolve()
