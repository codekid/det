from __future__ import annotations

import re
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

_STG_COL_ID = re.compile(r"^[a-z][a-z0-9_]*$")


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


def _require_dbt_col_id(name: str, *, where: str) -> str:
    if not _STG_COL_ID.match(name):
        raise ValueError(
            f"{where}: column id must be snake_case identifier, got {name!r}"
        )
    return name


class DbtSilverConfig(BaseModel):
    """Knobs for `det scaffold-dbt` silver model generation + column tests."""

    materialized: Literal["table", "incremental", "view"] = "table"
    unique_key: list[str] = Field(default_factory=lambda: ["__row_hash"])
    order_by: list[str] = Field(
        default_factory=lambda: ["__extract_run_datetime desc"]
    )
    incremental_strategy: Literal["delete+insert", "append", "merge"] = "delete+insert"
    watermark: str = "__extract_run_datetime"
    lookback: str | None = None
    # Prefer tests on silver (materialized/deduped) over stg views on large lakes.
    not_null: list[str] = Field(default_factory=list)
    unique: list[str] = Field(default_factory=list)
    accepted_values: dict[str, list[str]] = Field(default_factory=dict)

    @field_validator("unique_key", "order_by", "not_null", "unique", mode="before")
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

    @model_validator(mode="after")
    def _validate_silver_tests(self) -> DbtSilverConfig:
        for name in (*self.not_null, *self.unique):
            _require_dbt_col_id(name, where="dbt.silver test column")
        for name, values in self.accepted_values.items():
            _require_dbt_col_id(name, where="dbt.silver.accepted_values key")
            if not values:
                raise ValueError(
                    f"dbt.silver.accepted_values[{name!r}] must be a non-empty list"
                )
        return self


class DbtStgConfig(BaseModel):
    """
    Knobs for `det scaffold-dbt` staging generation.

    Adapt wire-faithful bronze in stg (coalesce renames, sentinels, maps).
    Column tests belong under ``dbt.silver`` (cheaper on large lakes).
    Does not rewrite bronze on load/migrate. Nested flatten is not supported here.
    """

    coalesce: dict[str, list[str]] = Field(default_factory=dict)
    null_sentinels: dict[str, list[Any]] = Field(default_factory=dict)
    rename: dict[str, str] = Field(default_factory=dict)
    exclude: list[str] = Field(default_factory=list)
    map: dict[str, dict[str, str]] = Field(default_factory=dict)

    @field_validator("exclude", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        if isinstance(v, list):
            return [str(x) for x in v]
        raise TypeError("expected string or list of strings")

    @model_validator(mode="after")
    def _validate_stg(self) -> DbtStgConfig:
        for canonical, sources in self.coalesce.items():
            _require_dbt_col_id(canonical, where="dbt.stg.coalesce key")
            if not sources:
                raise ValueError(
                    f"dbt.stg.coalesce[{canonical!r}] must be a non-empty list"
                )
            for src in sources:
                _require_dbt_col_id(src, where=f"dbt.stg.coalesce[{canonical!r}]")
        for name, sentinels in self.null_sentinels.items():
            _require_dbt_col_id(name, where="dbt.stg.null_sentinels key")
            if not sentinels:
                raise ValueError(
                    f"dbt.stg.null_sentinels[{name!r}] must be a non-empty list"
                )
        seen_targets: set[str] = set()
        for src, dest in self.rename.items():
            _require_dbt_col_id(src, where="dbt.stg.rename key")
            _require_dbt_col_id(dest, where="dbt.stg.rename value")
            if dest in seen_targets:
                raise ValueError(
                    f"dbt.stg.rename: duplicate target column {dest!r}"
                )
            seen_targets.add(dest)
        for name in self.exclude:
            _require_dbt_col_id(name, where="dbt.stg.exclude")
            if name.startswith("__"):
                raise ValueError(
                    f"dbt.stg.exclude cannot drop meta column {name!r}"
                )
        for name, mapping in self.map.items():
            _require_dbt_col_id(name, where="dbt.stg.map key")
            if not mapping:
                raise ValueError(f"dbt.stg.map[{name!r}] must be a non-empty mapping")
            for k, v in mapping.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise ValueError(
                        f"dbt.stg.map[{name!r}] values must be string→string"
                    )
        return self


class DbtConfig(BaseModel):
    silver: DbtSilverConfig = Field(default_factory=DbtSilverConfig)
    stg: DbtStgConfig = Field(default_factory=DbtStgConfig)


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
    # Wire era integer stamped on raw manifests. Bump only on true wire breaks
    # together with a new lake ``dataset:`` (see det-migrate skill).
    wire_version: int = 1

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
        if self.wire_version < 1:
            raise ValueError("wire_version must be a positive integer (>= 1)")
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
