from __future__ import annotations

import re
from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

from det.runtime.ids import (
    default_schema_path,
    fs_dataset_relpath,
    lake_dataset_id,
    validate_canonical_id,
)
from det.runtime.naming import BronzeConfig
from det.runtime.secrets import looks_like_secret_name
from det.runtime.slo import SloConfig

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
    # ``det`` is the multi-destination bronze writer. ``dlt`` is a deprecated alias
    # for the same backend (dlt never lands bronze). ``thin`` is filesystem-only.
    library: Literal["det", "dlt", "thin"] = "det"
    # SQL INSERT batch size and JSONL flush cadence. Coerce/validate stay per-row.
    chunk_rows: int = Field(default=10_000, ge=1)


# Iceberg bronze partition profile (create-time only; not hive layout).
IcebergPartition = Literal["extract_run", "none"]


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
    # unpartitioned. Applied on create_table; existing tables keep their live spec.
    # Omit when type is iceberg → extract_run. Forbidden on other destination types.
    partition: IcebergPartition | None = None

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

    @property
    def iceberg_partition(self) -> IcebergPartition:
        """Resolved Iceberg partition profile (default extract_run)."""
        if self.type != "iceberg":
            raise ValueError(
                f"iceberg_partition requires destination.type iceberg, got {self.type}"
            )
        return self.partition or "extract_run"


class MedallionConfig(BaseModel):
    bronze_prefix: str = "bronze"
    raw_prefix: str = "raw"


def _require_dbt_col_id(name: str, *, where: str) -> str:
    if not _STG_COL_ID.match(name):
        raise ValueError(
            f"{where}: column id must be snake_case identifier, got {name!r}"
        )
    return name


def _require_relative_path(key: str, *, where: str) -> None:
    """Validate a relative dotted path (each segment snake_case)."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError(f"{where}: relative path must be a non-empty string")
    if key.startswith(".") or key.endswith(".") or ".." in key:
        raise ValueError(f"{where}: invalid relative path {key!r}")
    parts = key.split(".")
    if not parts or any(not p for p in parts):
        raise ValueError(f"{where}: invalid relative path {key!r}")
    for part in parts:
        _require_dbt_col_id(part, where=where)


def _validate_adapt_knob_keys(
    *,
    coalesce: dict[str, list[str]],
    null_sentinels: dict[str, list[Any]],
    map: dict[str, dict[str, str]],
    rename: dict[str, str],
    exclude: list[str],
    not_null: list[str],
    unique: list[str],
    accepted_values: dict[str, list[str]],
    children: dict[str, Any],
    where: str,
) -> None:
    for key, sources in coalesce.items():
        _require_relative_path(key, where=f"{where}.coalesce key")
        if not sources:
            raise ValueError(f"{where}.coalesce[{key!r}] must be a non-empty list")
        for src in sources:
            _require_relative_path(src, where=f"{where}.coalesce[{key!r}]")
    for key, sentinels in null_sentinels.items():
        _require_relative_path(key, where=f"{where}.null_sentinels key")
        if not sentinels:
            raise ValueError(
                f"{where}.null_sentinels[{key!r}] must be a non-empty list"
            )
    for key, mapping in map.items():
        _require_relative_path(key, where=f"{where}.map key")
        if not mapping:
            raise ValueError(f"{where}.map[{key!r}] must be a non-empty mapping")
    for key, dest in rename.items():
        _require_relative_path(key, where=f"{where}.rename key")
        _require_dbt_col_id(dest, where=f"{where}.rename value")
    for key in exclude:
        _require_relative_path(key, where=f"{where}.exclude")
    for key in not_null:
        _require_relative_path(key, where=f"{where}.not_null")
    for key in unique:
        _require_relative_path(key, where=f"{where}.unique")
    for key, values in accepted_values.items():
        _require_relative_path(key, where=f"{where}.accepted_values key")
        if not values:
            raise ValueError(
                f"{where}.accepted_values[{key!r}] must be a non-empty list"
            )
    for name in children:
        _require_dbt_col_id(name, where=f"{where} child scope")


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


class FlattenConfig(BaseModel):
    """
    Struct flatten knobs for stg (and per-relation flatten).

    ``depth`` null/omit = unlimited object levels. Arrays are never auto-exploded.
    """

    depth: int | None = None
    include: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)

    @field_validator("include", "exclude", mode="before")
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
    def _validate_flatten(self) -> FlattenConfig:
        if self.depth is not None and self.depth < 1:
            raise ValueError("dbt.stg.flatten.depth must be >= 1 when set")
        for name in (*self.include, *self.exclude):
            _require_dbt_col_id(name, where="dbt.stg.flatten include/exclude")
        overlap = set(self.include) & set(self.exclude)
        if overlap:
            raise ValueError(
                f"dbt.stg.flatten include/exclude overlap: {sorted(overlap)}"
            )
        return self


_ADAPT_KNOB_KEYS = frozenset(
    {
        "coalesce",
        "null_sentinels",
        "map",
        "rename",
        "exclude",
        "not_null",
        "unique",
        "accepted_values",
        "children",
    }
)

_RELATION_RESERVED = frozenset(
    {
        "path",
        "materialized",
        "parent_key",
        "flatten",
        "relations",
        *_ADAPT_KNOB_KEYS,
    }
)


def _as_str_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [v]
    if isinstance(v, list):
        return [str(x) for x in v]
    raise TypeError("expected string or list of strings")


def _split_adapt_children(
    data: dict[str, Any],
    *,
    reserved: frozenset[str],
) -> dict[str, Any]:
    """Move non-reserved mapping keys into ``children`` AdaptScope nodes."""
    out: dict[str, Any] = {}
    children: dict[str, Any] = {}
    for key, value in data.items():
        if key in reserved:
            out[key] = value
            continue
        if not isinstance(value, dict):
            raise ValueError(
                f"nested adapt scope {key!r} must be a mapping, got {type(value).__name__}"
            )
        if key in _ADAPT_KNOB_KEYS:
            raise ValueError(f"invalid nested scope name {key!r} (reserved knob)")
        children[key] = value
    if children:
        existing = out.get("children")
        if isinstance(existing, dict):
            merged = {**existing, **children}
            out["children"] = merged
        else:
            out["children"] = children
    return out


class AdaptScope(BaseModel):
    """
    Path-scoped stg adaptations / relation tests.

    Knobs use **relative** names only. Any other mapping key is a child property
    scope (e.g. ``geo:`` under ``shipping_address``).
    """

    coalesce: dict[str, list[str]] = Field(default_factory=dict)
    null_sentinels: dict[str, list[Any]] = Field(default_factory=dict)
    map: dict[str, dict[str, str]] = Field(default_factory=dict)
    rename: dict[str, str] = Field(default_factory=dict)
    exclude: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    unique: list[str] = Field(default_factory=list)
    accepted_values: dict[str, list[str]] = Field(default_factory=dict)
    children: dict[str, AdaptScope] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _split_children(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return _split_adapt_children(data, reserved=_ADAPT_KNOB_KEYS)

    @field_validator("exclude", "not_null", "unique", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        return _as_str_list(v)

    @model_validator(mode="after")
    def _validate_scope(self) -> AdaptScope:
        _validate_adapt_knob_keys(
            coalesce=self.coalesce,
            null_sentinels=self.null_sentinels,
            map=self.map,
            rename=self.rename,
            exclude=self.exclude,
            not_null=self.not_null,
            unique=self.unique,
            accepted_values=self.accepted_values,
            children=self.children,
            where="dbt.stg.fields",
        )
        return self


class RelationConfig(BaseModel):
    """
    Explicit array → child stg/silver model.

    Root ``path`` is a top-level bronze array property. Nested ``relations`` use a
    ``path`` relative to the parent array item. ``materialized`` applies to both
    stg and silver (default view). Adapt/test knobs are relative to the item;
    other mapping keys are nested property scopes (same as AdaptScope).
    """

    path: str | None = None
    materialized: Literal["view", "table"] = "view"
    parent_key: str | None = None
    flatten: FlattenConfig = Field(default_factory=FlattenConfig)
    relations: dict[str, RelationConfig] = Field(default_factory=dict)
    coalesce: dict[str, list[str]] = Field(default_factory=dict)
    null_sentinels: dict[str, list[Any]] = Field(default_factory=dict)
    map: dict[str, dict[str, str]] = Field(default_factory=dict)
    rename: dict[str, str] = Field(default_factory=dict)
    exclude: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    unique: list[str] = Field(default_factory=list)
    accepted_values: dict[str, list[str]] = Field(default_factory=dict)
    children: dict[str, AdaptScope] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _split_children(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return _split_adapt_children(data, reserved=_RELATION_RESERVED)

    @field_validator("exclude", "not_null", "unique", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        return _as_str_list(v)

    @model_validator(mode="after")
    def _validate_relation(self) -> RelationConfig:
        if self.path is not None:
            _require_dbt_col_id(self.path, where="dbt.stg.relations.path")
            if "." in self.path:
                raise ValueError(
                    "dbt.stg.relations.path must be a single property name "
                    f"(no dots); nest via relations: for deeper arrays, got {self.path!r}"
                )
        if self.parent_key is not None:
            _require_dbt_col_id(self.parent_key, where="dbt.stg.relations.parent_key")
        _validate_adapt_knob_keys(
            coalesce=self.coalesce,
            null_sentinels=self.null_sentinels,
            map=self.map,
            rename=self.rename,
            exclude=self.exclude,
            not_null=self.not_null,
            unique=self.unique,
            accepted_values=self.accepted_values,
            children=self.children,
            where="dbt.stg.relations",
        )
        return self

    def to_adapt_scope(self) -> AdaptScope:
        return AdaptScope(
            coalesce=self.coalesce,
            null_sentinels=self.null_sentinels,
            map=self.map,
            rename=self.rename,
            exclude=self.exclude,
            not_null=self.not_null,
            unique=self.unique,
            accepted_values=self.accepted_values,
            children=self.children,
        )


def _normalize_relations(
    rels: dict[str, RelationConfig],
    *,
    where: str,
) -> dict[str, RelationConfig]:
    """Default path to relation key; recurse into nested relations."""
    fixed: dict[str, RelationConfig] = {}
    for name, rel in rels.items():
        _require_dbt_col_id(name, where=f"{where} key")
        path = rel.path or name
        _require_dbt_col_id(path, where=f"{where}[{name!r}].path")
        if "." in path:
            raise ValueError(
                f"{where}[{name!r}].path must be a single property name, got {path!r}"
            )
        nested = _normalize_relations(
            rel.relations, where=f"{where}[{name!r}].relations"
        )
        fixed[name] = rel.model_copy(update={"path": path, "relations": nested})
    return fixed


class ViewWarnConfig(BaseModel):
    """Advisory lake sample thresholds for view-materialized relations."""

    enabled: bool = True
    sample_rows: int = 5000
    parent_rows: int = 500_000
    child_rows: int = 2_000_000

    @model_validator(mode="after")
    def _validate_thresholds(self) -> ViewWarnConfig:
        if self.sample_rows < 1:
            raise ValueError("dbt.stg.view_warn.sample_rows must be >= 1")
        if self.parent_rows < 1:
            raise ValueError("dbt.stg.view_warn.parent_rows must be >= 1")
        if self.child_rows < 1:
            raise ValueError("dbt.stg.view_warn.child_rows must be >= 1")
        return self


class DbtStgConfig(BaseModel):
    """
    Knobs for `det scaffold-dbt` staging generation.

    Root coalesce/map/rename/exclude/null_sentinels apply to top-level scalars.
    Nested struct adaptations live under ``fields`` (path-scoped, relative keys).
    Relation adaptations/tests live on each ``relations`` entry.
    """

    flatten: FlattenConfig = Field(default_factory=FlattenConfig)
    relations: dict[str, RelationConfig] = Field(default_factory=dict)
    view_warn: ViewWarnConfig = Field(default_factory=ViewWarnConfig)
    fields: dict[str, AdaptScope] = Field(default_factory=dict)
    coalesce: dict[str, list[str]] = Field(default_factory=dict)
    null_sentinels: dict[str, list[Any]] = Field(default_factory=dict)
    rename: dict[str, str] = Field(default_factory=dict)
    exclude: list[str] = Field(default_factory=list)
    map: dict[str, dict[str, str]] = Field(default_factory=dict)

    @field_validator("exclude", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        return _as_str_list(v)

    @model_validator(mode="after")
    def _validate_stg(self) -> DbtStgConfig:
        fixed_relations = _normalize_relations(
            self.relations, where="dbt.stg.relations"
        )
        for name in self.fields:
            _require_dbt_col_id(name, where="dbt.stg.fields key")
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
        object.__setattr__(self, "relations", fixed_relations)
        return self


class DbtDocsConfig(BaseModel):
    """
    Consumer-facing column docs for scaffolded silver YAML.

    Keys are **post-stg** column names (after rename/coalesce). Wire-field docs
    live on the JSON Schema; ``dbt.stg`` stays transforms-only.
    """

    columns: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_docs(self) -> DbtDocsConfig:
        for name, text in self.columns.items():
            _require_dbt_col_id(name, where="dbt.docs.columns key")
            if not isinstance(text, str) or not text.strip():
                raise ValueError(
                    f"dbt.docs.columns[{name!r}] must be a non-empty string"
                )
        return self


class DbtConfig(BaseModel):
    silver: DbtSilverConfig = Field(default_factory=DbtSilverConfig)
    stg: DbtStgConfig = Field(default_factory=DbtStgConfig)
    docs: DbtDocsConfig = Field(default_factory=DbtDocsConfig)


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
    # Opt-in fleet SLOs (dbt tests). Omit → pipeline is not in ops_slo_expected.
    slo: SloConfig | None = None
    # Rejected if set: lake ids are always ``{name}_v{wire_version}``. Use
    # ``wire_version`` (and ``det migrate``) for cutovers.
    dataset: str | None = None
    # Wire-era stamp and lake id suffix. Lake paths/SQL use ``{name}_vN`` always
    # (including ``_v1``). Bump when the extract payload shape changes
    # incompatibly; rebuild older raw with ``det migrate --from-raw …_vN``.
    wire_version: int = 1
    # Programmatic lake id override (e.g. ``det migrate --to-bronze``). Not YAML.
    _lake_id: str | None = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def fill_default_schema_path(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        name = data.get("name")
        schema = data.get("schema")
        if schema is None or schema == "":
            schema = data.get("schema_path")
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
            raise ValueError(
                "pipeline top-level 'dataset:' is no longer supported; lake ids are "
                f"derived as {{name}}_v{{wire_version}} "
                f"(e.g. {self.name}_v{self.wire_version}). "
                "Bump wire_version for a cutover."
            )
        if self.wire_version < 1:
            raise ValueError("wire_version must be a positive integer (>= 1)")
        return self

    @property
    def canonical_id(self) -> str:
        """Lake / SQL dataset id: ``{name}_v{wire_version}`` (or migrate override)."""
        if self._lake_id is not None:
            return validate_canonical_id(self._lake_id)
        return lake_dataset_id(self.name, self.wire_version)

    def bronze_dataset(self) -> str:
        """Lake dataset id (``provider.source_vN``). Prefer path helpers."""
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
