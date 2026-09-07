"""dbt scaffold config models (silver, stg, docs)."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

_STG_COL_ID = re.compile(r"^[a-z][a-z0-9_]*$")
# Payload snake_case or DET meta columns (__extract_run_datetime, __row_hash, …).
_STG_COL_OR_META_ID = re.compile(r"^(?:[a-z][a-z0-9_]*|__[a-z][a-z0-9_]*)$")
# ``col`` or ``col asc|desc`` (column may be DET meta).
_SILVER_ORDER_BY = re.compile(
    r"^(?:[a-z][a-z0-9_]*|__[a-z][a-z0-9_]*)(?:\s+(?:asc|desc))?$",
    re.IGNORECASE,
)
# DuckDB/Postgres ``interval '…'`` fragment for incremental lookback.
_SILVER_LOOKBACK = re.compile(
    r"^\d+\s+(second|minute|hour|day|week|month|year)s?$",
    re.IGNORECASE,
)


def _require_dbt_col_id(name: str, *, where: str) -> str:
    if not _STG_COL_ID.match(name):
        raise ValueError(
            f"{where}: column id must be snake_case identifier, got {name!r}"
        )
    return name


def _require_dbt_col_or_meta_id(name: str, *, where: str) -> str:
    if not _STG_COL_OR_META_ID.match(name):
        raise ValueError(
            f"{where}: column id must be snake_case or DET meta (__*), got {name!r}"
        )
    return name


def _require_silver_order_by(fragment: str, *, where: str) -> str:
    text = fragment.strip()
    if not _SILVER_ORDER_BY.match(text):
        raise ValueError(
            f"{where}: expected 'col' or 'col asc|desc' "
            f"(snake_case or DET meta), got {fragment!r}"
        )
    return text


def _require_silver_lookback(value: str, *, where: str) -> str:
    text = value.strip()
    if not _SILVER_LOOKBACK.match(text):
        raise ValueError(
            f"{where}: expected '<n> <unit>' lookback "
            f"(e.g. '7 days', '1 hour'), got {value!r}"
        )
    return text


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


class BigQueryPartitionConfig(BaseModel):
    """dbt-bigquery ``partition_by`` dict for scaffolded silver models."""

    model_config = ConfigDict(extra="forbid")

    field: str
    data_type: Literal["timestamp", "date", "datetime", "int64"] = "timestamp"
    granularity: Literal["hour", "day", "month", "year"] | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_granularity(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        data_type = out.get("data_type", "timestamp")
        if data_type == "int64":
            if out.get("granularity") is not None:
                raise ValueError(
                    "dbt.*.bigquery.partition_by.granularity must be null "
                    "when data_type is int64"
                )
            out["granularity"] = None
        elif out.get("granularity") is None:
            out["granularity"] = "day"
        elif data_type == "date" and out.get("granularity") == "hour":
            raise ValueError(
                "dbt.*.bigquery.partition_by.granularity 'hour' is not valid "
                "when data_type is date"
            )
        return out

    @model_validator(mode="after")
    def _validate_partition(self) -> BigQueryPartitionConfig:
        _require_dbt_col_or_meta_id(
            self.field, where="dbt.*.bigquery.partition_by.field"
        )
        return self

    def to_dbt_dict(self) -> dict[str, str]:
        out: dict[str, str] = {"field": self.field, "data_type": self.data_type}
        if self.granularity is not None:
            out["granularity"] = self.granularity
        return out


class BigQuerySilverConfig(BaseModel):
    """Opt-in BigQuery table layout for scaffolded silver (parent or relation)."""

    model_config = ConfigDict(extra="forbid")

    partition_by: BigQueryPartitionConfig | None = None
    cluster_by: list[str] = Field(default_factory=list)
    require_partition_filter: bool = False

    @field_validator("cluster_by", mode="before")
    @classmethod
    def _as_str_list(cls, v: Any) -> list[str]:
        return _as_str_list(v)

    @model_validator(mode="after")
    def _validate_bigquery(self) -> BigQuerySilverConfig:
        if len(self.cluster_by) > 4:
            raise ValueError(
                "dbt.*.bigquery.cluster_by supports at most 4 columns (BigQuery limit)"
            )
        for name in self.cluster_by:
            _require_dbt_col_or_meta_id(name, where="dbt.*.bigquery.cluster_by")
        if self.require_partition_filter and self.partition_by is None:
            raise ValueError(
                "dbt.*.bigquery.require_partition_filter requires partition_by"
            )
        return self

    def has_layout(self) -> bool:
        return bool(
            self.partition_by is not None
            or self.cluster_by
            or self.require_partition_filter
        )


def _validate_bigquery_vs_materialized(
    bigquery: BigQuerySilverConfig | None,
    *,
    materialized: str,
    where: str,
) -> None:
    if bigquery is None or not bigquery.has_layout():
        return
    if materialized == "view":
        raise ValueError(
            f"{where}.bigquery partition/cluster requires materialized "
            "table or incremental (not view)"
        )


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
    bigquery: BigQuerySilverConfig | None = None
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

    @field_validator("unique_key")
    @classmethod
    def _unique_key_ids(cls, v: list[str]) -> list[str]:
        return [
            _require_dbt_col_or_meta_id(name, where="dbt.silver.unique_key")
            for name in v
        ]

    @field_validator("order_by")
    @classmethod
    def _order_by_fragments(cls, v: list[str]) -> list[str]:
        return [
            _require_silver_order_by(frag, where="dbt.silver.order_by") for frag in v
        ]

    @field_validator("watermark")
    @classmethod
    def _watermark_id(cls, v: str) -> str:
        return _require_dbt_col_or_meta_id(v, where="dbt.silver.watermark")

    @field_validator("lookback")
    @classmethod
    def _lookback_interval(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return _require_silver_lookback(v, where="dbt.silver.lookback")

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
        _validate_bigquery_vs_materialized(
            self.bigquery,
            materialized=self.materialized,
            where="dbt.silver",
        )
        return self


class FlattenConfig(BaseModel):
    """
    Struct flatten knobs for stg (and per-relation flatten).

    ``depth`` null/omit = unlimited object levels. Arrays are never auto-exploded.
    Closed set — unknown keys are rejected (``extra=forbid``).
    """

    model_config = ConfigDict(extra="forbid")

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
        "load",
        "parent_key",
        "grain",
        "flatten",
        "relations",
        "bigquery",
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
    scope (e.g. ``geo:`` under ``shipping_address``), rewritten into ``children``
    before ``extra=forbid`` runs.
    """

    model_config = ConfigDict(extra="forbid")

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
    ``path`` relative to the parent array item.

    Without ``load``, ``materialized`` applies to both stg and silver (default
    view). With ``load`` (``full_refresh`` | ``parent_replace``), stg stays a
    view and silver follows the load mapping — see
    ``det.scaffold.relation_load.resolve_relation_materialization``.

    ``grain`` lists item field names that identify a row at this nest level.
    Scaffold emits path-qualified spine columns (``line_items__line_id``). Empty
    grain falls back to ``{path}__index`` from unnest ordinality.

    Closed set — unknown keys are rejected after nested adapt scopes are
    rewritten into ``children``.
    """

    model_config = ConfigDict(extra="forbid")

    path: str | None = None
    materialized: Literal["view", "table"] = "view"
    load: Literal["full_refresh", "parent_replace"] | None = None
    parent_key: str | None = None
    grain: list[str] = Field(default_factory=list)
    flatten: FlattenConfig = Field(default_factory=FlattenConfig)
    relations: dict[str, RelationConfig] = Field(default_factory=dict)
    bigquery: BigQuerySilverConfig | None = None
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

    @field_validator("exclude", "not_null", "unique", "grain", mode="before")
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
        for field in self.grain:
            _require_dbt_col_id(field, where="dbt.stg.relations.grain")
        if self.load == "parent_replace" and self.materialized == "table":
            raise ValueError(
                "dbt.stg.relations: load=parent_replace conflicts with "
                "materialized=table (omit materialized or use load only; "
                "parent_replace maps silver to incremental)"
            )
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
        silver_mat = self.materialized
        if self.load == "full_refresh":
            silver_mat = "table"
        elif self.load == "parent_replace":
            silver_mat = "incremental"
        _validate_bigquery_vs_materialized(
            self.bigquery,
            materialized=silver_mat,
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

    model_config = ConfigDict(extra="forbid")

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

    Closed scaffold API — unknown keys fail at load (``extra=forbid``). Prefer
    hand-edited stg SQL for one-off semantics; expand the set only via SemVer.
    """

    model_config = ConfigDict(extra="forbid")

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
