"""Compile path-scoped dbt.stg AdaptScope trees into flat column adaptations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from det.runtime.config import AdaptScope, RelationConfig, _require_dbt_col_id


@dataclass
class FlatAdapt:
    """Column-id keyed adaptations (default flatten names before rename)."""

    coalesce: dict[str, list[str]] = field(default_factory=dict)
    null_sentinels: dict[str, list[Any]] = field(default_factory=dict)
    map: dict[str, dict[str, str]] = field(default_factory=dict)
    # default_col_id → final output name
    rename: dict[str, str] = field(default_factory=dict)
    exclude: set[str] = field(default_factory=set)
    # Final names (after rename) for silver tests
    not_null: list[str] = field(default_factory=list)
    unique: list[str] = field(default_factory=list)
    accepted_values: dict[str, list[str]] = field(default_factory=dict)


def split_relative_path(key: str, *, where: str) -> tuple[str, ...]:
    """Split a relative dotted path into snake_case segments."""
    if not isinstance(key, str) or not key.strip():
        raise ValueError(f"{where}: relative path must be a non-empty string")
    if key.startswith(".") or key.endswith(".") or ".." in key:
        raise ValueError(f"{where}: invalid relative path {key!r}")
    parts = tuple(p for p in key.split(".") if p)
    if not parts:
        raise ValueError(f"{where}: invalid relative path {key!r}")
    for part in parts:
        _require_dbt_col_id(part, where=where)
    return parts


def default_column_id(prefix: tuple[str, ...], relative: tuple[str, ...]) -> str:
    return "__".join(prefix + relative)


def _reject_restated_prefix(key: str, prefix: tuple[str, ...], *, where: str) -> None:
    """Forbid restating the scope prefix in a relative key."""
    if not prefix:
        return
    prefix_dot = ".".join(prefix)
    if key == prefix_dot or key.startswith(prefix_dot + "."):
        raise ValueError(
            f"{where}: relative path must not restate scope prefix "
            f"{prefix_dot!r}, got {key!r}"
        )


def scoped_rename_output(
    prefix: tuple[str, ...],
    relative: tuple[str, ...],
    dest: str,
) -> str:
    """
    Prefix-preserving scoped rename.

    ``prefix + relative`` is the default column; output is ``prefix + (dest,)``
    (relative segments replaced as a unit by a single snake_case leaf).
    When ``prefix`` is empty (item-root / root scalar scope), ``dest`` is the
    full output name.
    """
    _require_dbt_col_id(dest, where="scoped rename value")
    if prefix:
        return "__".join(prefix + (dest,))
    return dest


def merge_flat_adapt(*parts: FlatAdapt) -> FlatAdapt:
    out = FlatAdapt()
    for part in parts:
        out.coalesce.update(part.coalesce)
        out.null_sentinels.update(part.null_sentinels)
        out.map.update(part.map)
        out.rename.update(part.rename)
        out.exclude |= part.exclude
        for name in part.not_null:
            if name not in out.not_null:
                out.not_null.append(name)
        for name in part.unique:
            if name not in out.unique:
                out.unique.append(name)
        out.accepted_values.update(part.accepted_values)
    return out


def compile_adapt_scope(
    scope: AdaptScope,
    prefix: tuple[str, ...] = (),
    *,
    where: str = "dbt.stg.fields",
) -> FlatAdapt:
    """Compile an AdaptScope tree into FlatAdapt using ``prefix`` path parts."""
    out = FlatAdapt()

    for key, sources in scope.coalesce.items():
        _reject_restated_prefix(key, prefix, where=f"{where}.coalesce")
        rel = split_relative_path(key, where=f"{where}.coalesce key")
        if not sources:
            raise ValueError(f"{where}.coalesce[{key!r}] must be a non-empty list")
        src_cols: list[str] = []
        for src in sources:
            _reject_restated_prefix(src, prefix, where=f"{where}.coalesce[{key!r}]")
            src_cols.append(
                default_column_id(
                    prefix, split_relative_path(src, where=f"{where}.coalesce source")
                )
            )
        out.coalesce[default_column_id(prefix, rel)] = src_cols

    for key, sentinels in scope.null_sentinels.items():
        _reject_restated_prefix(key, prefix, where=f"{where}.null_sentinels")
        rel = split_relative_path(key, where=f"{where}.null_sentinels key")
        if not sentinels:
            raise ValueError(
                f"{where}.null_sentinels[{key!r}] must be a non-empty list"
            )
        out.null_sentinels[default_column_id(prefix, rel)] = list(sentinels)

    for key, mapping in scope.map.items():
        _reject_restated_prefix(key, prefix, where=f"{where}.map")
        rel = split_relative_path(key, where=f"{where}.map key")
        if not mapping:
            raise ValueError(f"{where}.map[{key!r}] must be a non-empty mapping")
        out.map[default_column_id(prefix, rel)] = dict(mapping)

    for key, dest in scope.rename.items():
        _reject_restated_prefix(key, prefix, where=f"{where}.rename")
        rel = split_relative_path(key, where=f"{where}.rename key")
        default = default_column_id(prefix, rel)
        out.rename[default] = scoped_rename_output(prefix, rel, dest)

    for key in scope.exclude:
        _reject_restated_prefix(key, prefix, where=f"{where}.exclude")
        rel = split_relative_path(key, where=f"{where}.exclude")
        col = default_column_id(prefix, rel)
        if col.startswith("__"):
            raise ValueError(f"{where}.exclude cannot drop meta column {col!r}")
        out.exclude.add(col)

    def _final(default: str) -> str:
        return out.rename.get(default, default)

    for key in scope.not_null:
        _reject_restated_prefix(key, prefix, where=f"{where}.not_null")
        rel = split_relative_path(key, where=f"{where}.not_null")
        out.not_null.append(_final(default_column_id(prefix, rel)))

    for key in scope.unique:
        _reject_restated_prefix(key, prefix, where=f"{where}.unique")
        rel = split_relative_path(key, where=f"{where}.unique")
        out.unique.append(_final(default_column_id(prefix, rel)))

    for key, values in scope.accepted_values.items():
        _reject_restated_prefix(key, prefix, where=f"{where}.accepted_values")
        rel = split_relative_path(key, where=f"{where}.accepted_values key")
        if not values:
            raise ValueError(
                f"{where}.accepted_values[{key!r}] must be a non-empty list"
            )
        out.accepted_values[_final(default_column_id(prefix, rel))] = list(values)

    for child_name, child in scope.children.items():
        _require_dbt_col_id(child_name, where=f"{where} child scope")
        child_flat = compile_adapt_scope(
            child,
            prefix + (child_name,),
            where=f"{where}.{child_name}",
        )
        out = merge_flat_adapt(out, child_flat)

    return out


def compile_stg_fields(fields: dict[str, AdaptScope]) -> FlatAdapt:
    """Compile ``dbt.stg.fields`` top-level property scopes."""
    parts = [
        compile_adapt_scope(scope, (name,), where=f"dbt.stg.fields.{name}")
        for name, scope in fields.items()
    ]
    return merge_flat_adapt(*parts) if parts else FlatAdapt()


def compile_relation_adapt(relation: RelationConfig) -> FlatAdapt:
    """Compile a relation's item-root knobs + nested property scopes."""
    return compile_adapt_scope(
        relation.to_adapt_scope(),
        (),
        where="dbt.stg.relations",
    )


def flat_adapt_from_root_stg(
    *,
    coalesce: dict[str, list[str]],
    null_sentinels: dict[str, list[Any]],
    map: dict[str, dict[str, str]],
    rename: dict[str, str],
    exclude: list[str],
) -> FlatAdapt:
    """Root-level stg knobs: keys are already column ids; rename is full replace."""
    return FlatAdapt(
        coalesce=dict(coalesce),
        null_sentinels=dict(null_sentinels),
        map={k: dict(v) for k, v in map.items()},
        rename=dict(rename),
        exclude=set(exclude),
    )
