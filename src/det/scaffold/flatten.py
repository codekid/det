"""Schema → flatten plan for dbt.stg nested struct expansion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from det.runtime.config import FlattenConfig, _require_dbt_col_id


@dataclass(frozen=True)
class FlattenLeaf:
    """One scalar leaf promoted onto the parent/relation row."""

    parts: tuple[str, ...]
    column_name: str
    extract_path: str
    prop: dict[str, Any]
    root: str


def path_parts_to_column(parts: tuple[str, ...]) -> str:
    return "__".join(parts)


def _allowed_types(prop: dict[str, Any]) -> set[str]:
    raw = prop.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {t for t in raw if isinstance(t, str)}
    return set()


def is_object_prop(prop: dict[str, Any]) -> bool:
    allowed = _allowed_types(prop)
    if "object" in allowed:
        return True
    if "properties" in prop and "array" not in allowed:
        return True
    return False


def is_array_prop(prop: dict[str, Any]) -> bool:
    allowed = _allowed_types(prop)
    if "array" in allowed:
        return True
    return "items" in prop and "object" not in allowed


def is_scalar_prop(prop: dict[str, Any]) -> bool:
    if is_array_prop(prop) or is_object_prop(prop):
        return False
    allowed = _allowed_types(prop)
    if not allowed:
        return "properties" not in prop and "items" not in prop
    return bool(allowed & {"string", "integer", "number", "boolean", "null"})


def _object_properties(prop: dict[str, Any]) -> dict[str, Any]:
    props = prop.get("properties") or {}
    return props if isinstance(props, dict) else {}


def _array_item_prop(prop: dict[str, Any]) -> dict[str, Any] | None:
    items = prop.get("items")
    if isinstance(items, dict):
        return items
    return None


def plan_flatten(
    schema_or_prop: dict[str, Any],
    flatten: FlattenConfig,
    *,
    relation_paths: set[str] | None = None,
    include_root_scalars: bool = False,
    relative_extract: bool = False,
) -> list[FlattenLeaf]:
    """
    Walk object properties and emit scalar leaves under ``flatten.depth``.

    Parent mode (default): only nested scalars under object roots; ``root`` is the
    top-level bronze column and ``extract_path`` is relative to that column.

    Relation mode (``include_root_scalars=True``, ``relative_extract=True``):
    item scalars and nested leaves; ``extract_path`` is relative to the unnested
    item column (``_rel``).
    """
    relation_paths = relation_paths or set()
    props = schema_or_prop.get("properties") or {}
    if not isinstance(props, dict):
        props = {}

    include = set(flatten.include)
    exclude = set(flatten.exclude)
    max_depth = flatten.depth

    leaves: list[FlattenLeaf] = []

    def extract_path_for(parts: tuple[str, ...]) -> str:
        if relative_extract:
            return "$." + ".".join(parts)
        if len(parts) == 1:
            return "$"
        return "$." + ".".join(parts[1:])

    def walk(
        cur_props: dict[str, Any],
        parts: tuple[str, ...],
        object_depth: int,
    ) -> None:
        for name, prop in cur_props.items():
            if not isinstance(name, str) or not isinstance(prop, dict):
                continue
            if not parts:
                if include and name not in include:
                    continue
                if name in exclude:
                    continue
                if name in relation_paths:
                    continue

            next_parts = parts + (name,)
            if is_array_prop(prop):
                continue
            if is_object_prop(prop):
                next_depth = object_depth + 1
                if max_depth is not None and next_depth > max_depth:
                    continue
                nested = _object_properties(prop)
                if nested:
                    walk(nested, next_parts, next_depth)
                continue

            if not parts and not include_root_scalars:
                continue

            col = path_parts_to_column(next_parts)
            _require_dbt_col_id(col, where="flatten column")
            leaves.append(
                FlattenLeaf(
                    parts=next_parts,
                    column_name=col,
                    extract_path=extract_path_for(next_parts),
                    prop=prop,
                    root=next_parts[0] if not relative_extract else "_rel",
                )
            )

    walk(props, (), object_depth=0)
    return leaves


def detect_flatten_collisions(
    leaves: list[FlattenLeaf],
    *,
    reserved: set[str],
) -> None:
    """Raise ValueError if flattened names collide with each other or reserved ids."""
    seen: dict[str, tuple[str, ...]] = {}
    for leaf in leaves:
        name = leaf.column_name
        if name in reserved:
            raise ValueError(
                f"flatten column {name!r} collides with existing column "
                f"(from path {'.'.join(leaf.parts)})"
            )
        prior = seen.get(name)
        if prior is not None:
            raise ValueError(
                f"flatten column {name!r} collides: "
                f"{'.'.join(prior)} vs {'.'.join(leaf.parts)}"
            )
        seen[name] = leaf.parts


def _item_schema_from_array_prop(arr: dict[str, Any], *, path: str) -> dict[str, Any]:
    if not is_array_prop(arr):
        raise ValueError(
            f"dbt.stg.relations path {path!r} must be an array property in the schema"
        )
    item = _array_item_prop(arr)
    if item is None:
        return {"type": "object", "properties": {}}
    if is_object_prop(item):
        return item
    return {"type": "object", "properties": {"value": item}}


def relation_item_schema(schema: dict[str, Any], path: str) -> dict[str, Any]:
    """Return a schema-like dict for the array item at top-level ``path``."""
    return relation_item_schema_at(schema, [path])


def relation_item_schema_at(
    schema: dict[str, Any], path_chain: list[str]
) -> dict[str, Any]:
    """
    Walk ``path_chain`` of array properties (root schema → nested item arrays).

    Example: ``["line_items", "tax_lines"]`` → item schema of tax_lines.
    """
    if not path_chain:
        raise ValueError("relation path_chain must be non-empty")
    cur_schema = schema
    for i, path in enumerate(path_chain):
        props = cur_schema.get("properties") or {}
        if not isinstance(props, dict):
            props = {}
        arr = props.get(path)
        if not isinstance(arr, dict):
            joined = ".".join(path_chain[: i + 1])
            raise ValueError(
                f"dbt.stg.relations path {joined!r} missing array property {path!r}"
            )
        cur_schema = _item_schema_from_array_prop(arr, path=".".join(path_chain[: i + 1]))
    return cur_schema


def flattened_roots(leaves: list[FlattenLeaf]) -> set[str]:
    return {leaf.root for leaf in leaves if leaf.root != "_rel"}


def iter_relation_paths(
    relations: dict[str, Any],
    *,
    prefix: tuple[str, ...] = (),
    path_chain: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], tuple[str, ...], Any]]:
    """
    Flatten nested RelationConfig tree.

    Returns list of ``(name_parts, path_chain, relation)`` where ``name_parts``
    become the model suffix (``line_items``, ``line_items__tax_lines``, …).
    """
    out: list[tuple[tuple[str, ...], tuple[str, ...], Any]] = []
    for name, rel in relations.items():
        path = getattr(rel, "path", None) or name
        parts = prefix + (name,)
        chain = path_chain + (path,)
        out.append((parts, chain, rel))
        nested = getattr(rel, "relations", None) or {}
        if nested:
            out.extend(
                iter_relation_paths(nested, prefix=parts, path_chain=chain)
            )
    return out
