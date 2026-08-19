"""Dry-run generative helpers for DET MCP (schema / mapper drafts)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from det.mcp.context import project_root, resolve_under_root
from det.mcp.inspect import clamp_sample_limit, sample_raw
from det.runtime.config import load_pipeline_config
from det.runtime.ids import default_schema_path
from det.runtime.naming import to_snake_case
from det.runtime.pipelines import resolve_pipeline_ref
from det.validation.jsonschema_validator import load_json_schema

_JSON_TYPE_ORDER = ("null", "boolean", "integer", "number", "string", "object", "array")


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path.resolve())


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def _merge_types(existing: set[str], new: str) -> set[str]:
    out = set(existing)
    out.add(new)
    # integer ⊂ number for JSON Schema practicality when both seen
    if "integer" in out and "number" in out:
        out.discard("integer")
    return out


def _type_schema(types: set[str]) -> Any:
    ordered = [t for t in _JSON_TYPE_ORDER if t in types]
    if not ordered:
        return "string"
    if len(ordered) == 1:
        return ordered[0]
    return ordered


def _infer_object_properties(
    rows: list[dict[str, Any]],
    *,
    depth: int = 0,
    max_depth: int = 1,
) -> tuple[dict[str, Any], list[str]]:
    """Infer properties + required from homogeneous object rows."""
    key_types: dict[str, set[str]] = {}
    key_present: dict[str, int] = {}
    nested_rows: dict[str, list[dict[str, Any]]] = {}
    array_item_types: dict[str, set[str]] = {}
    n = len(rows)

    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if key.startswith("__"):
                continue
            key_present[key] = key_present.get(key, 0) + 1
            jt = _json_type(value)
            key_types[key] = _merge_types(key_types.get(key, set()), jt)
            if jt == "object" and isinstance(value, dict) and depth < max_depth:
                nested_rows.setdefault(key, []).append(value)
            elif jt == "array" and isinstance(value, list):
                for item in value:
                    array_item_types.setdefault(key, set())
                    array_item_types[key] = _merge_types(
                        array_item_types[key], _json_type(item)
                    )

    properties: dict[str, Any] = {}
    for key, types in sorted(key_types.items()):
        prop: dict[str, Any] = {"type": _type_schema(types)}
        if "object" in types and key in nested_rows and depth < max_depth:
            nested_props, nested_req = _infer_object_properties(
                nested_rows[key], depth=depth + 1, max_depth=max_depth
            )
            prop["properties"] = nested_props
            if nested_req:
                prop["required"] = nested_req
            prop["additionalProperties"] = False
        if "array" in types and key in array_item_types:
            prop["items"] = {"type": _type_schema(array_item_types[key])}
        properties[key] = prop

    required = sorted(k for k, count in key_present.items() if count == n and n > 0)
    return properties, required


def infer_schema_from_records(
    records: list[dict[str, Any]],
    *,
    title: str | None = None,
) -> dict[str, Any]:
    """
    Infer a DET-style Draft 2020-12 object schema from sample rows.

    Nested objects recurse one level. Runtime ``__*`` meta keys are ignored.
    """
    rows = [r for r in records if isinstance(r, dict)]
    properties, required = _infer_object_properties(rows)
    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if title:
        schema["$title"] = title
    if required:
        schema["required"] = required
    else:
        schema["required"] = []
    schema["description"] = (
        "Inferred from sample rows (dry-run). Review before production use."
    )
    return schema


def schema_to_yaml(schema: dict[str, Any]) -> str:
    return yaml.safe_dump(schema, sort_keys=False, allow_unicode=True)


def schema_from_sample_dry_run(
    pipeline: str | None = None,
    *,
    run_path: str | None = None,
    interval_start: str | None = None,
    interval_end: str | None = None,
    extract_run_datetime: str | None = None,
    records: list[dict[str, Any]] | None = None,
    limit: int = 50,
    schema_out: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Infer a bronze JSON Schema from named sample rows or inline records.

    Dry-run only — never writes ``schema_out``.
    """
    base = _root(root)
    capped = clamp_sample_limit(limit)
    title: str | None = None
    would_write: str

    if records is not None:
        sampled = [dict(r) for r in records[:capped] if isinstance(r, dict)]
        if pipeline:
            resolved = resolve_pipeline_ref(pipeline, project_root=base)
            config = load_pipeline_config(resolved.path)
            title = config.canonical_id.replace(".", "_")
            would_write = schema_out or default_schema_path(config.canonical_id)
        else:
            would_write = schema_out or "schemas/inferred.schema.yaml"
    else:
        if not pipeline:
            raise ValueError("pipeline is required when records are not provided")
        sampled_raw = sample_raw(
            pipeline,
            stage="named",
            limit=capped,
            run_path=run_path,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
            root=base,
        )
        sampled = [
            row["data"]
            for row in sampled_raw.get("rows") or []
            if isinstance(row, dict) and isinstance(row.get("data"), dict)
        ]
        resolved = resolve_pipeline_ref(pipeline, project_root=base)
        config = load_pipeline_config(resolved.path)
        title = config.canonical_id.replace(".", "_")
        would_write = schema_out or default_schema_path(config.canonical_id)

    if not sampled:
        raise ValueError("no sample rows available to infer schema")

    # Preview path must stay under project root when provided.
    if schema_out is not None:
        resolve_under_root(would_write, root=base)

    schema = infer_schema_from_records(sampled, title=title)
    yaml_text = schema_to_yaml(schema)
    out_path = Path(would_write)
    return {
        "dry_run": True,
        "pipeline": pipeline,
        "schema": schema,
        "yaml": yaml_text,
        "would_write": would_write,
        "rows_sampled": len(sampled),
        "limit": capped,
        "note": (
            "Dry-run only — no file written. Review YAML, then write manually or via "
            f"a confirmed edit to {would_write}."
            + (
                " Path already exists."
                if (base / out_path).is_file()
                else ""
            )
        ),
    }


def _prop_type_set(prop: dict[str, Any]) -> frozenset[str]:
    raw = prop.get("type")
    if raw is None:
        return frozenset()
    if isinstance(raw, str):
        return frozenset({raw})
    if isinstance(raw, list):
        return frozenset(str(t) for t in raw)
    return frozenset()


def _normalize_key(name: str) -> str:
    return to_snake_case(name).lower()


def diff_schema_properties(
    from_schema: dict[str, Any],
    to_schema: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Diff top-level properties into add / remove / rename ops.

    Rename heuristic: snake/camel case-insensitive match first; then unambiguous
    1:1 unpaired keys that share the same JSON type set (ignoring null).
    """
    from_props = from_schema.get("properties") or {}
    to_props = to_schema.get("properties") or {}
    if not isinstance(from_props, dict) or not isinstance(to_props, dict):
        raise ValueError("both schemas must have object properties maps")

    from_keys = set(from_props)
    to_keys = set(to_props)
    shared = from_keys & to_keys
    removed = sorted(from_keys - to_keys)
    added = sorted(to_keys - from_keys)

    ops: list[dict[str, Any]] = []
    for key in sorted(shared):
        ft = _prop_type_set(from_props[key] if isinstance(from_props[key], dict) else {})
        tt = _prop_type_set(to_props[key] if isinstance(to_props[key], dict) else {})
        if ft != tt:
            ops.append(
                {
                    "op": "type_change",
                    "field": key,
                    "from_type": sorted(ft),
                    "to_type": sorted(tt),
                }
            )

    renames: list[tuple[str, str]] = []
    rem_left = list(removed)
    add_left = list(added)

    # Pass 1: normalized name match
    rem_by_norm = {_normalize_key(k): k for k in rem_left}
    add_by_norm = {_normalize_key(k): k for k in add_left}
    for norm in sorted(set(rem_by_norm) & set(add_by_norm)):
        old_k, new_k = rem_by_norm[norm], add_by_norm[norm]
        if old_k == new_k:
            continue
        renames.append((old_k, new_k))
        rem_left.remove(old_k)
        add_left.remove(new_k)

    # Pass 2: unambiguous same-type 1:1 among leftovers
    if len(rem_left) == 1 and len(add_left) == 1:
        old_k, new_k = rem_left[0], add_left[0]
        ft = _prop_type_set(from_props[old_k]) - {"null"}
        tt = _prop_type_set(to_props[new_k]) - {"null"}
        if ft and ft == tt:
            renames.append((old_k, new_k))
            rem_left.clear()
            add_left.clear()

    for old_k, new_k in renames:
        ops.append(
            {
                "op": "rename",
                "from": old_k,
                "to": new_k,
                "from_type": sorted(_prop_type_set(from_props[old_k])),
                "to_type": sorted(_prop_type_set(to_props[new_k])),
            }
        )
    for key in rem_left:
        ops.append(
            {
                "op": "remove",
                "field": key,
                "type": sorted(_prop_type_set(from_props[key])),
            }
        )
    for key in add_left:
        ops.append(
            {
                "op": "add",
                "field": key,
                "type": sorted(_prop_type_set(to_props[key])),
            }
        )
    return ops


def _mapper_code(mapper_name: str, ops: list[dict[str, Any]]) -> str:
    renames = [o for o in ops if o["op"] == "rename"]
    removes = [o for o in ops if o["op"] == "remove"]
    lines = [
        "from __future__ import annotations",
        "",
        "from typing import Any",
        "",
        "from det.sources.base import mapper",
        "",
        "",
        f"@mapper({mapper_name!r})",
        f"def {mapper_name}(row: dict[str, Any]) -> dict[str, Any]:",
        '    """Generated mapper stub — review before using with det migrate."""',
        "    out = dict(row)",
    ]
    for op in renames:
        src, dst = op["from"], op["to"]
        lines.append(f"    if {src!r} in out and {dst!r} not in out:")
        lines.append(f"        out[{dst!r}] = out.pop({src!r})")
    for op in removes:
        field = op["field"]
        lines.append(f"    out.pop({field!r}, None)")
    lines.append("    return out")
    lines.append("")
    return "\n".join(lines)


def mapper_from_diff_dry_run(
    from_schema: str,
    to_schema: str,
    mapper_name: str,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Diff two schema YAML/JSON files and draft a mapper stub.

    Rename detection: normalized snake_case match, then unambiguous same-type 1:1
    among remaining unpaired keys. Dry-run only — never writes source files.
    """
    base = _root(root)
    from_path = resolve_under_root(from_schema, root=base)
    to_path = resolve_under_root(to_schema, root=base)
    if not from_path.is_file():
        raise FileNotFoundError(f"from_schema not found: {_rel(from_path, base)}")
    if not to_path.is_file():
        raise FileNotFoundError(f"to_schema not found: {_rel(to_path, base)}")

    name = (mapper_name or "").strip()
    if not name.isidentifier():
        raise ValueError(f"mapper_name must be a valid Python identifier, got {mapper_name!r}")

    from_doc = load_json_schema(from_path)
    to_doc = load_json_schema(to_path)
    ops = diff_schema_properties(from_doc, to_doc)
    code = _mapper_code(name, ops)
    register_hint = (
        f'@mapper("{name}")  # on the function in src/det/sources/<provider>/<source>.py'
    )
    return {
        "dry_run": True,
        "mapper_name": name,
        "from_schema": _rel(from_path, base),
        "to_schema": _rel(to_path, base),
        "ops": ops,
        "code": code,
        "register_hint": register_hint,
        "note": (
            "Dry-run only — no file written. Review ops/code, add the function next to "
            "the source plugin with @mapper, then det migrate with --mapper "
            f"{name}."
        ),
    }
