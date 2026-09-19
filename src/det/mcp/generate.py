"""Dry-run generative helpers for DET MCP (schema / mapper drafts)."""

from __future__ import annotations

from dataclasses import dataclass
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
_SCALAR_TYPES = frozenset({"null", "boolean", "integer", "number", "string"})
_DRAFT_2020_12 = "https://json-schema.org/draft/2020-12/schema"


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root))
    except ValueError:
        return str(path.resolve())


def _strip_meta_keys(value: Any) -> Any:
    """Drop DET runtime ``__*`` keys from objects (recursive)."""
    if isinstance(value, dict):
        return {
            k: _strip_meta_keys(v)
            for k, v in value.items()
            if not (isinstance(k, str) and k.startswith("__"))
        }
    if isinstance(value, list):
        return [_strip_meta_keys(item) for item in value]
    return value


def _type_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [str(t) for t in raw if isinstance(t, str)]
    return []


def _ordered_types(types: set[str]) -> list[str]:
    return [t for t in _JSON_TYPE_ORDER if t in types]


def _emit_type(types: set[str]) -> Any:
    ordered = _ordered_types(types)
    if not ordered:
        return "string"
    if len(ordered) == 1:
        return ordered[0]
    return ordered


def _resolve_scalar_conflict(
    types: set[str], *, path: str, warnings: list[str]
) -> set[str]:
    """Apply DET widen rules for scaffold-safe ``type`` unions."""
    out = set(types)
    if "integer" in out and "number" in out:
        out.discard("integer")
    non_null = out - {"null"}
    if "string" in non_null and (non_null & {"integer", "number", "boolean"}):
        kept = {"string"} | ({"null"} & out)
        dropped = sorted(non_null - {"string"})
        warnings.append(
            f"{path}: mixed {', '.join(dropped)}+string in sample; widened to string"
        )
        return kept
    hard = non_null - _SCALAR_TYPES
    # object/array mixed with scalars → string (opaque) + warn
    if hard and (non_null & _SCALAR_TYPES):
        warnings.append(
            f"{path}: mixed structural+scalar types {sorted(non_null)}; "
            "widened to string"
        )
        return {"string"} | ({"null"} & out)
    if len(non_null) > 1 and not hard:
        # e.g. boolean+integer — widen to string
        if non_null <= _SCALAR_TYPES and "string" not in non_null:
            if non_null <= {"integer", "number"}:
                return out  # already collapsed above
            warnings.append(
                f"{path}: mixed scalar types {sorted(non_null)}; widened to string"
            )
            return {"string"} | ({"null"} & out)
    return out


def _fold_type_options(
    options: list[Any], *, path: str, warnings: list[str]
) -> dict[str, Any] | None:
    """Fold simple anyOf/oneOf branches into a single type node, or None.

    Object/array branches are inspected (not immediately rejected). A structural
    type mixed with a scalar widens via ``_resolve_scalar_conflict`` to
    ``string``. Nested unions or structure-only conflicts stay unfolded.
    """
    type_sets: list[set[str]] = []
    saw_structure = False
    for opt in options:
        if not isinstance(opt, dict):
            return None
        # Nested composition inside a branch → genuinely complex.
        if any(k in opt for k in ("anyOf", "oneOf", "allOf")):
            return None
        ts = set(_type_list(opt.get("type")))
        if "properties" in opt:
            saw_structure = True
            ts.add("object")
        if "items" in opt:
            saw_structure = True
            ts.add("array")
        if not ts:
            return None
        type_sets.append(ts)

    merged: set[str] = set()
    for ts in type_sets:
        merged |= ts
    non_null = merged - {"null"}
    hard = non_null - _SCALAR_TYPES
    scalars = non_null & (_SCALAR_TYPES - {"null"})

    if saw_structure and hard and scalars:
        # e.g. object|string or array|integer → opaque string for bronze.
        resolved = _resolve_scalar_conflict(merged, path=path, warnings=warnings)
        return {"type": _emit_type(resolved)}

    if saw_structure:
        # Structure-only (or ambiguous object shapes) — leave for human review.
        # Caller keeps the union and recursively normalizes each branch.
        return None

    merged = _resolve_scalar_conflict(merged, path=path, warnings=warnings)
    return {"type": _emit_type(merged)}


def _normalize_union_branches(
    branches: list[Any], *, path: str, warnings: list[str]
) -> list[Any]:
    """Normalize each retained anyOf/oneOf branch (close objects, nest)."""
    out: list[Any] = []
    for i, opt in enumerate(branches):
        child_path = f"{path or '$'}[{i}]"
        out.append(_normalize_schema_node(opt, path=child_path, warnings=warnings))
    return out


def _normalize_schema_node(
    node: Any, *, path: str, warnings: list[str]
) -> Any:
    """Recursively close objects and fold simple unions for DET bronze drafts."""
    if not isinstance(node, dict):
        return node
    out = dict(node)

    if "anyOf" in out and isinstance(out["anyOf"], list):
        folded = _fold_type_options(out["anyOf"], path=path or "$", warnings=warnings)
        if folded is not None:
            out.pop("anyOf", None)
            out.pop("oneOf", None)
            out.update(folded)
            # Fold may widen to scalar; drop leftover structural keywords now.
            folded_types = set(_type_list(folded.get("type")))
            if folded_types and "object" not in folded_types and "array" not in folded_types:
                for key in ("properties", "required", "items", "additionalProperties"):
                    out.pop(key, None)
        else:
            warnings.append(
                f"{path or '$'}: left anyOf intact (not a simple scalar union); review"
            )
            out["anyOf"] = _normalize_union_branches(
                out["anyOf"], path=path or "$", warnings=warnings
            )
    elif "oneOf" in out and isinstance(out["oneOf"], list):
        folded = _fold_type_options(out["oneOf"], path=path or "$", warnings=warnings)
        if folded is not None:
            out.pop("oneOf", None)
            out.pop("anyOf", None)
            out.update(folded)
            folded_types = set(_type_list(folded.get("type")))
            if folded_types and "object" not in folded_types and "array" not in folded_types:
                for key in ("properties", "required", "items", "additionalProperties"):
                    out.pop(key, None)
        else:
            warnings.append(
                f"{path or '$'}: left oneOf intact (not a simple scalar union); review"
            )
            out["oneOf"] = _normalize_union_branches(
                out["oneOf"], path=path or "$", warnings=warnings
            )

    if "type" in out:
        types = set(_type_list(out["type"]))
        if types:
            resolved = _resolve_scalar_conflict(
                types, path=path or "$", warnings=warnings
            )
            out["type"] = _emit_type(resolved)

    types_now = set(_type_list(out.get("type")))
    # After widen-to-scalar, drop structural keywords so we do not re-close as object.
    if types_now and "object" not in types_now and "array" not in types_now:
        for key in ("properties", "required", "items", "additionalProperties"):
            out.pop(key, None)

    if "object" in types_now or "properties" in out:
        out["additionalProperties"] = False
        props = out.get("properties")
        if isinstance(props, dict):
            new_props: dict[str, Any] = {}
            for key, prop in props.items():
                child_path = f"{path}.{key}" if path else str(key)
                new_props[key] = _normalize_schema_node(
                    prop, path=child_path, warnings=warnings
                )
            out["properties"] = new_props
        if "required" in out and not isinstance(out["required"], list):
            out["required"] = []

    if "items" in out:
        items_path = f"{path}.items" if path else "items"
        out["items"] = _normalize_schema_node(
            out["items"], path=items_path, warnings=warnings
        )

    return out


def _normalize_inferred_schema(
    raw: dict[str, Any],
    *,
    title: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    body = _normalize_schema_node(dict(raw), path="", warnings=warnings)
    if not isinstance(body, dict):
        body = {"type": "object", "properties": {}, "additionalProperties": False}
    props = body.get("properties") if isinstance(body.get("properties"), dict) else {}
    required = (
        list(body["required"]) if isinstance(body.get("required"), list) else []
    )
    schema: dict[str, Any] = {
        "$schema": _DRAFT_2020_12,
        "type": "object",
        "properties": props,
        "additionalProperties": False,
        "required": required,
        "description": (
            "Inferred from sample rows via genson (dry-run). "
            "Review before production use."
        ),
    }
    if title:
        schema["$title"] = title
    return schema, warnings


@dataclass(frozen=True)
class InferredSchemaResult:
    """DET-normalized inferred schema plus mechanical conflict warnings."""

    schema: dict[str, Any]
    warnings: list[str]


def infer_schema_from_records(
    records: list[dict[str, Any]],
    *,
    title: str | None = None,
) -> InferredSchemaResult:
    """
    Infer a DET-style Draft 2020-12 object schema from sample rows.

    Uses genson for deep nested objects and array-of-object item schemas.
    Runtime ``__*`` meta keys are stripped before inference. Mixed scalar
    conflicts (e.g. integer+string) widen mechanically to ``string`` with a
    warning — never invents ``format: date-time``.
    """
    try:
        from genson import SchemaBuilder
    except ImportError as exc:
        raise ImportError(
            "schema inference requires the mcp extra (genson). "
            'Install with: uv pip install "det-elt[mcp]"'
        ) from exc

    rows = [_strip_meta_keys(r) for r in records if isinstance(r, dict)]
    rows = [r for r in rows if isinstance(r, dict)]
    if not rows:
        schema: dict[str, Any] = {
            "$schema": _DRAFT_2020_12,
            "type": "object",
            "properties": {},
            "additionalProperties": False,
            "required": [],
            "description": (
                "Inferred from sample rows via genson (dry-run). "
                "Review before production use."
            ),
        }
        if title:
            schema["$title"] = title
        return InferredSchemaResult(schema=schema, warnings=[])

    builder = SchemaBuilder()
    for row in rows:
        builder.add_object(row)
    raw = builder.to_schema()
    if not isinstance(raw, dict):
        raise TypeError(f"genson returned non-object schema: {type(raw)}")
    schema, warnings = _normalize_inferred_schema(raw, title=title)
    return InferredSchemaResult(schema=schema, warnings=warnings)


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
    limit: int = 1000,
    schema_out: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """
    Infer a bronze JSON Schema from named sample rows or inline records.

    Dry-run only — never writes ``schema_out``. Deep nested / array-of-object
    structure comes from genson; review ``warnings`` before writing.
    """
    from det.mcp.inspect import MAX_SAMPLE_LIMIT

    base = _root(root)
    capped = clamp_sample_limit(limit if limit is not None else MAX_SAMPLE_LIMIT)
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

    inferred = infer_schema_from_records(sampled, title=title)
    schema = inferred.schema
    warnings = list(inferred.warnings)
    yaml_text = schema_to_yaml(schema)
    out_path = Path(would_write)
    note = (
        "Dry-run only — no file written. Review YAML, then write manually or via "
        f"a confirmed edit to {would_write}."
        + (" Path already exists." if (base / out_path).is_file() else "")
    )
    if warnings:
        note += (
            " Review warnings (mechanical type widenings) before accepting the draft."
        )
    return {
        "dry_run": True,
        "pipeline": pipeline,
        "schema": schema,
        "yaml": yaml_text,
        "would_write": would_write,
        "rows_sampled": len(sampled),
        "limit": capped,
        "warnings": warnings,
        "note": note,
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
        raise ValueError(
            f"mapper_name must be a valid Python identifier, got {mapper_name!r}"
        )

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
