"""Read-only dbt model catalog for DET MCP (silver / gold / ops)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import yaml

from det.mcp.context import project_root

Layer = Literal["stg", "silver", "gold", "ops"]
Warehouse = Literal["analytics", "ops"]

_CONFIG_BLOCK = re.compile(
    r"\{\{\s*config\s*\((.*?)\)\s*\}\}",
    re.DOTALL | re.IGNORECASE,
)
_SCHEMA_ASSIGN = re.compile(r"\bschema\s*=\s*['\"]([^'\"]+)['\"]")
_MATERIALIZED_ASSIGN = re.compile(r"\bmaterialized\s*=\s*['\"]([^'\"]+)['\"]")


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def dbt_models_dir(root: Path) -> Path:
    return root / "dbt" / "models"


def _folder_defaults(rel_posix: str) -> tuple[Warehouse, str | None]:
    if "/ops/" in f"/{rel_posix}" or rel_posix.startswith("ops/"):
        return "ops", "ops"
    if "/gold/" in f"/{rel_posix}" or rel_posix.startswith("gold/"):
        return "analytics", "gold"
    return "analytics", None


def _layer_for(name: str, warehouse: Warehouse, rel_posix: str) -> Layer:
    if name.startswith("stg_"):
        return "stg"
    if warehouse == "ops":
        return "ops"
    if "/gold/" in f"/{rel_posix}" or rel_posix.startswith("gold/"):
        return "gold"
    if name.startswith("silver_"):
        return "silver"
    return "silver"


def _parse_sql_config(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    match = _CONFIG_BLOCK.search(text)
    if match is None:
        return out
    body = match.group(1)
    schema = _SCHEMA_ASSIGN.search(body)
    if schema:
        out["schema"] = schema.group(1)
    materialized = _MATERIALIZED_ASSIGN.search(body)
    if materialized:
        out["materialized"] = materialized.group(1)
    return out


def _load_yaml_models(models_dir: Path) -> dict[str, dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for path in sorted(models_dir.rglob("*.yml")):
        try:
            doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            continue
        if not isinstance(doc, dict):
            continue
        for entry in doc.get("models") or []:
            if not isinstance(entry, dict) or not entry.get("name"):
                continue
            by_name[str(entry["name"])] = entry
    return by_name


def _yaml_columns(entry: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not entry:
        return []
    cols: list[dict[str, Any]] = []
    for col in entry.get("columns") or []:
        if not isinstance(col, dict) or not col.get("name"):
            continue
        item: dict[str, Any] = {"name": str(col["name"])}
        if col.get("description"):
            item["description"] = str(col["description"]).strip()
        tests = col.get("tests")
        if tests:
            item["tests"] = tests
        cols.append(item)
    return cols


def _yaml_grain(entry: dict[str, Any] | None) -> list[str]:
    if not entry:
        return []
    config = entry.get("config") if isinstance(entry.get("config"), dict) else {}
    meta = config.get("meta") if isinstance(config.get("meta"), dict) else {}
    grain = meta.get("grain")
    if isinstance(grain, list):
        return [str(g) for g in grain]
    if isinstance(grain, str) and grain.strip():
        return [grain.strip()]
    return []


def _model_description(entry: dict[str, Any] | None) -> str | None:
    if not entry:
        return None
    raw = entry.get("description")
    if not isinstance(raw, str):
        return None
    text = " ".join(raw.split())
    return text or None


def list_dbt_models(*, root: Path | None = None) -> dict[str, Any]:
    """List dbt models under dbt/models (includes ops; skips analyses)."""
    base = _root(root)
    models_dir = dbt_models_dir(base)
    yaml_models = _load_yaml_models(models_dir) if models_dir.is_dir() else {}
    models: list[dict[str, Any]] = []
    if models_dir.is_dir():
        for sql_path in sorted(models_dir.rglob("*.sql")):
            rel = sql_path.relative_to(models_dir).as_posix()
            name = sql_path.stem
            warehouse, default_schema = _folder_defaults(rel)
            cfg = _parse_sql_config(sql_path.read_text(encoding="utf-8"))
            schema = cfg.get("schema") or default_schema
            yaml_entry = yaml_models.get(name)
            models.append(
                {
                    "name": name,
                    "schema": schema,
                    "layer": _layer_for(name, warehouse, rel),
                    "warehouse": warehouse,
                    "description": _model_description(yaml_entry),
                    "materialized": cfg.get("materialized"),
                    "path": f"dbt/models/{rel}",
                    "grain": _yaml_grain(yaml_entry),
                }
            )
    return {
        "project_root": str(base),
        "models": models,
        "note": (
            "Physical dbt models. Certified gold/ops metrics go through Cube "
            "(cube_meta / cube_load). query_analytics is a capped SQL escape hatch."
        ),
    }


def describe_dbt_model(name: str, *, root: Path | None = None) -> dict[str, Any]:
    """Describe one dbt model (YAML columns + SQL config)."""
    listed = list_dbt_models(root=root)
    match = next((m for m in listed["models"] if m["name"] == name), None)
    if match is None:
        return {
            "ok": False,
            "error": "model_not_found",
            "name": name,
            "models": [m["name"] for m in listed["models"]],
        }
    models_dir = dbt_models_dir(_root(root))
    yaml_entry = _load_yaml_models(models_dir).get(name)
    return {
        "ok": True,
        **match,
        "columns": _yaml_columns(yaml_entry),
    }
