"""Capped read-only DuckDB SQL over analytics (gold/silver) or ops."""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from det.mcp.context import project_root
from det.mcp.inspect import clamp_sample_limit
from det.optional_deps import require_duckdb
from det.runtime.lake import relpath as lake_relpath

Warehouse = Literal["analytics", "ops"]

_FORBIDDEN = re.compile(
    r"\b("
    r"insert\s+into|update\s+\w+|delete\s+from|merge\s+into|"
    r"truncate\s+table|drop\s+|create\s+|alter\s+|"
    r"attach\s+|detach\s+|copy\s+|pragma\s+|load\s+|"
    r"install\s+|export\s+|import\s+|vacuum\b|checkpoint\b|"
    r"grant\s+|revoke\s+|execute\s+"
    r")",
    re.IGNORECASE,
)
_FROM_JOIN = re.compile(
    r"\b(?:from|join)\s+(?:only\s+)?(?P<ident>\"[^\"]+\"|[A-Za-z_][\w]*)"
    r"(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w]*))?",
    re.IGNORECASE,
)
_QUALIFIED = re.compile(
    r"(?:\"[^\"]+\"|[A-Za-z_][\w]*)\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w]*)"
)


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def analytics_duckdb_path(root: Path) -> Path:
    raw = os.environ.get("DET_ANALYTICS_DUCKDB", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (root / "data" / "analytics.duckdb").resolve()


def ops_duckdb_path(root: Path) -> Path:
    raw = os.environ.get("DET_OPS_DUCKDB", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (root / "data" / "det_ops.duckdb").resolve()


def warehouse_path(warehouse: Warehouse, root: Path) -> Path:
    if warehouse == "ops":
        return ops_duckdb_path(root)
    return analytics_duckdb_path(root)


def _strip_ident(token: str) -> str:
    token = token.strip()
    if len(token) >= 2 and token[0] == '"' and token[-1] == '"':
        return token[1:-1].replace('""', '"')
    return token


def _schema_allowed(schema: str, warehouse: Warehouse) -> bool:
    if warehouse == "ops":
        return schema == "ops"
    return schema == "gold" or schema.startswith("silver_")


def _extract_schemas(sql: str) -> set[str]:
    schemas: set[str] = set()
    for match in _FROM_JOIN.finditer(sql):
        ident = match.group("ident")
        tail = sql[match.end() :]
        dotted = re.match(r"\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w]*)", tail)
        if dotted:
            schemas.add(_strip_ident(ident))
    for match in _QUALIFIED.finditer(sql):
        left = match.group(0).split(".", 1)[0]
        schemas.add(_strip_ident(left))
    return schemas


def _reject_sql(sql: str) -> str | None:
    stripped = sql.strip()
    if not stripped:
        return "empty SQL"
    if ";" in stripped.rstrip(";"):
        return "multiple statements are not allowed"
    body = stripped.rstrip(";").strip()
    head = re.split(r"\s+", body, maxsplit=1)[0].lower()
    if head not in {"select", "with"}:
        return "only SELECT / WITH statements are allowed"
    if _FORBIDDEN.search(body):
        return "statement contains a forbidden keyword"
    return None


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def query_analytics(
    sql: str,
    *,
    warehouse: Warehouse = "analytics",
    limit: int | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Run a capped read-only SELECT against analytics or ops DuckDB."""
    base = _root(root)
    capped = clamp_sample_limit(limit)
    db_path = warehouse_path(warehouse, base)
    note = (
        "Certified gold/ops metrics should use cube_load. "
        "This tool is a capped SELECT escape hatch for row detail."
    )
    out: dict[str, Any] = {
        "warehouse": warehouse,
        "limit": capped,
        "connection": lake_relpath(db_path, base),
        "note": note,
    }
    reason = _reject_sql(sql)
    if reason:
        return {**out, "ok": False, "error": "sql_rejected", "detail": reason, "rows": []}

    body = sql.strip().rstrip(";").strip()
    schemas = _extract_schemas(body)
    if not schemas:
        return {
            **out,
            "ok": False,
            "error": "sql_rejected",
            "detail": "qualify tables as schema.table (gold / silver_* / ops)",
            "rows": [],
        }
    bad = sorted(s for s in schemas if not _schema_allowed(s, warehouse))
    if bad:
        allowed = "ops" if warehouse == "ops" else "gold, silver_*"
        return {
            **out,
            "ok": False,
            "error": "sql_rejected",
            "detail": f"schema(s) not allowed for warehouse={warehouse}: {bad}; allowed: {allowed}",
            "rows": [],
        }

    if not db_path.is_file():
        return {
            **out,
            "ok": False,
            "error": "duckdb_missing",
            "detail": f"DuckDB file not found for warehouse={warehouse}",
            "rows": [],
        }

    wrapped = f"select * from ({body}) as _det_q limit {capped + 1}"
    duckdb = require_duckdb()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = con.execute(wrapped)
        cols = [d[0] for d in result.description]
        fetched = result.fetchall()
    except Exception as exc:
        return {
            **out,
            "ok": False,
            "error": "query_failed",
            "detail": str(exc),
            "rows": [],
        }
    finally:
        con.close()

    truncated = len(fetched) > capped
    rows = [
        {
            "index": i,
            "data": {
                col: _jsonable(val) for col, val in zip(cols, row, strict=True)
            },
        }
        for i, row in enumerate(fetched[:capped])
    ]
    return {
        **out,
        "ok": True,
        "schemas": sorted(schemas),
        "truncated": truncated,
        "rows": rows,
        "errors": [],
    }
