"""Local DuckDB warehouse path helpers (analytics / ops)."""

from __future__ import annotations

import os
from pathlib import Path


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
