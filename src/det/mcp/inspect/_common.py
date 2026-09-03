"""Shared constants, limits, and path helpers for DET MCP inspect."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from det.mcp.context import PathSandboxError, project_root, resolve_under_root
from det.runtime.bronze_runs import (
    DEFAULT_LIST_LIMIT,
    clamp_list_limit,
    run_dict,
    walk_hive_runs,
)
from det.runtime.config import PipelineConfig, load_pipeline_config
from det.runtime.lake import LakeRef, is_lake_uri, open_lake
from det.runtime.lake import relpath as lake_relpath
from det.runtime.pipelines import resolve_pipeline_ref

# Re-exports for ``det.mcp.inspect`` / ``_partitions`` (historical private names).
_run_dict = run_dict

__all__ = [
    "DEFAULT_LIST_LIMIT",
    "DEFAULT_SAMPLE_LIMIT",
    "MAX_SAMPLE_LIMIT",
    "MAX_WIRE_CHARS",
    "RUN_KEY_FIELDS",
    "SampleStage",
    "_assert_under_bronze",
    "_assert_under_raw",
    "_load_pipeline",
    "_parse_hive_key",
    "_posix_parts",
    "_quote_ident",
    "_rel",
    "_resolve_lake_run_path",
    "_root",
    "_run_dict",
    "_run_key",
    "clamp_list_limit",
    "clamp_sample_limit",
    "resolve_migrate_validate_limit",
    "walk_hive_runs",
]

DEFAULT_SAMPLE_LIMIT = 5
MAX_SAMPLE_LIMIT = 50
MAX_WIRE_CHARS = 4000

SampleStage = Literal["wire", "rows", "named", "coerced"]
RUN_KEY_FIELDS = ("interval_start", "interval_end", "extract_run_datetime")


def clamp_sample_limit(limit: int | None = None) -> int:
    """Caller-controlled sample size; default 5, clamped to 1..50."""
    if limit is None:
        return DEFAULT_SAMPLE_LIMIT
    return max(1, min(int(limit), MAX_SAMPLE_LIMIT))


def resolve_migrate_validate_limit(limit: int) -> int | None:
    """Map MCP migrate_dry_run validate_limit: 0 → full partition, 1–50 → clamp."""
    value = int(limit)
    if value == 0:
        return None
    if 1 <= value <= MAX_SAMPLE_LIMIT:
        return clamp_sample_limit(value)
    raise ValueError(
        f"validate_limit must be 0 (full partition, gated) or 1..{MAX_SAMPLE_LIMIT}, "
        f"got {value}"
    )


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _load_pipeline(pipeline: str, root: Path) -> tuple[PipelineConfig, Path]:
    resolved = resolve_pipeline_ref(pipeline, project_root=root)
    return load_pipeline_config(resolved.path), resolved.path


def _rel(path: Path | LakeRef, root: Path) -> str:
    return lake_relpath(path, root)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _parse_hive_key(dirname: str, prefix: str) -> str | None:
    if not dirname.startswith(prefix):
        return None
    return dirname[len(prefix) :]


def _run_key(run: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(run["interval_start"]),
        str(run["interval_end"]),
        str(run["extract_run_datetime"]),
    )


def _assert_under_raw(run_dir: Path | LakeRef, *, root: Path) -> None:
    parts = _posix_parts(run_dir)
    if "raw" not in parts:
        raise PathSandboxError(
            f"run path must be under a lake raw/ tree: {_rel(run_dir, root)}"
        )


def _assert_under_bronze(run_dir: Path | LakeRef, *, root: Path) -> None:
    parts = _posix_parts(run_dir)
    if "bronze" not in parts:
        raise PathSandboxError(
            f"run path must be under a lake bronze/ tree: {_rel(run_dir, root)}"
        )


def _posix_parts(path: Path | LakeRef) -> tuple[str, ...]:
    if isinstance(path, LakeRef):
        return tuple(str(path).replace("\\", "/").split("/"))
    return path.resolve().parts


def _resolve_lake_run_path(run_path: str, *, root: Path) -> Path | LakeRef:
    if is_lake_uri(run_path):
        return open_lake(run_path, root)
    return resolve_under_root(run_path, root=root)
