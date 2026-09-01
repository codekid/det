"""Shared constants, limits, and path helpers for DET MCP inspect."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from det.mcp.context import PathSandboxError, project_root, resolve_under_root
from det.runtime.config import PipelineConfig, load_pipeline_config
from det.runtime.lake import LakeRef, is_lake_uri, open_lake
from det.runtime.lake import relpath as lake_relpath
from det.runtime.manifest import is_committed_raw_dir
from det.runtime.meta import from_partition_value, resolve_interval
from det.runtime.pipelines import resolve_pipeline_ref

DEFAULT_LIST_LIMIT = 200
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


def clamp_list_limit(limit: int | None = None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    return max(1, min(int(limit), DEFAULT_LIST_LIMIT))


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
    return dirname[len(prefix):]


def _run_dict(
    *,
    interval_start: str,
    interval_end: str,
    extract_run_datetime: str,
    path: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "interval_start": interval_start,
        "interval_end": interval_end,
        "extract_run_datetime": extract_run_datetime,
    }
    if path is not None:
        out["path"] = path
    return out


def _run_key(run: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(run["interval_start"]),
        str(run["interval_end"]),
        str(run["extract_run_datetime"]),
    )


def walk_hive_runs(
    dataset_dir: Path | LakeRef,
    *,
    root: Path,
    limit: int,
    interval_start: str | None = None,
    interval_end: str | None = None,
    normalize_iso: bool = True,
    require_committed: bool = False,
) -> list[dict[str, Any]]:
    """Walk hive interval/extract-run dirs; optionally filter and normalize to ISO."""
    out: list[dict[str, Any]] = []
    if not dataset_dir.is_dir():
        return out

    window: tuple[str, str] | None = None
    if interval_start is not None:
        window = resolve_interval(interval_start, interval_end)

    for start_dir in sorted(dataset_dir.iterdir()):
        if not start_dir.is_dir():
            continue
        start_raw = _parse_hive_key(start_dir.name, "__interval_start_datetime=")
        if start_raw is None:
            continue
        start_val = from_partition_value(start_raw) if normalize_iso else start_raw
        if window is not None and not (window[0] <= start_val < window[1]):
            continue
        for end_dir in sorted(start_dir.iterdir()):
            if not end_dir.is_dir():
                continue
            end_raw = _parse_hive_key(end_dir.name, "__interval_end_datetime=")
            if end_raw is None:
                continue
            end_val = from_partition_value(end_raw) if normalize_iso else end_raw
            for run_dir in sorted(end_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                run_raw = _parse_hive_key(run_dir.name, "__extract_run_datetime=")
                if run_raw is None:
                    continue
                if require_committed and not is_committed_raw_dir(run_dir):
                    continue
                run_val = from_partition_value(run_raw) if normalize_iso else run_raw
                out.append(
                    _run_dict(
                        interval_start=start_val,
                        interval_end=end_val,
                        extract_run_datetime=run_val,
                        path=_rel(run_dir, root),
                    )
                )
                if len(out) >= limit:
                    return out
    return out


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
