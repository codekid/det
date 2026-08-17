"""Advisory lake sampling for view-materialized dbt.stg relations."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.destinations.models import bronze_dataset_dir, lake_root
from det.logging import get_logger
from det.runtime.config import PipelineConfig
from det.runtime.lake import LakeRef

logger = get_logger(__name__)


@dataclass(frozen=True)
class ViewSizeWarning:
    message: str


def _bronze_jsonl_files(bronze_root: Path | LakeRef) -> list[Path | LakeRef]:
    if not bronze_root.is_dir():
        return []
    return sorted(p for p in bronze_root.rglob("data.jsonl") if p.is_file())


def _estimate_parent_rows(
    files: list[Path | LakeRef], *, sample_lines: int = 50
) -> int | None:
    """Estimate parent row count from total bytes / mean bytes-per-line."""
    if not files:
        return None
    total_bytes = 0
    sample_bytes = 0
    sample_count = 0
    for path in files:
        try:
            total_bytes += path.stat().st_size
        except OSError:
            continue
        if sample_count >= sample_lines:
            continue
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if sample_count >= sample_lines:
                        break
                    if not line.strip():
                        continue
                    sample_bytes += len(line.encode("utf-8"))
                    sample_count += 1
        except OSError:
            continue
    if sample_count == 0 or sample_bytes == 0 or total_bytes == 0:
        return None
    mean = sample_bytes / sample_count
    return max(1, int(total_bytes / mean))


def _iter_sample_rows(
    files: list[Path | LakeRef],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in files:
        if len(rows) >= limit:
            break
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if len(rows) >= limit:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        rows.append(obj)
        except OSError:
            continue
    return rows


def _mean_array_len(rows: list[dict[str, Any]], path: str) -> float | None:
    lengths: list[int] = []
    for row in rows:
        val = row.get(path)
        if val is None:
            lengths.append(0)
            continue
        if isinstance(val, list):
            lengths.append(len(val))
            continue
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, list):
                lengths.append(len(parsed))
    if not lengths:
        return None
    return sum(lengths) / len(lengths)


def collect_view_size_warnings(
    config: PipelineConfig,
    *,
    project_root: Path,
    lake_path: str | Path | None = None,
) -> list[ViewSizeWarning]:
    """
    Sample bronze and return warnings for large view-materialized relations.

    Never raises for missing lake data; returns an empty list when there is
    nothing to evaluate.
    """
    stg = config.dbt.stg
    warn_cfg = stg.view_warn
    if not warn_cfg.enabled:
        return []

    view_relations = {
        name: rel
        for name, rel in stg.relations.items()
        if rel.materialized == "view"
    }
    if not view_relations:
        return []

    root = project_root.resolve()
    if lake_path is not None:
        # Rebuild destination path override for bronze_dataset_dir
        dest = config.destination.model_copy(update={"path": str(lake_path)})
        cfg = config.model_copy(update={"destination": dest})
        bronze_root = bronze_dataset_dir(cfg, root)
    else:
        bronze_root = bronze_dataset_dir(config, root)

    files = _bronze_jsonl_files(bronze_root)
    if not files:
        return []

    parent_est = _estimate_parent_rows(files)
    if parent_est is None:
        return []

    warnings: list[ViewSizeWarning] = []
    if parent_est >= warn_cfg.parent_rows:
        warnings.append(
            ViewSizeWarning(
                message=(
                    f"view_warn: estimated parent bronze rows ~{parent_est:,} "
                    f"(>= {warn_cfg.parent_rows:,}); prefer materialized: table "
                    f"for large relations on {config.bronze_dataset()}"
                )
            )
        )

    sample = _iter_sample_rows(files, limit=warn_cfg.sample_rows)
    if not sample:
        return warnings

    for name, rel in view_relations.items():
        path = rel.path or name
        mean_len = _mean_array_len(sample, path)
        if mean_len is None:
            continue
        child_est = int(parent_est * mean_len)
        if child_est >= warn_cfg.child_rows:
            warnings.append(
                ViewSizeWarning(
                    message=(
                        f"view_warn: relation {name!r} (path {path!r}) estimated "
                        f"~{child_est:,} unnested rows "
                        f"(parent~{parent_est:,} × mean_len~{mean_len:.2f}) "
                        f">= {warn_cfg.child_rows:,}; consider "
                        f"dbt.stg.relations.{name}.materialized: table"
                    )
                )
            )
    return warnings


def emit_view_size_warnings(
    config: PipelineConfig,
    *,
    project_root: Path,
    lake_path: str | Path | None = None,
) -> list[ViewSizeWarning]:
    """Collect warnings and log them (advisory; never fails)."""
    warnings = collect_view_size_warnings(
        config, project_root=project_root, lake_path=lake_path
    )
    for w in warnings:
        logger.warning(w.message)
    return warnings


def resolve_lake_path(config: PipelineConfig, project_root: Path) -> str:
    return str(lake_root(config.destination, project_root))
