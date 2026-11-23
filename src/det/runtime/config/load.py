"""Load pipeline YAML and path helpers."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from det.runtime.config.pipeline import PipelineConfig
from det.runtime.pipelines import resolve_pipeline_ref


def parse_override_assignment(assignment: str) -> tuple[str, str]:
    """Split ``dotted.key=value`` into ``(key, raw_value)``."""
    key, sep, value = str(assignment).partition("=")
    if not sep:
        raise ValueError(f"Override must be dotted.key=value, got: {assignment!r}")
    key = key.strip()
    if not key:
        raise ValueError(f"Override key must be non-empty: {assignment!r}")
    return key, value


def assert_compatible_override_keys(assignments: Sequence[str]) -> list[str]:
    """
    Return override keys; reject duplicates and dotted-prefix overlaps.

    ``destination=…`` plus ``destination.bq_dataset=…`` is order-dependent under
    last-value-wins apply. Approval digests sort ``--set`` flags, so overlapping
    keys would let one digest map to two effective configs.
    """
    keys: list[str] = []
    seen: set[str] = set()
    for assignment in assignments:
        key, _ = parse_override_assignment(assignment)
        if key in seen:
            raise ValueError(
                f"duplicate --set key {key!r}; repeating a key would desync "
                "the approval digest from last-value-wins load order"
            )
        for other in seen:
            if key.startswith(f"{other}.") or other.startswith(f"{key}."):
                raise ValueError(
                    f"--set key {key!r} overlaps {other!r}; parent and nested "
                    "keys cannot both be set (approval digest would not bind a "
                    "unique effective config)"
                )
        seen.add(key)
        keys.append(key)
    return keys


def apply_overrides(raw: dict[str, Any], assignments: Sequence[str]) -> dict[str, Any]:
    """
    Apply `dotted.key=value` assignments over a parsed pipeline mapping.

    Values are parsed as YAML so ints, bools, and null work as expected.
    """
    assert_compatible_override_keys(assignments)
    out = deepcopy(raw)
    for assignment in assignments:
        key, value = parse_override_assignment(assignment)
        parts = [p for p in key.split(".") if p]
        if not parts:
            raise ValueError(f"Override key must be non-empty: {assignment!r}")
        cursor: dict[str, Any] = out
        for part in parts[:-1]:
            existing = cursor.get(part)
            if not isinstance(existing, dict):
                existing = {}
                cursor[part] = existing
            cursor = existing
        cursor[parts[-1]] = yaml.safe_load(value)
    return out


def load_pipeline_config(
    path: Path | str,
    overrides: Sequence[str] | None = None,
) -> PipelineConfig:
    p = Path(path)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Pipeline config must be a mapping: {p}")
    if overrides:
        raw = apply_overrides(raw, overrides)
    return PipelineConfig.model_validate(raw)


def load_pipeline(
    pipeline: PipelineConfig | Path | str,
    *,
    project_root: Path,
    overrides: Sequence[str] | None = None,
) -> PipelineConfig:
    """
    Load a pipeline from a config object, YAML path, or canonical id.

    Canonical ids (``provider.source``) and slash forms resolve under
    ``{project_root}/configs/pipelines/`` — same rules as the CLI.

    ``overrides`` apply only when loading from a path or id. Passing a
    ``PipelineConfig`` with non-empty overrides raises ``ValueError``.
    """
    if isinstance(pipeline, PipelineConfig):
        if overrides:
            raise ValueError(
                "overrides cannot be applied when pipeline is already a "
                "PipelineConfig; pass a path or canonical id instead"
            )
        return pipeline

    resolved = resolve_pipeline_ref(pipeline, project_root=project_root)
    return load_pipeline_config(resolved.path, overrides=overrides)


def resolve_path(base: Path, maybe_relative: str) -> Path:
    path = Path(maybe_relative)
    if path.is_absolute():
        return path
    return (base / path).resolve()
