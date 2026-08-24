"""
Resolve pipeline refs to YAML paths under ``configs/pipelines/``.

Refs (in order):
1. Absolute or relative path to an existing ``.yaml`` / ``.yml`` file
2. Canonical id ``provider.source`` → ``configs/pipelines/{provider}/{source}.yaml``
3. Slash form ``provider/source`` (with or without ``configs/pipelines/`` prefix)
4. Clear error listing known pipeline ids
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from det.errors import DetNotFoundError
from det.runtime.ids import CANONICAL_ID_RE, fs_dataset_parts, validate_canonical_id


class PipelineRefError(DetNotFoundError, FileNotFoundError):
    """Pipeline ref could not be resolved."""


@dataclass(frozen=True)
class ResolvedPipeline:
    """Result of resolving a pipeline ref against a project root."""

    ref: str
    path: Path
    canonical_id: str
    project_root: Path

    @property
    def relative_path(self) -> str:
        try:
            return str(self.path.resolve().relative_to(self.project_root.resolve()))
        except ValueError:
            return str(self.path.resolve())


def resolve_project_root(explicit: Path | str | None = None) -> Path:
    """
    Project root resolution: ``--project-root`` > ``DET_PROJECT_ROOT`` > cwd.
    """
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    env = os.environ.get("DET_PROJECT_ROOT")
    if env and env.strip():
        return Path(env).expanduser().resolve()
    return Path.cwd().resolve()


def pipelines_dir(project_root: Path) -> Path:
    return project_root / "configs" / "pipelines"


def discover_pipeline_files(project_root: Path) -> list[Path]:
    root = pipelines_dir(project_root)
    if not root.is_dir():
        return []
    return sorted(
        p for p in root.rglob("*") if p.is_file() and p.suffix in {".yaml", ".yml"}
    )


def canonical_id_from_path(path: Path, project_root: Path) -> str:
    """Derive ``provider.source`` from a path under ``configs/pipelines/``."""
    pipelines_root = pipelines_dir(project_root).resolve()
    rel = path.resolve().relative_to(pipelines_root)
    return ".".join(rel.with_suffix("").parts)


def list_pipeline_ids(project_root: Path) -> list[str]:
    return [
        canonical_id_from_path(p, project_root)
        for p in discover_pipeline_files(project_root)
    ]


def _known_hint(project_root: Path) -> str:
    known = list_pipeline_ids(project_root)
    if not known:
        return f"(no pipelines under {pipelines_dir(project_root)})"
    preview = ", ".join(known[:20])
    more = f" (+{len(known) - 20} more)" if len(known) > 20 else ""
    return f"known: {preview}{more}"


def _as_yaml_candidate(path: Path) -> list[Path]:
    """Return path plus .yaml/.yml variants when suffix is missing."""
    if path.suffix in {".yaml", ".yml"}:
        return [path]
    return [Path(str(path) + ".yaml"), Path(str(path) + ".yml")]


def resolve_pipeline_ref(
    ref: str | Path,
    *,
    project_root: Path | str | None = None,
) -> ResolvedPipeline:
    """
    Resolve a pipeline ref to an on-disk YAML path.

    Accepts canonical ids (``noaa.storm_events``), slash forms, or file paths.
    """
    root = resolve_project_root(project_root)
    raw = str(ref).strip()
    if not raw:
        raise PipelineRefError("pipeline ref must be non-empty")

    p = Path(raw).expanduser()

    # 1) Explicit path (absolute or relative) to a YAML file that exists.
    if p.suffix in {".yaml", ".yml"}:
        candidate = p if p.is_absolute() else (root / p)
        candidate = candidate.resolve()
        if candidate.is_file():
            try:
                cid = canonical_id_from_path(candidate, root)
            except ValueError:
                # Outside configs/pipelines — still allow explicit path.
                cid = candidate.stem
            return ResolvedPipeline(
                ref=raw, path=candidate, canonical_id=cid, project_root=root
            )
        raise PipelineRefError(
            f"pipeline file not found: {candidate} ({_known_hint(root)})"
        )

    # 2) Canonical dotted id → configs/pipelines/{provider}/{source}.yaml
    if CANONICAL_ID_RE.match(raw):
        parts = fs_dataset_parts(validate_canonical_id(raw))
        base = pipelines_dir(root).joinpath(*parts[:-1], parts[-1])
        for candidate in _as_yaml_candidate(base):
            if candidate.is_file():
                resolved = candidate.resolve()
                return ResolvedPipeline(
                    ref=raw,
                    path=resolved,
                    canonical_id=canonical_id_from_path(resolved, root),
                    project_root=root,
                )

    # 3) Slash form: noaa/storm_events or configs/pipelines/noaa/storm_events
    slash = raw.replace("\\", "/")
    if "/" in slash:
        rel = slash.removeprefix("./")
        if rel.startswith("configs/pipelines/"):
            rel = rel[len("configs/pipelines/") :]
        base = pipelines_dir(root).joinpath(*Path(rel).parts)
        for candidate in _as_yaml_candidate(base):
            if candidate.is_file():
                resolved = candidate.resolve()
                return ResolvedPipeline(
                    ref=raw,
                    path=resolved,
                    canonical_id=canonical_id_from_path(resolved, root),
                    project_root=root,
                )
        # Also try as project-relative path without forcing pipelines/
        for candidate in _as_yaml_candidate(root / slash):
            if candidate.is_file():
                resolved = candidate.resolve()
                try:
                    cid = canonical_id_from_path(resolved, root)
                except ValueError:
                    cid = resolved.stem
                return ResolvedPipeline(
                    ref=raw, path=resolved, canonical_id=cid, project_root=root
                )

    # 4) Single-segment stem under configs/pipelines/ (legacy flat files only).
    if "." not in raw and "/" not in raw and "\\" not in raw:
        base = pipelines_dir(root) / raw
        for candidate in _as_yaml_candidate(base):
            if candidate.is_file():
                resolved = candidate.resolve()
                return ResolvedPipeline(
                    ref=raw,
                    path=resolved,
                    canonical_id=canonical_id_from_path(resolved, root),
                    project_root=root,
                )

    raise PipelineRefError(
        f"pipeline not found: {raw!r}. "
        f"Use a canonical id (provider.source), slash form, or YAML path. "
        f"{_known_hint(root)}"
    )
