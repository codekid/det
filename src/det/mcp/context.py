from __future__ import annotations

import os
from pathlib import Path


class PathSandboxError(ValueError):
    """Raised when a path escapes DET_PROJECT_ROOT."""


def project_root() -> Path:
    """Resolve DET_PROJECT_ROOT (default: cwd)."""
    raw = os.environ.get("DET_PROJECT_ROOT")
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.cwd().resolve()


def resolve_under_root(path: str | Path, *, root: Path | None = None) -> Path:
    """
    Resolve path relative to project root and reject escapes.

    Absolute paths are allowed only when they remain under root.
    """
    base = root or project_root()
    p = Path(path).expanduser()
    if not p.is_absolute():
        p = (base / p).resolve()
    else:
        p = p.resolve()
    try:
        p.relative_to(base)
    except ValueError as exc:
        raise PathSandboxError(f"path escapes project root {base}: {p}") from exc
    return p


def pipelines_dir(root: Path | None = None) -> Path:
    return (root or project_root()) / "configs" / "pipelines"


def schemas_dir(root: Path | None = None) -> Path:
    return (root or project_root()) / "schemas"
