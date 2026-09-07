"""Open lake roots and related helpers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from det.logging import get_logger
from det.optional_deps import pip_extra_hint
from det.runtime.lake.backends.fsspec_backend import _FsspecBackend
from det.runtime.lake.backends.local import _LocalBackend
from det.runtime.lake.backends.memory import (
    _MEMORY_DIRS,
    _MEMORY_GENS,
    _MEMORY_STORES,
    _MEMORY_VERSIONS,
    _MemoryBackend,
)
from det.runtime.lake.mode import DEFAULT_LAKE_REL, LakeMode, lake_mode_from_env, validate_lake_mode
from det.runtime.lake.ref import LakeRef

logger = get_logger("det.runtime.lake")

_CLOUD_EXPERIMENTAL_WARNED = False


def _lake_pkg():
    """Resolve package module at call time so tests can monkeypatch it."""
    import det.runtime.lake as lake_mod

    return lake_mod


def open_lake(
    spec: str,
    project_root: Path,
    *,
    env: Mapping[str, str] | None = None,
    lake_mode: LakeMode | None = None,
) -> LakeRef:
    """Open a lake root. Never pass a URI through ``pathlib.Path``."""
    global _CLOUD_EXPERIMENTAL_WARNED
    lake = _lake_pkg()
    text = (spec or "").strip() or DEFAULT_LAKE_REL
    mode = lake_mode if lake_mode is not None else lake_mode_from_env(env)
    validate_lake_mode(text, mode)
    # Read/write the package attribute so resets via det.runtime.lake apply.
    if mode == "cloud" and not lake._CLOUD_EXPERIMENTAL_WARNED:
        logger.warning(
            "object-store lake: CI MinIO/GCS soaks cover extract→Iceberg; "
            "set DET_ICEBERG_CATALOG=rest|glue for a managed metastore "
            "(default hadoop uses version-hint on the lake)",
            lake_mode=mode,
            lake=text,
        )
        _CLOUD_EXPERIMENTAL_WARNED = True
        lake._CLOUD_EXPERIMENTAL_WARNED = True
    if text.startswith("memory://"):
        return _open_memory(text)
    if text.startswith("s3://"):
        # Look up via package so tests can monkeypatch lake._import_fsspec.
        fs = lake._import_fsspec("s3")
        from det.runtime.object_store import fsspec_s3_kwargs

        key = text[len("s3://") :].rstrip("/")
        return LakeRef(
            _FsspecBackend(fs.filesystem("s3", **fsspec_s3_kwargs(env)), "s3"),
            key,
        )
    if text.startswith(("gs://", "gcs://")):
        fs = lake._import_fsspec("gcs")
        from det.runtime.object_store import fsspec_gcs_kwargs

        rest = text.split("://", 1)[1].rstrip("/")
        return LakeRef(
            _FsspecBackend(fs.filesystem("gcs", **fsspec_gcs_kwargs(env)), "gs"),
            rest,
        )
    path = Path(text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    else:
        path = path.resolve()
    return LakeRef(_LocalBackend(), str(path))


def clear_memory_lakes() -> None:
    _MEMORY_STORES.clear()
    _MEMORY_DIRS.clear()
    _MEMORY_VERSIONS.clear()
    _MEMORY_GENS.clear()


def reset_lake_mode_warning_for_tests() -> None:
    """Test helper: allow the cloud experimental warning to fire again."""
    global _CLOUD_EXPERIMENTAL_WARNED
    _CLOUD_EXPERIMENTAL_WARNED = False
    _lake_pkg()._CLOUD_EXPERIMENTAL_WARNED = False


def relpath(path: Path | LakeRef, root: Path) -> str:
    """Project-relative path for local refs; URI string for object lakes."""
    if isinstance(path, LakeRef):
        if path.is_local:
            try:
                return str(path.to_path().resolve().relative_to(root.resolve()))
            except ValueError:
                return str(path)
        return str(path)
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def _import_fsspec(extra: Literal["s3", "gcs"]):
    hint = pip_extra_hint(extra)
    try:
        import fsspec
    except ImportError as exc:
        raise ImportError(
            f"Object lake {extra} requires the optional extra: {hint}"
        ) from exc
    pkg = "s3fs" if extra == "s3" else "gcsfs"
    try:
        __import__(pkg)
    except ImportError as exc:
        raise ImportError(
            f"Object lake {extra} requires the optional extra: {hint}"
        ) from exc
    return fsspec


def _open_memory(spec: str) -> LakeRef:
    rest = spec[len("memory://") :]
    store_id, _, prefix = rest.partition("/")
    store_id = store_id or "_default"
    store = _MEMORY_STORES.setdefault(store_id, {})
    dirs = _MEMORY_DIRS.setdefault(store_id, set())
    versions = _MEMORY_VERSIONS.setdefault(store_id, {})
    key = prefix.strip("/")
    return LakeRef(
        _MemoryBackend(
            store,
            dirs,
            versions,
            store_id=store_id,
            display_root=f"memory://{store_id}",
        ),
        key,
    )
