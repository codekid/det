"""DET-owned lake I/O: local pathlib, s3://, gs://, and in-memory tests.

The lake root is a runtime location (default ``./data/lake``), not a per-pipeline
contract. ``destination.type`` still only chooses bronze serving.

``DET_LAKE_MODE`` (local|cloud) is policy around the URI shape — not a second
writer path. Unset defaults to local.
"""

from __future__ import annotations

import fcntl
import fnmatch
import io
import os
import secrets
import shutil
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, Literal

from det.logging import get_logger

logger = get_logger(__name__)

DEFAULT_LAKE_REL = "./data/lake"
ENV_LAKE_MODE = "DET_LAKE_MODE"
LakeMode = Literal["local", "cloud"]
_OBJECT_SCHEMES = ("s3://", "gs://", "gcs://")
_MEMORY_STORES: dict[str, dict[str, bytes]] = {}
_MEMORY_DIRS: dict[str, set[str]] = {}
# Parallel generation counters for memory:// CAS (store_id → key → version str).
_MEMORY_VERSIONS: dict[str, dict[str, str]] = {}
_MEMORY_GENS: dict[str, int] = {}
_CLOUD_EXPERIMENTAL_WARNED = False


class ObjectVersionConflict(Exception):
    """Conditional put/delete failed: object version no longer matches."""


class ObjectCasUnsupported(RuntimeError):
    """Object store cannot apply strong preconditions (fail closed for leases)."""


def is_lake_uri(spec: str) -> bool:
    text = (spec or "").strip()
    return text.startswith((*_OBJECT_SCHEMES, "memory://"))


def is_object_lake_spec(spec: str) -> bool:
    text = (spec or "").strip()
    return text.startswith(_OBJECT_SCHEMES)


def lake_mode_from_env(env: Mapping[str, str] | None = None) -> LakeMode:
    """Parse ``DET_LAKE_MODE``. Unset or empty → ``local``."""
    environ = os.environ if env is None else env
    raw = (environ.get(ENV_LAKE_MODE) or "").strip().lower()
    if not raw:
        return "local"
    if raw in {"local", "cloud"}:
        return raw  # type: ignore[return-value]
    raise ValueError(
        f"{ENV_LAKE_MODE} must be 'local' or 'cloud', got {raw!r}"
    )


def validate_lake_mode(spec: str, mode: LakeMode) -> None:
    """Raise ``ValueError`` when lake URI shape disagrees with ``DET_LAKE_MODE``."""
    text = (spec or "").strip() or DEFAULT_LAKE_REL
    if mode == "local":
        if is_object_lake_spec(text):
            raise ValueError(
                f"DET_LAKE_MODE=local forbids object-store lakes "
                f"(got {text!r}); use a filesystem path or set "
                f"{ENV_LAKE_MODE}=cloud"
            )
        return
    # cloud
    if text.startswith("memory://"):
        raise ValueError(
            f"DET_LAKE_MODE=cloud forbids memory:// lakes (got {text!r}); "
            f"use s3:// or gs://, or set {ENV_LAKE_MODE}=local"
        )
    if not is_object_lake_spec(text):
        raise ValueError(
            f"DET_LAKE_MODE=cloud requires an s3:// or gs:// lake "
            f"(got {text!r}); set {ENV_LAKE_MODE}=local for filesystem lakes"
        )


def pick_lake_spec(
    *,
    cli_lake_path: str | None = None,
    destination_path: str | None = None,
    settings_lake_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> str:
    """
    Resolve the lake root spec (URI or local path). First hit wins:

    1. CLI ``--lake-path`` / ``DetSettings.lake_override``
    2. Explicit ``destination.path`` in YAML
    3. ``DetSettings.lake_path`` (usually from ``DET_LAKE_PATH`` via ``from_env``)
    4. ``DET_LAKE_PATH``
    5. ``./data/lake``
    """
    if cli_lake_path is not None and str(cli_lake_path).strip():
        return str(cli_lake_path).strip()
    if destination_path is not None and str(destination_path).strip():
        return str(destination_path).strip()
    if settings_lake_path is not None and str(settings_lake_path).strip():
        return str(settings_lake_path).strip()
    environ = os.environ if env is None else env
    env_val = environ.get("DET_LAKE_PATH")
    if env_val is not None and str(env_val).strip():
        return str(env_val).strip()
    return DEFAULT_LAKE_REL


def open_lake(
    spec: str,
    project_root: Path,
    *,
    env: Mapping[str, str] | None = None,
    lake_mode: LakeMode | None = None,
) -> LakeRef:
    """Open a lake root. Never pass a URI through ``pathlib.Path``."""
    global _CLOUD_EXPERIMENTAL_WARNED
    text = (spec or "").strip() or DEFAULT_LAKE_REL
    mode = lake_mode if lake_mode is not None else lake_mode_from_env(env)
    validate_lake_mode(text, mode)
    if mode == "cloud" and not _CLOUD_EXPERIMENTAL_WARNED:
        logger.warning(
            "object-store lake: CI MinIO/GCS soaks cover extract→Iceberg; "
            "shared multi-writer / Glue catalogs are still out of scope",
            lake_mode=mode,
            lake=text,
        )
        _CLOUD_EXPERIMENTAL_WARNED = True
    if text.startswith("memory://"):
        return _open_memory(text)
    if text.startswith("s3://"):
        fs = _import_fsspec("s3")
        from det.runtime.object_store import fsspec_s3_kwargs

        key = text[len("s3://") :].rstrip("/")
        return LakeRef(
            _FsspecBackend(fs.filesystem("s3", **fsspec_s3_kwargs(env)), "s3"),
            key,
        )
    if text.startswith(("gs://", "gcs://")):
        fs = _import_fsspec("gcs")
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
    hint = f"pip install 'det[{extra}]'"
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


class LakeRef:
    """Path-like reference under a lake filesystem (local, object, or memory)."""

    def __init__(self, backend: _Backend, key: str) -> None:
        self._backend = backend
        self._key = key

    def __truediv__(self, other: str | os.PathLike[str] | LakeRef) -> LakeRef:
        part = other._key if isinstance(other, LakeRef) else str(other)
        part = part.strip("/\\")
        if not part:
            return self
        return LakeRef(self._backend, self._backend.join(self._key, part))

    def __str__(self) -> str:
        return self._backend.display(self._key)

    def __repr__(self) -> str:
        return f"LakeRef({self})"

    def __fspath__(self) -> str:
        if not self.is_local:
            raise TypeError(f"{self} is not a local path")
        return self._key

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LakeRef):
            return self._backend.same(other._backend) and self._key == other._key
        if isinstance(other, Path) and self.is_local:
            return self.to_path() == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self._backend.identity(), self._key))

    def __lt__(self, other: object) -> bool:
        if isinstance(other, LakeRef):
            return str(self) < str(other)
        if isinstance(other, Path):
            return str(self) < str(other)
        return NotImplemented

    @property
    def name(self) -> str:
        return self._backend.name(self._key)

    @property
    def parent(self) -> LakeRef:
        return LakeRef(self._backend, self._backend.parent(self._key))

    @property
    def suffix(self) -> str:
        return PurePosixPath(self.name).suffix

    @property
    def parts(self) -> tuple[str, ...]:
        if self.is_local:
            return self.to_path().parts
        return tuple(p for p in str(self).replace("\\", "/").split("/") if p)

    @property
    def is_local(self) -> bool:
        return self._backend.kind == "local"

    def to_path(self) -> Path:
        if not self.is_local:
            raise TypeError(f"{self} is not a local path")
        return Path(self._key)

    def with_name(self, name: str) -> LakeRef:
        return self.parent / name

    def as_posix(self) -> str:
        if self.is_local:
            return self.to_path().as_posix()
        return str(self)

    def resolve(self) -> LakeRef:
        return self

    def mkdir(self, parents: bool = True, exist_ok: bool = True) -> None:
        self._backend.mkdir(self._key, parents=parents, exist_ok=exist_ok)

    def exists(self) -> bool:
        return self._backend.exists(self._key)

    def is_file(self) -> bool:
        return self._backend.is_file(self._key)

    def is_dir(self) -> bool:
        return self._backend.is_dir(self._key)

    def iterdir(self) -> Iterator[LakeRef]:
        for child in self._backend.iterdir(self._key):
            yield LakeRef(self._backend, child)

    def rglob(self, pattern: str) -> Iterator[LakeRef]:
        for child in self._backend.iter_files(self._key):
            ref = LakeRef(self._backend, child)
            rel = self._backend.rel_key(self._key, child)
            if _match_rglob(pattern, rel, ref.name):
                yield ref

    def glob(self, pattern: str) -> Iterator[LakeRef]:
        yield from self.rglob(pattern)

    def open(
        self,
        mode: str = "r",
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        return self._backend.open(
            self._key, mode, encoding=encoding, errors=errors, newline=newline
        )

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        with self.open("r", encoding=encoding, errors=errors) as fh:
            return fh.read()

    def write_text(self, data: str, encoding: str = "utf-8") -> None:
        self.parent.mkdir(parents=True, exist_ok=True)
        with self.open("w", encoding=encoding) as fh:
            fh.write(data)

    def read_bytes(self) -> bytes:
        with self.open("rb") as fh:
            return fh.read()

    def write_bytes(self, data: bytes) -> None:
        self.parent.mkdir(parents=True, exist_ok=True)
        with self.open("wb") as fh:
            fh.write(data)

    def create_exclusive(self, data: bytes) -> str:
        """Create this key only if absent. Returns an opaque object version string.

        Raises ``FileExistsError`` if the key already exists. On ``s3://`` /
        ``gs://`` this uses conditional create (no soft exists+wb fallback).
        """
        self.parent.mkdir(parents=True, exist_ok=True)
        return self._backend.create_exclusive(self._key, data)

    def object_version(self) -> str | None:
        """Opaque version (etag / generation / local stamp) or None if missing."""
        return self._backend.object_version(self._key)

    def replace_if_match(self, expected_version: str, data: bytes) -> str:
        """Replace bytes only if ``expected_version`` still matches. Returns new version."""
        return self._backend.replace_if_match(self._key, expected_version, data)

    def delete_if_match(self, expected_version: str) -> bool:
        """Delete only if ``expected_version`` still matches. False if gone/mismatched."""
        return self._backend.delete_if_match(self._key, expected_version)

    def unlink(self, missing_ok: bool = False) -> None:
        self._backend.unlink(self._key, missing_ok=missing_ok)

    def rmtree(self, ignore_errors: bool = False) -> None:
        self._backend.rmtree(self._key, ignore_errors=ignore_errors)

    def stat(self) -> SimpleNamespace:
        return SimpleNamespace(st_size=self._backend.size(self._key))

    def relative_to(self, other: LakeRef | Path) -> PurePosixPath:
        if isinstance(other, Path):
            if not self.is_local:
                raise ValueError(f"{self} is not relative to {other}")
            return PurePosixPath(self.to_path().relative_to(other).as_posix())
        rel = self._backend.rel_key(other._key, self._key)
        if rel is None:
            raise ValueError(f"{self} is not relative to {other}")
        return PurePosixPath(rel)

    def is_relative_to(self, other: LakeRef | Path) -> bool:
        try:
            self.relative_to(other)
            return True
        except ValueError:
            return False


def _match_rglob(pattern: str, rel: str | None, name: str) -> bool:
    if rel is None:
        return False
    posix = rel.replace("\\", "/")
    if pattern in {"*", "**", "**/*"}:
        return True
    if pattern.startswith("**/"):
        return fnmatch.fnmatch(name, pattern[3:]) or fnmatch.fnmatch(posix, pattern)
    if "/" in pattern or "**" in pattern:
        return fnmatch.fnmatch(posix, pattern) or fnmatch.fnmatch(name, pattern)
    return fnmatch.fnmatch(name, pattern)


class _Backend:
    kind: str = "abstract"

    def identity(self) -> object:
        return id(self)

    def same(self, other: _Backend) -> bool:
        return self.identity() == other.identity()

    def join(self, key: str, part: str) -> str:
        raise NotImplementedError

    def parent(self, key: str) -> str:
        raise NotImplementedError

    def name(self, key: str) -> str:
        raise NotImplementedError

    def display(self, key: str) -> str:
        raise NotImplementedError

    def mkdir(self, key: str, *, parents: bool, exist_ok: bool) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def is_file(self, key: str) -> bool:
        raise NotImplementedError

    def is_dir(self, key: str) -> bool:
        raise NotImplementedError

    def iterdir(self, key: str) -> list[str]:
        raise NotImplementedError

    def iter_files(self, key: str) -> list[str]:
        raise NotImplementedError

    def open(self, key: str, mode: str, **kwargs: Any) -> Any:
        raise NotImplementedError

    def unlink(self, key: str, *, missing_ok: bool) -> None:
        raise NotImplementedError

    def rmtree(self, key: str, *, ignore_errors: bool) -> None:
        raise NotImplementedError

    def size(self, key: str) -> int:
        raise NotImplementedError

    def create_exclusive(self, key: str, data: bytes) -> str:
        raise NotImplementedError

    def object_version(self, key: str) -> str | None:
        raise NotImplementedError

    def replace_if_match(self, key: str, expected_version: str, data: bytes) -> str:
        raise NotImplementedError

    def delete_if_match(self, key: str, expected_version: str) -> bool:
        raise NotImplementedError

    def rel_key(self, root: str, child: str) -> str | None:
        raise NotImplementedError


def _local_cas_lock_path(key: str) -> Path:
    path = Path(key)
    return path.with_name(f".{path.name}.detcas")


def _local_gen_path(key: str) -> Path:
    path = Path(key)
    return path.with_name(f".{path.name}.detgen")


def _local_read_gen(key: str) -> int:
    try:
        return int(_local_gen_path(key).read_text(encoding="utf-8").strip() or "0")
    except (OSError, ValueError):
        return 0


def _local_write_gen(key: str, gen: int) -> None:
    _local_gen_path(key).write_text(str(gen), encoding="utf-8")


@contextmanager
def _local_cas_guard(key: str) -> Iterator[None]:
    """Exclusive flock around local version check + mutate (cross-process CAS)."""
    lock_path = _local_cas_lock_path(key)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class _LocalBackend(_Backend):
    kind = "local"

    def identity(self) -> object:
        return "local"

    def join(self, key: str, part: str) -> str:
        return str(Path(key) / part)

    def parent(self, key: str) -> str:
        return str(Path(key).parent)

    def name(self, key: str) -> str:
        return Path(key).name

    def display(self, key: str) -> str:
        return key

    def mkdir(self, key: str, *, parents: bool, exist_ok: bool) -> None:
        Path(key).mkdir(parents=parents, exist_ok=exist_ok)

    def exists(self, key: str) -> bool:
        return Path(key).exists()

    def is_file(self, key: str) -> bool:
        return Path(key).is_file()

    def is_dir(self, key: str) -> bool:
        return Path(key).is_dir()

    def iterdir(self, key: str) -> list[str]:
        p = Path(key)
        if not p.is_dir():
            return []
        return sorted(str(c) for c in p.iterdir())

    def iter_files(self, key: str) -> list[str]:
        p = Path(key)
        if not p.exists():
            return []
        if p.is_file():
            return [str(p)]
        return sorted(str(c) for c in p.rglob("*") if c.is_file())

    def open(self, key: str, mode: str, **kwargs: Any) -> Any:
        encoding = kwargs.get("encoding")
        errors = kwargs.get("errors")
        newline = kwargs.get("newline")
        if "b" in mode:
            return Path(key).open(mode)
        return Path(key).open(mode, encoding=encoding, errors=errors, newline=newline)

    def unlink(self, key: str, *, missing_ok: bool) -> None:
        Path(key).unlink(missing_ok=missing_ok)

    def rmtree(self, key: str, *, ignore_errors: bool) -> None:
        shutil.rmtree(key, ignore_errors=ignore_errors)

    def size(self, key: str) -> int:
        return Path(key).stat().st_size

    def create_exclusive(self, key: str, data: bytes) -> str:
        Path(key).parent.mkdir(parents=True, exist_ok=True)
        with _local_cas_guard(key):
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            fd = os.open(key, flags, 0o644)
            try:
                os.write(fd, data)
            finally:
                os.close(fd)
            # Keep prior .detgen across delete/recreate so versions cannot rewind.
            _local_write_gen(key, _local_read_gen(key) + 1)
            version = self.object_version(key)
            if version is None:
                raise RuntimeError(f"local exclusive create missing version for {key}")
            return version

    def object_version(self, key: str) -> str | None:
        path = Path(key)
        if not path.is_file():
            return None
        st = path.stat()
        return f"local:{st.st_mtime_ns}:{st.st_size}:{st.st_ino}:{_local_read_gen(key)}"

    def replace_if_match(self, key: str, expected_version: str, data: bytes) -> str:
        with _local_cas_guard(key):
            current = self.object_version(key)
            if current is None or current != expected_version:
                raise ObjectVersionConflict(key)
            path = Path(key)
            tmp = path.with_name(
                f".{path.name}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
            )
            try:
                tmp.write_bytes(data)
                os.replace(tmp, path)
            except Exception:
                tmp.unlink(missing_ok=True)
                raise
            _local_write_gen(key, _local_read_gen(key) + 1)
            version = self.object_version(key)
            if version is None:
                raise RuntimeError(f"local replace missing version for {key}")
            return version

    def delete_if_match(self, key: str, expected_version: str) -> bool:
        with _local_cas_guard(key):
            current = self.object_version(key)
            if current is None:
                return False
            if current != expected_version:
                return False
            Path(key).unlink(missing_ok=True)
            # Preserve .detgen so a later create_exclusive increments, not restarts.
            return True

    def rel_key(self, root: str, child: str) -> str | None:
        try:
            return Path(child).resolve().relative_to(Path(root).resolve()).as_posix()
        except ValueError:
            return None


class _FsspecBackend(_Backend):
    kind = "object"

    def __init__(self, fs: Any, scheme: str) -> None:
        self.fs = fs
        self.scheme = scheme

    def identity(self) -> object:
        return (self.kind, self.scheme, id(self.fs))

    def join(self, key: str, part: str) -> str:
        base = key.rstrip("/")
        return f"{base}/{part}" if base else part

    def parent(self, key: str) -> str:
        if "/" not in key.rstrip("/"):
            return ""
        return key.rstrip("/").rsplit("/", 1)[0]

    def name(self, key: str) -> str:
        return key.rstrip("/").rsplit("/", 1)[-1] if key else ""

    def display(self, key: str) -> str:
        return f"{self.scheme}://{key}"

    def mkdir(self, key: str, *, parents: bool, exist_ok: bool) -> None:
        if not key:
            return
        self.fs.makedirs(key, exist_ok=True)

    def exists(self, key: str) -> bool:
        try:
            return bool(self.fs.exists(key))
        except FileNotFoundError:
            return False

    def is_file(self, key: str) -> bool:
        try:
            return bool(self.fs.isfile(key))
        except FileNotFoundError:
            return False

    def is_dir(self, key: str) -> bool:
        try:
            if self.fs.isdir(key):
                return True
        except FileNotFoundError:
            return False
        # Prefix with children counts as a dir even without a marker object.
        return bool(self.iterdir(key))

    def iterdir(self, key: str) -> list[str]:
        prefix = key.rstrip("/")
        try:
            listing = self.fs.ls(prefix, detail=False) if prefix else []
        except FileNotFoundError:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for item in listing:
            text = str(item).rstrip("/")
            if text == prefix:
                continue
            if text not in seen:
                seen.add(text)
                out.append(text)
        return sorted(out)

    def iter_files(self, key: str) -> list[str]:
        prefix = key.rstrip("/")
        try:
            found = self.fs.find(prefix)
        except FileNotFoundError:
            return []
        return sorted(str(p) for p in found)

    def open(self, key: str, mode: str, **kwargs: Any) -> Any:
        encoding = kwargs.get("encoding")
        errors = kwargs.get("errors")
        newline = kwargs.get("newline")
        binary_mode = mode.replace("t", "")
        if "b" not in binary_mode:
            binary_mode = binary_mode + "b"
        fh = self.fs.open(key, binary_mode)
        if "b" in mode and "t" not in mode:
            return fh
        return io.TextIOWrapper(
            fh, encoding=encoding or "utf-8", errors=errors, newline=newline
        )

    def unlink(self, key: str, *, missing_ok: bool) -> None:
        try:
            self.fs.rm(key)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def rmtree(self, key: str, *, ignore_errors: bool) -> None:
        try:
            self.fs.rm(key, recursive=True)
        except FileNotFoundError:
            if not ignore_errors:
                raise
        except Exception:
            if not ignore_errors:
                raise

    def size(self, key: str) -> int:
        info = self.fs.info(key)
        return int(info.get("size") or 0)

    def create_exclusive(self, key: str, data: bytes) -> str:
        """Strong exclusive create on s3/gs. No soft exists+wb fallback."""
        parent = self.parent(key)
        if parent:
            self.mkdir(parent, parents=True, exist_ok=True)
        if self.scheme == "s3":
            return self._s3_put(key, data, if_none_match="*")
        if self.scheme == "gs":
            return self._gcs_put(key, data, if_generation_match=0)
        raise ObjectCasUnsupported(
            f"lake exclusive create requires s3:// or gs://, got scheme={self.scheme!r}"
        )

    def object_version(self, key: str) -> str | None:
        if not self.exists(key):
            return None
        try:
            info = self.fs.info(key)
        except FileNotFoundError:
            return None
        if self.scheme == "s3":
            etag = info.get("ETag") or info.get("etag")
            if etag is None:
                return None
            return f"etag:{str(etag).strip(chr(34))}"
        if self.scheme == "gs":
            gen = info.get("generation")
            if gen is None:
                # gcsfs sometimes nests under custom metadata keys
                gen = info.get("Generation")
            if gen is None:
                return None
            return f"gen:{gen}"
        raise ObjectCasUnsupported(
            f"object_version unsupported for scheme={self.scheme!r}"
        )

    def replace_if_match(self, key: str, expected_version: str, data: bytes) -> str:
        if self.scheme == "s3":
            etag = _strip_version_prefix(expected_version, "etag:")
            return self._s3_put(key, data, if_match=etag)
        if self.scheme == "gs":
            gen = int(_strip_version_prefix(expected_version, "gen:"))
            return self._gcs_put(key, data, if_generation_match=gen)
        raise ObjectCasUnsupported(
            f"replace_if_match unsupported for scheme={self.scheme!r}"
        )

    def delete_if_match(self, key: str, expected_version: str) -> bool:
        if self.scheme == "s3":
            etag = _strip_version_prefix(expected_version, "etag:")
            return self._s3_delete(key, if_match=etag)
        if self.scheme == "gs":
            gen = int(_strip_version_prefix(expected_version, "gen:"))
            return self._gcs_delete(key, if_generation_match=gen)
        raise ObjectCasUnsupported(
            f"delete_if_match unsupported for scheme={self.scheme!r}"
        )

    def _s3_put(
        self,
        key: str,
        data: bytes,
        *,
        if_none_match: str | None = None,
        if_match: str | None = None,
    ) -> str:
        bucket, obj = _split_s3_key(key)
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": obj,
            "Body": data,
        }
        if if_none_match is not None:
            kwargs["IfNoneMatch"] = if_none_match
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        try:
            out = self.fs.call_s3("put_object", **kwargs)
        except FileExistsError:
            raise
        except Exception as exc:
            _raise_s3_cas(exc, key, create=(if_none_match is not None))
            raise  # pragma: no cover
        self.fs.invalidate_cache(key)
        etag = (out or {}).get("ETag") if isinstance(out, dict) else None
        if not etag:
            version = self.object_version(key)
            if version is None:
                raise ObjectCasUnsupported(
                    f"s3 put succeeded but etag missing for {key}"
                )
            return version
        return f"etag:{str(etag).strip(chr(34))}"

    def _s3_delete(self, key: str, *, if_match: str) -> bool:
        bucket, obj = _split_s3_key(key)
        try:
            self.fs.call_s3(
                "delete_object",
                Bucket=bucket,
                Key=obj,
                IfMatch=if_match,
            )
        except FileNotFoundError:
            return False
        except Exception as exc:
            if _is_precondition_failed(exc):
                return False
            if _is_not_found(exc):
                return False
            raise ObjectCasUnsupported(
                f"s3 conditional delete failed for {key}: {exc}"
            ) from exc
        self.fs.invalidate_cache(key)
        return True

    def _gcs_put(
        self,
        key: str,
        data: bytes,
        *,
        if_generation_match: int,
    ) -> str:
        try:
            if if_generation_match == 0:
                self.fs.pipe(key, data, mode="create")
            else:
                self._gcs_upload_generation_match(key, data, if_generation_match)
        except Exception as exc:
            if if_generation_match == 0 and (
                _is_precondition_failed(exc) or self.exists(key)
            ):
                raise FileExistsError(key) from exc
            if _is_precondition_failed(exc):
                raise ObjectVersionConflict(key) from exc
            raise ObjectCasUnsupported(
                f"gcs conditional put failed for {key}: {exc}"
            ) from exc
        version = self.object_version(key)
        if version is None:
            raise ObjectCasUnsupported(f"gcs put succeeded but generation missing for {key}")
        return version

    def _gcs_upload_generation_match(
        self, key: str, data: bytes, generation: int
    ) -> None:
        from urllib.parse import quote

        bucket, obj = _split_gcs_key(key)
        base = getattr(self.fs, "_location", "https://storage.googleapis.com")
        upload_path = f"{base}/upload/storage/v1/b/{quote(bucket)}/o"
        self.fs.call(
            "POST",
            upload_path,
            uploadType="media",
            name=obj,
            ifGenerationMatch=str(generation),
            data=data,
            json_out=True,
        )
        self.fs.invalidate_cache(key)

    def _gcs_delete(self, key: str, *, if_generation_match: int) -> bool:
        try:
            bucket, obj = _split_gcs_key(key)
            self.fs.call(
                "DELETE",
                "b/{}/o/{}",
                bucket,
                obj,
                ifGenerationMatch=str(if_generation_match),
            )
        except FileNotFoundError:
            return False
        except Exception as exc:
            if _is_precondition_failed(exc):
                return False
            if _is_not_found(exc):
                return False
            raise ObjectCasUnsupported(
                f"gcs conditional delete failed for {key}: {exc}"
            ) from exc
        self.fs.invalidate_cache(key)
        return True

    def rel_key(self, root: str, child: str) -> str | None:
        root_n = root.rstrip("/")
        child_n = child.rstrip("/")
        if not root_n:
            return child_n
        if child_n == root_n:
            return "."
        prefix = root_n + "/"
        if child_n.startswith(prefix):
            return child_n[len(prefix) :]
        return None


def _split_s3_key(key: str) -> tuple[str, str]:
    text = key.lstrip("/")
    if "/" not in text:
        raise ValueError(f"s3 key must be bucket/object, got {key!r}")
    bucket, obj = text.split("/", 1)
    return bucket, obj


def _strip_version_prefix(version: str, prefix: str) -> str:
    if version.startswith(prefix):
        return version[len(prefix) :]
    return version


def _split_gcs_key(key: str) -> tuple[str, str]:
    return _split_s3_key(key)


def _is_precondition_failed(exc: BaseException) -> bool:
    """True only for explicit 412 / precondition conflict signals (no message heuristics)."""
    names = {"PreconditionFailed", "Conflict", "ObjectVersionConflict"}
    codes = {"PreconditionFailed", "412", "ConditionalRequestConflict"}
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if type(cur).__name__ in names:
            return True
        response = getattr(cur, "response", None)
        if isinstance(response, dict):
            err = response.get("Error") or {}
            if str(err.get("Code") or "") in codes:
                return True
            status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
            if status == 412:
                return True
        status = getattr(cur, "status_code", None)
        if status is None:
            status = getattr(cur, "code", None)
        if status == 412:
            return True
        cur = cur.__cause__  # type: ignore[assignment]
    return False


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    name = type(exc).__name__
    if name in {"NoSuchKey", "NotFound"}:
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        err = response.get("Error") or {}
        if str(err.get("Code") or "") in {"NoSuchKey", "404", "NotFound"}:
            return True
    return False


def _raise_s3_cas(exc: BaseException, key: str, *, create: bool) -> None:
    if _is_precondition_failed(exc):
        if create:
            raise FileExistsError(key) from exc
        raise ObjectVersionConflict(key) from exc
    raise ObjectCasUnsupported(f"s3 conditional put failed for {key}: {exc}") from exc


class _MemoryBackend(_Backend):
    kind = "memory"

    def __init__(
        self,
        store: dict[str, bytes],
        dirs: set[str],
        versions: dict[str, str],
        *,
        store_id: str,
        display_root: str,
    ) -> None:
        self.store = store
        self._dirs = dirs
        self._versions = versions
        self._store_id = store_id
        self.display_root = display_root.rstrip("/")

    def _bump(self) -> str:
        n = _MEMORY_GENS.get(self._store_id, 1)
        _MEMORY_GENS[self._store_id] = n + 1
        return f"mem:{n}"

    def identity(self) -> object:
        return ("memory", id(self.store))

    def join(self, key: str, part: str) -> str:
        base = key.strip("/")
        return f"{base}/{part}" if base else part

    def parent(self, key: str) -> str:
        key = key.strip("/")
        if "/" not in key:
            return ""
        return key.rsplit("/", 1)[0]

    def name(self, key: str) -> str:
        key = key.strip("/")
        return key.rsplit("/", 1)[-1] if key else ""

    def display(self, key: str) -> str:
        key = key.strip("/")
        return f"{self.display_root}/{key}" if key else self.display_root

    def mkdir(self, key: str, *, parents: bool, exist_ok: bool) -> None:
        del parents, exist_ok
        key = key.strip("/")
        if key:
            self._dirs.add(key)
            # Implicit parents.
            cur = key
            while "/" in cur:
                cur = cur.rsplit("/", 1)[0]
                self._dirs.add(cur)

    def exists(self, key: str) -> bool:
        return self.is_file(key) or self.is_dir(key)

    def is_file(self, key: str) -> bool:
        return key.strip("/") in self.store

    def is_dir(self, key: str) -> bool:
        key = key.strip("/")
        if key in self._dirs:
            return True
        prefix = f"{key}/" if key else ""
        return any(k.startswith(prefix) for k in self.store) or any(
            d.startswith(prefix) for d in self._dirs if prefix
        )

    def iterdir(self, key: str) -> list[str]:
        key = key.strip("/")
        prefix = f"{key}/" if key else ""
        children: set[str] = set()
        for item in list(self.store) + list(self._dirs):
            if prefix and not item.startswith(prefix):
                continue
            rest = item[len(prefix) :] if prefix else item
            if not rest:
                continue
            child = rest.split("/", 1)[0]
            children.add(f"{prefix}{child}" if prefix else child)
        return sorted(children)

    def iter_files(self, key: str) -> list[str]:
        key = key.strip("/")
        prefix = f"{key}/" if key else ""
        if key in self.store:
            return [key]
        return sorted(k for k in self.store if not prefix or k.startswith(prefix))

    def open(self, key: str, mode: str, **kwargs: Any) -> Any:
        key = key.strip("/")
        encoding = kwargs.get("encoding") or "utf-8"
        errors = kwargs.get("errors")
        newline = kwargs.get("newline")
        if "r" in mode and "+" not in mode:
            if key not in self.store:
                raise FileNotFoundError(key)
            raw = io.BytesIO(self.store[key])
            if "b" in mode:
                return raw
            return io.TextIOWrapper(raw, encoding=encoding, errors=errors, newline=newline)
        buf = _MemoryWriter(self.store, self._versions, key, self)
        if "b" in mode:
            return buf
        return io.TextIOWrapper(buf, encoding=encoding, errors=errors, newline=newline)

    def unlink(self, key: str, *, missing_ok: bool) -> None:
        key = key.strip("/")
        if key in self.store:
            del self.store[key]
            self._versions.pop(key, None)
            return
        if not missing_ok:
            raise FileNotFoundError(key)

    def rmtree(self, key: str, *, ignore_errors: bool) -> None:
        del ignore_errors
        key = key.strip("/")
        prefix = f"{key}/" if key else ""
        for item in list(self.store):
            if item == key or (prefix and item.startswith(prefix)):
                del self.store[item]
        self._dirs = {
            d for d in self._dirs if d != key and not (prefix and d.startswith(prefix))
        }

    def size(self, key: str) -> int:
        key = key.strip("/")
        if key not in self.store:
            raise FileNotFoundError(key)
        return len(self.store[key])

    def create_exclusive(self, key: str, data: bytes) -> str:
        key = key.strip("/")
        parent = self.parent(key)
        if parent:
            self.mkdir(parent, parents=True, exist_ok=True)
        if self.store.setdefault(key, data) is not data:
            raise FileExistsError(key)
        version = self._bump()
        self._versions[key] = version
        return version

    def object_version(self, key: str) -> str | None:
        key = key.strip("/")
        if key not in self.store:
            return None
        return self._versions.get(key)

    def replace_if_match(self, key: str, expected_version: str, data: bytes) -> str:
        key = key.strip("/")
        current = self._versions.get(key) if key in self.store else None
        if current is None or current != expected_version:
            raise ObjectVersionConflict(key)
        self.store[key] = data
        version = self._bump()
        self._versions[key] = version
        return version

    def delete_if_match(self, key: str, expected_version: str) -> bool:
        key = key.strip("/")
        current = self._versions.get(key) if key in self.store else None
        if current is None:
            return False
        if current != expected_version:
            return False
        del self.store[key]
        self._versions.pop(key, None)
        return True

    def rel_key(self, root: str, child: str) -> str | None:
        root_n = root.strip("/")
        child_n = child.strip("/")
        if not root_n:
            return child_n
        if child_n == root_n:
            return "."
        prefix = root_n + "/"
        if child_n.startswith(prefix):
            return child_n[len(prefix) :]
        return None


class _MemoryWriter(io.BytesIO):
    def __init__(
        self, store: dict[str, bytes], versions: dict[str, str], key: str, backend: _MemoryBackend
    ) -> None:
        super().__init__()
        self._store = store
        self._versions = versions
        self._key = key
        self._backend = backend

    def flush(self) -> None:
        super().flush()
        self._store[self._key] = self.getvalue()
        self._versions[self._key] = self._backend._bump()

    def close(self) -> None:
        if not self.closed:
            self._store[self._key] = self.getvalue()
            self._versions[self._key] = self._backend._bump()
        super().close()

