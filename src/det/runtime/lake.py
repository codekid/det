"""DET-owned lake I/O: local pathlib, s3://, gs://, and in-memory tests.

The lake root is a runtime location (default ``./data/lake``), not a per-pipeline
contract. ``destination.type`` still only chooses bronze serving.

``DET_LAKE_MODE`` (local|cloud) is policy around the URI shape — not a second
writer path. Unset defaults to local.
"""

from __future__ import annotations

import fnmatch
import io
import os
import shutil
from collections.abc import Iterator, Mapping
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
_CLOUD_EXPERIMENTAL_WARNED = False


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
    env: Mapping[str, str] | None = None,
) -> str:
    """
    Resolve the lake root spec (URI or local path). First hit wins:

    1. CLI ``--lake-path``
    2. Explicit ``destination.path`` in YAML
    3. ``DET_LAKE_PATH``
    4. ``./data/lake``
    """
    if cli_lake_path is not None and str(cli_lake_path).strip():
        return str(cli_lake_path).strip()
    if destination_path is not None and str(destination_path).strip():
        return str(destination_path).strip()
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
) -> LakeRef:
    """Open a lake root. Never pass a URI through ``pathlib.Path``."""
    global _CLOUD_EXPERIMENTAL_WARNED
    text = (spec or "").strip() or DEFAULT_LAKE_REL
    mode = lake_mode_from_env(env)
    validate_lake_mode(text, mode)
    if mode == "cloud" and not _CLOUD_EXPERIMENTAL_WARNED:
        logger.warning(
            "object-store lake: CI MinIO soak covers extract→Iceberg→DuckDB "
            "iceberg_scan; shared multi-writer / Glue catalogs are still out of scope",
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
        rest = text.split("://", 1)[1].rstrip("/")
        return LakeRef(_FsspecBackend(fs.filesystem("gcs"), "gs"), rest)
    path = Path(text)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    else:
        path = path.resolve()
    return LakeRef(_LocalBackend(), str(path))


def clear_memory_lakes() -> None:
    _MEMORY_STORES.clear()
    _MEMORY_DIRS.clear()


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
    key = prefix.strip("/")
    return LakeRef(
        _MemoryBackend(store, dirs, display_root=f"memory://{store_id}"),
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

    def create_exclusive(self, data: bytes) -> None:
        """Create this key only if it does not exist. Raises FileExistsError if it does."""
        self.parent.mkdir(parents=True, exist_ok=True)
        self._backend.create_exclusive(self._key, data)

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

    def create_exclusive(self, key: str, data: bytes) -> None:
        raise NotImplementedError

    def rel_key(self, root: str, child: str) -> str | None:
        raise NotImplementedError


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

    def create_exclusive(self, key: str, data: bytes) -> None:
        Path(key).parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        fd = os.open(key, flags, 0o644)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)

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

    def create_exclusive(self, key: str, data: bytes) -> None:
        parent = self.parent(key)
        if parent:
            self.mkdir(parent, parents=True, exist_ok=True)
        try:
            with self.fs.open(key, "xb") as fh:
                fh.write(data)
            return
        except FileExistsError:
            raise
        except (OSError, ValueError, TypeError):
            if self.exists(key):
                raise FileExistsError(key) from None
            with self.fs.open(key, "wb") as fh:
                fh.write(data)

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


class _MemoryBackend(_Backend):
    kind = "memory"

    def __init__(
        self, store: dict[str, bytes], dirs: set[str], display_root: str
    ) -> None:
        self.store = store
        self._dirs = dirs
        self.display_root = display_root.rstrip("/")

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
        buf = _MemoryWriter(self.store, key)
        if "b" in mode:
            return buf
        return io.TextIOWrapper(buf, encoding=encoding, errors=errors, newline=newline)

    def unlink(self, key: str, *, missing_ok: bool) -> None:
        key = key.strip("/")
        if key in self.store:
            del self.store[key]
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

    def create_exclusive(self, key: str, data: bytes) -> None:
        key = key.strip("/")
        parent = self.parent(key)
        if parent:
            self.mkdir(parent, parents=True, exist_ok=True)
        if self.store.setdefault(key, data) is not data:
            raise FileExistsError(key)

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
    def __init__(self, store: dict[str, bytes], key: str) -> None:
        super().__init__()
        self._store = store
        self._key = key

    def flush(self) -> None:
        super().flush()
        self._store[self._key] = self.getvalue()

    def close(self) -> None:
        if not self.closed:
            self._store[self._key] = self.getvalue()
        super().close()

