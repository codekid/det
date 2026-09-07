"""Local filesystem lake backend and CAS generation helpers."""

from __future__ import annotations

import fcntl
import os
import secrets
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from det.logging import get_logger
from det.runtime.lake.backends.base import _Backend
from det.runtime.lake.errors import ObjectVersionConflict

logger = get_logger("det.runtime.lake")


def _lake_pkg():
    """Resolve package module at call time so tests can monkeypatch it."""
    import det.runtime.lake as lake_mod

    return lake_mod


def _local_cas_lock_path(key: str) -> Path:
    path = Path(key)
    return path.with_name(f".{path.name}.detcas")


def _local_gen_path(key: str) -> Path:
    path = Path(key)
    return path.with_name(f".{path.name}.detgen")


def _is_local_sidecar(path: Path) -> bool:
    name = path.name
    if name.endswith(".detcas") or name.endswith(".detgen"):
        return True
    # Atomic write temps from ``_local_write_gen`` / ``replace_if_match``:
    # ``.{name}.tmp.{pid}.{token_hex(4)}`` (token is 8 lowercase hex chars).
    if not name.startswith(".") or ".tmp." not in name:
        return False
    _prefix, _, suffix = name.rpartition(".tmp.")
    if not _prefix:
        return False
    pid, sep, token = suffix.partition(".")
    return bool(
        sep
        and pid.isdigit()
        and len(token) == 8
        and token == token.lower()
        and all(c in "0123456789abcdef" for c in token)
    )


def _local_read_gen(key: str) -> int:
    """Return generation; missing file → 0; corrupt/unreadable → raise."""
    path = _local_gen_path(key)
    try:
        text = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return 0
    except OSError as exc:
        raise RuntimeError(
            f"local lease generation unreadable for {key}: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            f"local lease generation not utf-8 for {key}"
        ) from exc
    if not text:
        raise RuntimeError(f"local lease generation empty for {key}")
    try:
        value = int(text)
    except ValueError as exc:
        raise RuntimeError(
            f"local lease generation corrupt for {key}: {text!r}"
        ) from exc
    if value < 0:
        raise RuntimeError(f"local lease generation negative for {key}: {value}")
    return value


def _local_write_gen(key: str, gen: int) -> None:
    path = _local_gen_path(key)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}.{secrets.token_hex(4)}")
    try:
        tmp.write_text(str(gen), encoding="utf-8")
        # Use package ``os`` so ``lake_mod.os.replace`` monkeypatches apply.
        _lake_pkg().os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


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
        return sorted(str(c) for c in p.iterdir() if not _is_local_sidecar(c))

    def iter_files(self, key: str) -> list[str]:
        p = Path(key)
        if not p.exists():
            return []
        if p.is_file():
            return [] if _is_local_sidecar(p) else [str(p)]
        return sorted(
            str(c)
            for c in p.rglob("*")
            if c.is_file() and not _is_local_sidecar(c)
        )

    def open(self, key: str, mode: str, **kwargs):
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
            try:
                # Keep prior .detgen across delete/recreate so versions cannot rewind.
                # Look up via package so tests can monkeypatch lake._local_write_gen.
                lake = _lake_pkg()
                lake._local_write_gen(key, lake._local_read_gen(key) + 1)
            except Exception:
                Path(key).unlink(missing_ok=True)
                raise
            version = self.object_version(key)
            if version is None:
                raise RuntimeError(f"local exclusive create missing version for {key}")
            return version

    def object_version(self, key: str) -> str | None:
        path = Path(key)
        if not path.is_file():
            return None
        st = path.stat()
        return f"local:{st.st_mtime_ns}:{st.st_size}:{st.st_ino}:{_lake_pkg()._local_read_gen(key)}"

    def replace_if_match(self, key: str, expected_version: str, data: bytes) -> str:
        with _local_cas_guard(key):
            current = self.object_version(key)
            if current is None or current != expected_version:
                raise ObjectVersionConflict(key)
            path = Path(key)
            tmp = path.with_name(
                f".{path.name}.tmp.{os.getpid()}.{secrets.token_hex(4)}"
            )
            lake = _lake_pkg()
            prior_gen = lake._local_read_gen(key)
            next_gen = prior_gen + 1
            gen_committed = False
            try:
                tmp.write_bytes(data)
                # Bump generation before publishing bytes so a failed metadata
                # write cannot leave new object content under a stale stamp.
                lake._local_write_gen(key, next_gen)
                gen_committed = True
                lake.os.replace(tmp, path)
            except Exception:
                tmp.unlink(missing_ok=True)
                if gen_committed:
                    try:
                        if prior_gen == 0:
                            lake._local_gen_path(key).unlink(missing_ok=True)
                        else:
                            lake._local_write_gen(key, prior_gen)
                    except Exception as restore_exc:
                        logger.warning(
                            "failed to restore local lease generation after publish error",
                            key=key,
                            prior_gen=prior_gen,
                            error=str(restore_exc),
                        )
                raise
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
