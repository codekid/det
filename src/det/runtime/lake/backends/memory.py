"""In-memory lake backend for tests."""

from __future__ import annotations

import io
from typing import Any

from det.runtime.lake.backends.base import _Backend
from det.runtime.lake.errors import ObjectVersionConflict

_MEMORY_STORES: dict[str, dict[str, bytes]] = {}
_MEMORY_DIRS: dict[str, set[str]] = {}
# Parallel generation counters for memory:// CAS (store_id → key → version str).
_MEMORY_VERSIONS: dict[str, dict[str, str]] = {}
_MEMORY_GENS: dict[str, int] = {}


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
        initial = self.store.get(key, b"") if "a" in mode else b""
        buf = _MemoryWriter(self.store, self._versions, key, self, initial=initial)
        if "a" in mode:
            buf.seek(0, io.SEEK_END)
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
        # Mutate the shared set in place so all backends for this store_id stay synced.
        for d in list(self._dirs):
            if d == key or (prefix and d.startswith(prefix)):
                self._dirs.discard(d)

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
        if key in self.store:
            raise FileExistsError(key)
        self.store[key] = data
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
        self,
        store: dict[str, bytes],
        versions: dict[str, str],
        key: str,
        backend: _MemoryBackend,
        *,
        initial: bytes = b"",
    ) -> None:
        super().__init__(initial)
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
