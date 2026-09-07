"""Path-like lake references."""

from __future__ import annotations

import fnmatch
import os
import re
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any

from det.runtime.lake.backends.base import _Backend


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
        """Match ``pattern`` against direct children (pathlib-style shallow glob).

        Patterns containing ``**`` recurse via :meth:`rglob`. Otherwise only
        immediate children from :meth:`iterdir` are considered.
        """
        if "**" in pattern:
            yield from self.rglob(pattern)
            return
        for child in self.iterdir():
            if fnmatch.fnmatch(child.name, pattern):
                yield child

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


def _glob_pattern_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a glob with pathlib-style ``**`` (zero or more directories)."""
    out: list[str] = ["^"]
    i = 0
    n = len(pattern)
    while i < n:
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        elif pattern[i] == "[":
            j = i + 1
            if j < n and pattern[j] == "!":
                j += 1
            # fnmatch: a ']' immediately after '[' / '[!' is literal.
            if j < n and pattern[j] == "]":
                j += 1
            while j < n and pattern[j] != "]":
                j += 1
            if j >= n:
                out.append(re.escape(pattern[i]))
                i += 1
            else:
                cls = pattern[i : j + 1]
                # fnmatch [!abc] → regex [^abc]
                if cls.startswith("[!"):
                    out.append("[^" + cls[2:])
                else:
                    out.append(cls)
                i = j + 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    out.append("$")
    return re.compile("".join(out))


def _match_rglob(pattern: str, rel: str | None, name: str) -> bool:
    if rel is None:
        return False
    posix = rel.replace("\\", "/")
    if pattern in {"*", "**", "**/*"}:
        return True
    if "**" in pattern:
        if _glob_pattern_to_regex(pattern).match(posix):
            return True
        # Leading **/ may also match on the basename (e.g. **/*.txt).
        if pattern.startswith("**/") and fnmatch.fnmatch(name, pattern[3:]):
            return True
        return False
    if "/" in pattern:
        return fnmatch.fnmatch(posix, pattern) or fnmatch.fnmatch(name, pattern)
    return fnmatch.fnmatch(name, pattern)
