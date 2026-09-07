"""Abstract lake backend."""

from __future__ import annotations

from typing import Any


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
