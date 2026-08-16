from __future__ import annotations

from collections.abc import Iterable, Iterator


def iter_chunks[T](items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield successive lists of at most *size* items from *items*."""
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
