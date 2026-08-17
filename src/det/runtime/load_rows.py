from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

from det.logging import get_logger
from det.runtime.coerce import CoerceError, coerce_record
from det.runtime.meta import attach_meta
from det.runtime.naming import BronzeNamingConfig, apply_naming
from det.sources.base import SourceRow
from det.validation.jsonschema_validator import SchemaValidationError, validate_record

logger = get_logger(__name__)


class CountingIter:
    """Wrap an iterable and count yielded items without materializing them."""

    def __init__(self, items: Iterable[Any]) -> None:
        self._items = items
        self.n = 0

    def __iter__(self) -> Iterator[Any]:
        for item in self._items:
            self.n += 1
            yield item


def iter_bronze_rows(
    source_rows: Iterable[SourceRow],
    *,
    schema: dict[str, Any],
    naming: BronzeNamingConfig,
    extract_run_datetime: str,
    interval_start_datetime: str,
    interval_end_datetime: str,
    bronze_loaded_at: str,
    mapper: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    log_every: int = 10_000,
) -> Iterator[dict[str, Any]]:
    """
    One-pass coerce → validate → attach_meta. Fail-closed on the first bad row.
    """
    n = 0
    for source_row in source_rows:
        named = apply_naming(source_row.data, naming)
        if mapper is not None:
            named = mapper(named)
        try:
            typed = coerce_record(named, schema)
        except CoerceError as exc:
            raise SchemaValidationError(str(exc), errors=[str(exc)]) from exc
        validate_record(typed, schema)
        n += 1
        if n == 1 or (log_every >= 1 and n % log_every == 0):
            logger.info("bronze row progress", rows=n)
        yield attach_meta(
            typed,
            filename=source_row.filename,
            extract_run_datetime=extract_run_datetime,
            interval_start_datetime=interval_start_datetime,
            interval_end_datetime=interval_end_datetime,
            bronze_loaded_at=bronze_loaded_at,
        )


def chain_first(first: Any, rest: Iterable[Any]) -> Iterator[Any]:
    yield first
    yield from rest
