from __future__ import annotations

from typing import Any

_START = "__interval_start_datetime"
_END = "__interval_end_datetime"
_RUN = "__extract_run_datetime"


def bronze_run_identity(
    records: list[dict[str, Any]],
) -> tuple[str, str, str] | None:
    """Return (interval_start, interval_end, extract_run) from the first row."""
    if not records:
        return None
    row = records[0]
    start, end, run = row.get(_START), row.get(_END), row.get(_RUN)
    if start is None or end is None or run is None:
        return None
    start_s, end_s, run_s = str(start), str(end), str(run)
    if not start_s.strip() or not end_s.strip() or not run_s.strip():
        return None
    return start_s, end_s, run_s


def require_bronze_run_identity(
    records: list[dict[str, Any]],
) -> tuple[str, str, str]:
    """
    Identity of this SQL bronze write. Every row must share the same extract run.

    Missing or mixed meta fails closed — replace-by-run cannot guess a DELETE key.
    """
    identity = bronze_run_identity(records)
    if identity is None:
        raise ValueError(
            "SQL bronze replace-by-run requires "
            f"{_START}, {_END}, and {_RUN} on the load batch"
        )
    start, end, run = identity
    for i, row in enumerate(records):
        if (
            str(row.get(_START)) != start
            or str(row.get(_END)) != end
            or str(row.get(_RUN)) != run
        ):
            raise ValueError(
                f"record[{i}] bronze run identity does not match the batch "
                f"({_START}, {_END}, {_RUN})"
            )
    return identity


def resolve_run_identity(
    run_identity: tuple[str, str, str] | None,
    first_chunk: list[dict[str, Any]] | None,
) -> tuple[str, str, str]:
    """Prefer caller-threaded identity; fall back to the first chunk when present."""
    if run_identity is not None:
        start, end, run = (str(run_identity[0]), str(run_identity[1]), str(run_identity[2]))
        if not start.strip() or not end.strip() or not run.strip():
            raise ValueError("run_identity parts must be non-empty")
        return start, end, run
    if first_chunk is None:
        raise ValueError(
            "empty bronze write requires run_identity "
            f"({_START}, {_END}, {_RUN}) from the runner"
        )
    return require_bronze_run_identity(first_chunk)


def assert_chunk_matches_identity(
    chunk: list[dict[str, Any]],
    expected: tuple[str, str, str],
) -> None:
    got = require_bronze_run_identity(chunk)
    if got != expected:
        raise ValueError(
            "bronze run identity does not match the batch "
            f"({got!r} vs {expected!r})"
        )


def delete_extract_run_sql(qualified: str, *, placeholder: str) -> str:
    """DELETE rows for one extract run. Params are ISO strings; engines coerce TIMESTAMP."""
    return (
        f"delete from {qualified} "
        f"where {_START} = {placeholder} "
        f"and {_END} = {placeholder} "
        f"and {_RUN} = {placeholder}"
    )
