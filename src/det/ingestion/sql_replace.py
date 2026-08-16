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


def delete_extract_run_sql(qualified: str, *, placeholder: str) -> str:
    """DELETE rows for one extract run. Params are ISO strings; engines coerce TIMESTAMP."""
    return (
        f"delete from {qualified} "
        f"where {_START} = {placeholder} "
        f"and {_END} = {placeholder} "
        f"and {_RUN} = {placeholder}"
    )
