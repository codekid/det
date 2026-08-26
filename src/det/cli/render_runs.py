from __future__ import annotations

import sys
from datetime import datetime

import typer


def _format_duration(value: object) -> str:
    try:
        milliseconds = max(0, int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    if milliseconds < 1000:
        return f"{milliseconds}ms"
    seconds = milliseconds / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(milliseconds // 1000, 60)
    return f"{minutes}m {remainder:02d}s"


def _format_started(value: object) -> str:
    text = str(value or "")
    if not text:
        return "-"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone().strftime("%b %d %H:%M:%S")
    except ValueError:
        return text[:19]


def _format_window_date(value: object) -> str:
    text = str(value or "")
    try:
        return datetime.fromisoformat(text).strftime("%b %d, %Y")
    except ValueError:
        return text or "-"


def _truncate(value: object, width: int) -> str:
    text = str(value or "-")
    if len(text) <= width:
        return text
    return text[: max(1, width - 1)] + "…"


def _table_widths(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> tuple[int, ...]:
    return tuple(
        max(len(header), *(len(row[index]) for row in rows)) for index, header in enumerate(headers)
    )


def _table_row(
    values: tuple[str, ...],
    widths: tuple[int, ...],
    *,
    status_color: bool = False,
) -> str:
    cells = [value.ljust(widths[index]) for index, value in enumerate(values)]
    if status_color and sys.stdout.isatty():
        color = typer.colors.GREEN if values[0] == "OK" else typer.colors.RED
        cells[0] = typer.style(cells[0], fg=color, bold=True)
    return "  ".join(cells).rstrip()


def _print_run_list(
    receipts: list[dict[str, object]],
    *,
    include_pipeline: bool,
    verbose: bool,
) -> None:
    headers = ("STATUS", "COMMAND")
    if include_pipeline:
        headers += ("PIPELINE",)
    headers += ("DURATION", "STARTED")

    rows: list[tuple[str, ...]] = []
    for receipt in receipts:
        values = (
            str(receipt.get("status") or "-").upper(),
            str(receipt.get("command") or "-"),
        )
        if include_pipeline:
            values += (_truncate(receipt.get("pipeline"), 30),)
        values += (
            _format_duration(receipt.get("duration_ms")),
            _format_started(receipt.get("started_at")),
        )
        rows.append(values)

    widths = _table_widths(headers, rows)
    typer.echo(_table_row(headers, widths))
    for receipt, row in zip(receipts, rows, strict=True):
        typer.echo(_table_row(row, widths, status_color=True))
        if receipt.get("status") == "error":
            code = str(receipt.get("error_code") or "unknown")
            message = str(receipt.get("error_message") or "")
            detail = f"{code}: {message}" if message else code
            typer.echo(f"        └─ {_truncate(detail, 110)}")
        if verbose:
            typer.echo(
                "        "
                f"Owner: {receipt.get('owner') or '-'}  "
                f"Destination: {receipt.get('destination') or '-'}"
            )
            typer.echo(
                "        "
                f"Interval: {receipt.get('interval_start') or '-'} → "
                f"{receipt.get('interval_end') or '-'}"
            )
            typer.echo(
                "        "
                f"Extract: {receipt.get('extract_run_datetime') or '-'}  "
                f"Attempt ID: {receipt.get('attempt_id') or '-'}"
            )
            if receipt.get("error_class"):
                typer.echo(f"        Error class: {receipt['error_class']}")


def _print_run_summary(
    payload: dict[str, object],
    *,
    include_pipeline: bool,
) -> None:
    typer.echo(
        f"Attempt window: {_format_window_date(payload.get('since'))} – "
        f"{_format_window_date(payload.get('until'))} (end exclusive)"
    )
    typer.echo()
    headers = ("COMMAND",)
    if include_pipeline:
        headers = ("PIPELINE",) + headers
    headers += ("ATTEMPTS", "OK", "ERRORS", "P50", "P95", "ROWS", "ERROR CODES")

    rows: list[tuple[str, ...]] = []
    groups = payload.get("groups")
    if not isinstance(groups, list):
        raise TypeError("summarize_runs payload.groups must be a list")
    for group in groups:
        if not isinstance(group, dict):
            raise TypeError("summarize_runs group must be a dict")
        values = (str(group.get("command") or "-"),)
        if include_pipeline:
            values = (_truncate(group.get("pipeline"), 30),) + values
        error_codes = group.get("error_codes")
        codes = ""
        if isinstance(error_codes, dict):
            codes = ", ".join(f"{name}×{count}" for name, count in sorted(error_codes.items()))
        rows_value = group.get("rows")
        shown_rows = "-" if group.get("command") == "extract" else f"{int(rows_value or 0):,}"
        values += (
            f"{int(group.get('attempts') or 0):,}",
            f"{int(group.get('ok') or 0):,}",
            f"{int(group.get('error') or 0):,}",
            _format_duration(group.get("p50_ms")),
            _format_duration(group.get("p95_ms")),
            shown_rows,
            codes or "-",
        )
        rows.append(values)

    widths = _table_widths(headers, rows)
    typer.echo(_table_row(headers, widths))
    for row in rows:
        typer.echo(_table_row(row, widths))

