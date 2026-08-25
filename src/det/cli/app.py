from __future__ import annotations

import sys

# Emit before any heavy imports so a hang during import is visible under `uv run`.
print("det: importing…", file=sys.stderr, flush=True)

import typer  # noqa: E402

from det.logging import configure_logging, get_logger  # noqa: E402

app = typer.Typer(
    name="det",
    help="DET — Data Extract Tool",
    no_args_is_help=True,
)
logger = get_logger(__name__)


@app.callback()
def main(
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
    log_format: str | None = typer.Option(
        None,
        "--log-format",
        help="json | console (default: json when stderr is not a TTY)",
    ),
) -> None:
    try:
        configure_logging(log_level, log_format=log_format)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--log-format") from exc
    logger.info("det starting", log_level=log_level)
    print("det: loading plugins…", file=sys.stderr, flush=True)
    from det.plugins import load_plugins

    load_plugins()
    logger.info("plugins loaded")
