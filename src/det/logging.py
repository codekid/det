from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Literal, TextIO
from urllib.parse import urlsplit, urlunsplit

import structlog

LogFormat = Literal["json", "console"]

_RUN_CONTEXT_KEYS = (
    "pipeline",
    "interval_start",
    "interval_end",
    "extract_run_datetime",
    "destination",
    "command",
    "lake",
)

_SECRET_KEYS = frozenset(
    {
        "connection",
        "password",
        "dsn",
        "secret",
        "token",
        "api_key",
        "apikey",
    }
)

# Resolved secret values seeded by det.runtime.secrets. Key-based scrubbing cannot
# reach a DSN quoted inside exception text, so the renderer output is scrubbed too.
_REDACT_VALUES: set[str] = set()
_REDACTED = "***"
# Short values collide with ordinary words; scrubbing them would corrupt logs.
_MIN_REDACT_LEN = 8

_URI_USERINFO_RE = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)[^/\s@\"']+@")
_URI_SECRET_QUERY_RE = re.compile(
    r"(?i)([?&](?:password|token|api_key|secret)=)[^&\s\"']+"
)


def resolve_log_format(
    explicit: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
    isatty: bool | None = None,
) -> LogFormat:
    """CLI ``--log-format`` > ``DET_LOG_FORMAT`` > JSON when stderr is not a TTY."""
    if explicit is not None and str(explicit).strip():
        return _normalize_log_format(str(explicit), source="--log-format")
    environ = os.environ if env is None else env
    raw = environ.get("DET_LOG_FORMAT")
    if raw is not None and str(raw).strip():
        return _normalize_log_format(str(raw), source="DET_LOG_FORMAT")
    tty = sys.stderr.isatty() if isatty is None else isatty
    return "console" if tty else "json"


def _normalize_log_format(raw: str, *, source: str) -> LogFormat:
    value = raw.strip().lower()
    if value not in {"json", "console"}:
        raise ValueError(f"{source} must be json or console, got {raw!r}")
    return value  # type: ignore[return-value]


def sanitize_lake_uri(spec: str) -> str:
    """Drop userinfo from a lake URI so logs never carry object-store secrets."""
    text = (spec or "").strip()
    if "://" not in text:
        return text
    parts = urlsplit(text)
    if not parts.username and not parts.password:
        return text
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


def redact_uri_credentials(text: str) -> str:
    """
    Mask userinfo and credential query params anywhere inside *text*.

    Unlike ``sanitize_lake_uri`` this works on compound strings such as
    ``destination.connection=postgresql://user:pass@host/db,foo=bar``.
    """
    if not text or "://" not in text:
        return text
    masked = _URI_USERINFO_RE.sub(lambda m: f"{m.group('scheme')}{_REDACTED}@", text)
    return _URI_SECRET_QUERY_RE.sub(rf"\1{_REDACTED}", masked)


def drop_secrets(
    _logger: Any, _method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Strip DSN / credential keys before they reach a renderer."""
    for key in list(event_dict):
        lowered = key.lower()
        if lowered in _SECRET_KEYS or lowered.endswith(
            ("_password", "_secret", "_token", "_dsn", "_connection")
        ):
            event_dict.pop(key, None)
    return event_dict


def register_secret_value(value: str | None) -> None:
    """Remember a resolved secret so rendered lines never echo it."""
    text = (value or "").strip()
    if len(text) < _MIN_REDACT_LEN:
        return
    _REDACT_VALUES.add(text)
    # A DSN usually fails inside a driver message that quotes only the password.
    if "://" in text:
        try:
            password = urlsplit(text).password
        except ValueError:
            password = None
        if password and len(password) >= _MIN_REDACT_LEN:
            _REDACT_VALUES.add(password)


def clear_secret_values() -> None:
    _REDACT_VALUES.clear()


def scrub_secrets(text: str) -> str:
    """Replace registered secret values, then mask URI userinfo / credential params."""
    if not text:
        return text
    out = text
    for secret in _REDACT_VALUES:
        if secret in out:
            out = out.replace(secret, _REDACTED)
    return redact_uri_credentials(out)


def scrub_rendered(_logger: Any, _method: str, event: Any) -> Any:
    """
    Final processor: scrub secrets from the rendered line.

    Key-based ``drop_secrets`` cannot reach a DSN quoted inside exception text,
    which ``dict_tracebacks`` renders verbatim.
    """
    if not isinstance(event, str):
        return event
    return scrub_secrets(event)


def configure_logging(
    level: str = "INFO",
    *,
    log_format: str | None = None,
    env: Mapping[str, str] | None = None,
    isatty: bool | None = None,
    stream: TextIO | None = None,
) -> None:
    """Configure structlog + stdlib logging once for CLI/runtime."""
    # Line-buffer stderr so progress shows up under `uv run` without waiting for exit.
    out = stream if stream is not None else sys.stderr
    if stream is None:
        try:
            sys.stderr.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
        except Exception:
            pass
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        stream=out,
        level=log_level,
        force=True,
    )
    chosen = resolve_log_format(log_format, env=env, isatty=isatty)
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        drop_secrets,
    ]
    if chosen == "json":
        processors.extend(
            [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ]
        )
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    processors.append(scrub_rendered)
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=out),
        # False so tests (CliRunner) and stderr swaps do not pin a closed stream.
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None):
    return structlog.get_logger(name)


def update_run_context(**kwargs: str | None) -> None:
    """Add or replace bound identity keys (e.g. extract_run after reading the manifest)."""
    payload = {
        key: value
        for key, value in kwargs.items()
        if key in _RUN_CONTEXT_KEYS and value is not None and value != ""
    }
    if payload:
        structlog.contextvars.bind_contextvars(**payload)


@contextmanager
def bound_run_context(**kwargs: str | None) -> Iterator[None]:
    """Bind run identity for nested loggers; always unbind on exit."""
    payload = {
        key: value
        for key, value in kwargs.items()
        if key in _RUN_CONTEXT_KEYS and value is not None and value != ""
    }
    if payload:
        structlog.contextvars.bind_contextvars(**payload)
    try:
        yield
    finally:
        structlog.contextvars.unbind_contextvars(*_RUN_CONTEXT_KEYS)
