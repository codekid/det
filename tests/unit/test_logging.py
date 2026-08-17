from __future__ import annotations

import io
import json

import pytest
import structlog

from det.logging import (
    bound_run_context,
    configure_logging,
    get_logger,
    resolve_log_format,
    sanitize_lake_uri,
    update_run_context,
)


@pytest.fixture(autouse=True)
def _reset_structlog():
    yield
    structlog.contextvars.clear_contextvars()
    structlog.reset_defaults()
    configure_logging("WARNING", log_format="console", isatty=True)


def _last_json(buf: io.StringIO) -> dict:
    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
    assert lines, "expected at least one log line"
    return json.loads(lines[-1])


def test_resolve_log_format_tty_console():
    assert resolve_log_format(isatty=True, env={}) == "console"


def test_resolve_log_format_nontty_json():
    assert resolve_log_format(isatty=False, env={}) == "json"


def test_env_overrides_tty():
    assert resolve_log_format(isatty=True, env={"DET_LOG_FORMAT": "json"}) == "json"


def test_cli_overrides_env():
    assert (
        resolve_log_format("console", env={"DET_LOG_FORMAT": "json"}, isatty=False)
        == "console"
    )


def test_invalid_log_format():
    with pytest.raises(ValueError, match="json or console"):
        resolve_log_format("xml", env={})


def test_non_tty_emits_json():
    buf = io.StringIO()
    configure_logging("INFO", isatty=False, stream=buf)
    get_logger("t").info("hello")
    obj = _last_json(buf)
    assert obj["event"] == "hello"
    assert obj["level"] == "info"


def test_explicit_console_not_json():
    buf = io.StringIO()
    configure_logging("INFO", log_format="console", stream=buf)
    get_logger("t").info("hello")
    text = buf.getvalue()
    with pytest.raises(json.JSONDecodeError):
        json.loads(text.strip().splitlines()[-1])
    assert "hello" in text


def test_bound_keys_on_nested_logger():
    buf = io.StringIO()
    configure_logging("INFO", log_format="json", stream=buf)
    with bound_run_context(
        command="extract",
        pipeline="noaa.storm_events",
        interval_start="2026-08-01T00:00:00+00:00",
        interval_end="2026-08-02T00:00:00+00:00",
        extract_run_datetime="2026-08-01T12:00:00+00:00",
        destination="filesystem",
        lake="./data/lake",
    ):
        get_logger("det.sources.noaa").info("Downloading NOAA file")
    obj = _last_json(buf)
    assert obj["event"] == "Downloading NOAA file"
    assert obj["pipeline"] == "noaa.storm_events"
    assert obj["command"] == "extract"
    assert obj["destination"] == "filesystem"
    assert obj["extract_run_datetime"] == "2026-08-01T12:00:00+00:00"
    assert obj["lake"] == "./data/lake"


def test_unbind_after_manager():
    buf = io.StringIO()
    configure_logging("INFO", log_format="json", stream=buf)
    with bound_run_context(command="load", pipeline="example_api.events"):
        get_logger("t").info("inside")
    get_logger("t").info("after")
    obj = _last_json(buf)
    assert obj["event"] == "after"
    assert "pipeline" not in obj
    assert "command" not in obj


def test_update_run_context_adds_extract_run():
    buf = io.StringIO()
    configure_logging("INFO", log_format="json", stream=buf)
    with bound_run_context(command="load", pipeline="example_api.events"):
        update_run_context(extract_run_datetime="2026-08-01T12:00:00+00:00")
        get_logger("t").info("resolved")
    obj = _last_json(buf)
    assert obj["extract_run_datetime"] == "2026-08-01T12:00:00+00:00"
    assert obj["pipeline"] == "example_api.events"


def test_connection_dsn_never_logged():
    buf = io.StringIO()
    configure_logging("INFO", log_format="json", stream=buf)
    get_logger("t").info(
        "writing",
        destination="postgres",
        connection="postgresql://user:secret@localhost/db",
        password="hunter2",
    )
    text = buf.getvalue()
    assert "secret" not in text
    assert "hunter2" not in text
    assert "postgresql://" not in text
    obj = _last_json(buf)
    assert "connection" not in obj
    assert obj["destination"] == "postgres"


def test_sanitize_lake_uri_strips_userinfo():
    assert sanitize_lake_uri("s3://bucket/lake") == "s3://bucket/lake"
    assert sanitize_lake_uri("s3://AKIA:secret@bucket/lake") == "s3://bucket/lake"
    assert sanitize_lake_uri("./data/lake") == "./data/lake"
    assert "secret" not in sanitize_lake_uri("gs://key:secret@bucket/prefix")
