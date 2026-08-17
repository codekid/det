"""Secret parsing and resolution: names in config, values from env or a file."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from det.runtime.secrets import (
    DSN_KEYS,
    HTTP_TOKEN_KEYS,
    SecretError,
    SecretNotSetError,
    SecretPayloadError,
    clear_secret_cache,
    invalidate_secret,
    looks_like_passwordful_uri,
    looks_like_secret_name,
    parse_secret_payload,
    resolve_secret,
    resolve_secrets_backend,
    secret_name_candidates,
    source_secret_names,
    uri_has_userinfo,
)


def test_bare_string_payload_becomes_value():
    assert parse_secret_payload("tok-abc123") == {"value": "tok-abc123"}


def test_json_object_payload_keeps_credential_keys():
    raw = json.dumps({"token": "tok-abc123"})
    assert parse_secret_payload(raw) == {"token": "tok-abc123"}


def test_dsn_payload_is_read_by_dsn_key(monkeypatch):
    monkeypatch.setenv("DET_POSTGRES_DSN", json.dumps({"dsn": "postgresql://h/db"}))
    assert resolve_secret("DET_POSTGRES_DSN", keys=DSN_KEYS) == "postgresql://h/db"


def test_non_credential_keys_are_ignored_with_one_warning():
    raw = json.dumps({"token": "tok-abc123", "base_url": "https://evil.example"})
    with capture_logs() as logs:
        payload = parse_secret_payload(raw, name="EXAMPLE_API")
        parse_secret_payload(raw, name="EXAMPLE_API")
    assert payload == {"token": "tok-abc123"}
    warnings = [e for e in logs if e["log_level"] == "warning"]
    assert len(warnings) == 1
    assert warnings[0]["ignored_keys"] == ["base_url"]
    assert "evil.example" not in json.dumps(warnings[0])


def test_json_array_payload_is_rejected():
    with pytest.raises(SecretPayloadError, match="string or JSON object"):
        parse_secret_payload('["tok"]')


def test_broken_json_object_is_rejected():
    with pytest.raises(SecretPayloadError, match="did not parse"):
        parse_secret_payload('{"token": ')


def test_numeric_token_stays_a_string_value():
    assert parse_secret_payload("1234567890") == {"value": "1234567890"}


def test_rds_style_parts_cannot_be_assembled(monkeypatch):
    monkeypatch.setenv(
        "DET_POSTGRES_DSN",
        json.dumps({"username": "det", "password": "pw", "host": "db", "port": 5432}),
    )
    with pytest.raises(SecretPayloadError, match="does not assemble a DSN"):
        resolve_secret("DET_POSTGRES_DSN", keys=DSN_KEYS)


def test_secret_name_candidates_prefers_det_prefix():
    assert secret_name_candidates("example_api") == ("DET_EXAMPLE_API", "EXAMPLE_API")


def test_source_secret_names_puts_explicit_auth_env_first():
    assert source_secret_names("example_api", "EXAMPLE_API_TOKEN") == (
        "EXAMPLE_API_TOKEN",
        "DET_EXAMPLE_API",
        "EXAMPLE_API",
    )


def test_det_prefixed_name_wins_over_bare_name(monkeypatch):
    monkeypatch.setenv("DET_EXAMPLE_API", "prefixed")
    monkeypatch.setenv("EXAMPLE_API", "bare")
    names = secret_name_candidates("example_api")
    assert resolve_secret(names, keys=HTTP_TOKEN_KEYS) == "prefixed"


def test_missing_secret_raises_and_names_what_it_tried(monkeypatch):
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    monkeypatch.delenv("EXAMPLE_API", raising=False)
    with pytest.raises(SecretNotSetError, match="DET_EXAMPLE_API, EXAMPLE_API"):
        resolve_secret(secret_name_candidates("example_api"), keys=HTTP_TOKEN_KEYS)


def test_cache_holds_a_value_until_invalidated(monkeypatch):
    monkeypatch.setenv("DET_EXAMPLE_API", "first-token")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "first-token"
    monkeypatch.setenv("DET_EXAMPLE_API", "rotated-token")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "first-token"
    invalidate_secret("DET_EXAMPLE_API")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "rotated-token"


def test_zero_ttl_re_reads_every_time(monkeypatch):
    monkeypatch.setenv("DET_SECRETS_TTL_SEC", "0")
    monkeypatch.setenv("DET_EXAMPLE_API", "first-token")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "first-token"
    monkeypatch.setenv("DET_EXAMPLE_API", "rotated-token")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "rotated-token"


def test_clear_secret_cache_isolates(monkeypatch):
    monkeypatch.setenv("DET_EXAMPLE_API", "first-token")
    resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS)
    clear_secret_cache()
    monkeypatch.setenv("DET_EXAMPLE_API", "rotated-token")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "rotated-token"


def test_invalid_backend_names_the_allowed_values(monkeypatch):
    monkeypatch.setenv("DET_SECRETS_BACKEND", "vault")
    with pytest.raises(ValueError, match="env, file"):
        resolve_secrets_backend()


def _write_secrets_file(tmp_path: Path, body: str) -> Path:
    path = tmp_path / ".env.secrets"
    path.write_text(body, encoding="utf-8")
    path.chmod(0o600)
    return path


def test_file_backend_supplies_a_missing_name(monkeypatch, tmp_path: Path):
    path = _write_secrets_file(tmp_path, "# creds\nDET_EXAMPLE_API=file-token\n")
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "file-token"


def test_env_wins_over_file(monkeypatch, tmp_path: Path):
    path = _write_secrets_file(tmp_path, "DET_EXAMPLE_API=file-token\n")
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.setenv("DET_EXAMPLE_API", "env-token")
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "env-token"


def test_file_backend_rejects_export_lines(monkeypatch, tmp_path: Path):
    path = _write_secrets_file(tmp_path, "export DET_EXAMPLE_API=file-token\n")
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    with pytest.raises(SecretError, match="export"):
        resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS)


def test_missing_secrets_file_is_an_error(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(tmp_path / "nope.env"))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    with pytest.raises(SecretError, match="no secrets file"):
        resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS)


def test_committed_secrets_file_is_refused(monkeypatch, tmp_path: Path):
    if not os.environ.get("PATH"):
        pytest.skip("no PATH for git")
    repo = tmp_path / "repo"
    repo.mkdir()
    try:
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        pytest.skip("git unavailable")
    path = repo / "creds.env"
    path.write_text("DET_EXAMPLE_API=file-token\n", encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    with pytest.raises(SecretError, match="not gitignored"):
        resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS)


def test_gitignored_secrets_file_is_read(monkeypatch, tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    try:
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        pytest.skip("git unavailable")
    (repo / ".gitignore").write_text("creds.env\n", encoding="utf-8")
    path = repo / "creds.env"
    path.write_text("DET_EXAMPLE_API=file-token\n", encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    assert resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS) == "file-token"


def test_file_backend_is_not_consulted_by_default(monkeypatch, tmp_path: Path):
    path = _write_secrets_file(tmp_path, "DET_EXAMPLE_API=file-token\n")
    monkeypatch.delenv("DET_SECRETS_BACKEND", raising=False)
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    with pytest.raises(SecretNotSetError):
        resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS)


def test_env_only_backend_ignores_the_file(monkeypatch, tmp_path: Path):
    path = _write_secrets_file(tmp_path, "DET_EXAMPLE_API=file-token\n")
    monkeypatch.setenv("DET_SECRETS_BACKEND", "file")
    monkeypatch.setenv("DET_SECRETS_FILE", str(path))
    monkeypatch.delenv("DET_EXAMPLE_API", raising=False)
    with pytest.raises(SecretNotSetError):
        resolve_secret("DET_EXAMPLE_API", keys=HTTP_TOKEN_KEYS, backend="env")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("postgresql://user:pw@host/db", True),
        ("postgresql://user@host/db", False),
        ("postgresql://host/db?password=pw", True),
        ("./data/analytics.duckdb", False),
        ("DET_POSTGRES_DSN", False),
    ],
)
def test_passwordful_uri_detection(text, expected):
    assert looks_like_passwordful_uri(text) is expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("s3://AKIA:secret@bucket/lake", True),
        ("s3://bucket/lake", False),
        ("./data/lake", False),
    ],
)
def test_userinfo_detection(text, expected):
    assert uri_has_userinfo(text) is expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("DET_POSTGRES_DSN", True),
        ("EXAMPLE_API", True),
        ("postgresql://host/db", False),
        ("./data/analytics.duckdb", False),
    ],
)
def test_secret_name_detection(text, expected):
    assert looks_like_secret_name(text) is expected
