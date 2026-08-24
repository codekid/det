"""
Secret resolution for DET: config carries names, the environment carries values.

One parser serves every store. A payload is either a bare string (the credential
itself) or a JSON object of credential keys. Callers request the keys they need,
so a secret can never smuggle configuration (base_url, host) into a run.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, urlsplit

from det.logging import get_logger, register_secret_value

logger = get_logger(__name__)

SecretsBackend = Literal["env", "file"]
_BACKENDS = ("env", "file")

DEFAULT_CACHE_TTL_SEC = 300
DEFAULT_SECRETS_FILENAME = ".env.secrets"

# Credential keys a caller may request. Rotation tooling adds host/port/dbname;
# those are ignored so config can never arrive through the secret store.
CREDENTIAL_KEYS: frozenset[str] = frozenset(
    {
        "value",
        "token",
        "api_key",
        "dsn",
        "client_id",
        "client_secret",
        "username",
        "password",
    }
)

# Preference order per caller kind.
HTTP_TOKEN_KEYS = ("token", "api_key", "value")
DSN_KEYS = ("dsn", "value")

_SECRET_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_DOTENV_LINE_RE = re.compile(r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>.*)$")
_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")

# name -> (monotonic read time, payload, "env" | "file")
_CACHE: dict[str, tuple[float, dict[str, str], str]] = {}
_FILE_CACHE: dict[str, tuple[float, dict[str, str]]] = {}
_WARNED_EXTRAS: set[str] = set()


class SecretError(RuntimeError):
    """Base class for secret resolution failures."""


class SecretNotSetError(SecretError):
    """No configured store supplied a value for the requested name."""


class SecretPayloadError(SecretError):
    """A store supplied a payload DET cannot use."""


def clear_secret_cache() -> None:
    """Drop cached payloads (tests, and after a forced rotation)."""
    _CACHE.clear()
    _FILE_CACHE.clear()
    _WARNED_EXTRAS.clear()


def invalidate_secret(name: str) -> None:
    """Forget one cached payload so the next read re-fetches it."""
    _CACHE.pop(name, None)
    _FILE_CACHE.clear()


def cache_ttl_sec(env: Mapping[str, str] | None = None) -> int:
    """Seconds a resolved payload stays cached so long backfills see rotations."""
    environ = os.environ if env is None else env
    raw = (environ.get("DET_SECRETS_TTL_SEC") or "").strip()
    if not raw:
        return DEFAULT_CACHE_TTL_SEC
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"DET_SECRETS_TTL_SEC must be a whole number of seconds, got {raw!r}"
        ) from exc
    if value < 0:
        raise ValueError(f"DET_SECRETS_TTL_SEC must be >= 0, got {value}")
    return value


def resolve_secrets_backend(env: Mapping[str, str] | None = None) -> SecretsBackend:
    """``DET_SECRETS_BACKEND``; defaults to env-only. Fails closed on a typo."""
    environ = os.environ if env is None else env
    raw = (environ.get("DET_SECRETS_BACKEND") or "env").strip().lower()
    if raw not in _BACKENDS:
        raise ValueError(
            f"DET_SECRETS_BACKEND must be one of {', '.join(_BACKENDS)}, got {raw!r}"
        )
    return raw  # type: ignore[return-value]


def looks_like_secret_name(text: str | None) -> bool:
    """True for an env-style identifier (``DET_POSTGRES_DSN``), not a literal."""
    return bool(_SECRET_NAME_RE.match((text or "").strip()))


def looks_like_uri(text: str | None) -> bool:
    return "://" in (text or "").strip()


def uri_has_userinfo(text: str | None) -> bool:
    """True when a URI embeds user or password (``s3://AKIA:secret@bucket``)."""
    value = (text or "").strip()
    if "://" not in value:
        return False
    try:
        parts = urlsplit(value)
    except ValueError:
        return False
    return bool(parts.username or parts.password)


def looks_like_passwordful_uri(text: str | None) -> bool:
    """True when a URI carries a password in userinfo or a ``password=`` query."""
    value = (text or "").strip()
    if "://" not in value:
        return False
    try:
        parts = urlsplit(value)
    except ValueError:
        return False
    if parts.password:
        return True
    return any(
        key.lower() == "password" and val for key, val in parse_qsl(parts.query)
    )


def secret_name_candidates(provider: str) -> tuple[str, str]:
    """``example_api`` -> ``DET_EXAMPLE_API`` then ``EXAMPLE_API``."""
    slug = _NON_ALNUM_RE.sub("_", (provider or "").strip()).strip("_").upper()
    if not slug:
        raise ValueError(f"cannot derive a secret name from provider {provider!r}")
    return (f"DET_{slug}", slug)


def source_secret_names(provider: str, auth_env: str | None = None) -> tuple[str, ...]:
    """
    Names to try for a source credential, in order.

    An explicit ``auth_env`` wins (keeps ``EXAMPLE_API_TOKEN`` working); the
    provider-derived names follow so one secret can serve every dataset.
    """
    names: list[str] = []
    explicit = (auth_env or "").strip()
    if explicit:
        names.append(explicit)
    names.extend(secret_name_candidates(provider))
    return tuple(dict.fromkeys(names))


def parse_secret_payload(raw: str, *, name: str = "secret") -> dict[str, str]:
    """
    Normalize a stored payload to a credential mapping.

    A JSON object keeps its allowlisted keys; anything else is a bare credential
    string exposed as ``value``. Unknown keys are ignored with one warning.
    """
    text = (raw or "").strip()
    if not text:
        raise SecretPayloadError(f"{name}: secret is empty")
    if text.startswith("["):
        raise SecretPayloadError(f"{name}: secret must be a string or JSON object")
    if not text.startswith("{"):
        return {"value": text}

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SecretPayloadError(f"{name}: secret looks like JSON but did not parse") from exc
    if not isinstance(data, dict):
        raise SecretPayloadError(f"{name}: secret must be a string or JSON object")

    payload: dict[str, str] = {}
    extras: list[str] = []
    for key, value in data.items():
        if key in CREDENTIAL_KEYS:
            if value is not None:
                payload[key] = str(value)
        else:
            extras.append(str(key))
    if extras and name not in _WARNED_EXTRAS:
        _WARNED_EXTRAS.add(name)
        logger.warning(
            "secret payload has non-credential keys (ignored)",
            secret_name=name,
            ignored_keys=sorted(extras),
        )
    return payload


def resolve_secret(
    names: str | Sequence[str],
    *,
    keys: Sequence[str],
    env: Mapping[str, str] | None = None,
    project_root: Path | None = None,
    backend: SecretsBackend | None = None,
) -> str:
    """
    First value found for *names*, reading the first present key in *keys*.

    Env always wins; the optional file backend is a fallback. Pass
    ``backend="env"`` to pin process env (MCP inspect never touches a store).
    When a ``DetSettings`` is active (``use_settings``), raw values come from
    ``settings.resolve_secret`` (callable hook) instead of the process cache.
    Raises ``SecretNotSetError`` when nothing supplies the secret: a declared
    credential that cannot be resolved must fail the run, never downgrade
    to an unauthenticated request.
    """
    candidates = [names] if isinstance(names, str) else list(names)
    candidates = [str(n).strip() for n in candidates if str(n or "").strip()]
    if not candidates:
        raise ValueError("resolve_secret requires at least one name")
    if not keys:
        raise ValueError("resolve_secret requires at least one key")

    from det.runtime.settings import get_active_settings

    settings = get_active_settings()
    if settings is not None and backend is None:
        return _resolve_via_settings(candidates, keys=keys, settings=settings)

    chosen = backend or resolve_secrets_backend(env)
    for name in candidates:
        payload = _payload_for(name, backend=chosen, env=env, project_root=project_root)
        if payload is None:
            continue
        for key in keys:
            value = payload.get(key)
            if value is not None and value.strip():
                return value
        raise SecretPayloadError(
            f"{name}: no usable credential key (wanted {', '.join(keys)}; "
            f"found {', '.join(sorted(payload)) or 'none'}). "
            "DET does not assemble a DSN from parts."
        )

    raise SecretNotSetError(
        f"secret is not set: tried {', '.join(candidates)} "
        f"(DET_SECRETS_BACKEND={chosen})"
    )


def _resolve_via_settings(
    candidates: list[str],
    *,
    keys: Sequence[str],
    settings: Any,
) -> str:
    for name in candidates:
        raw = settings.resolve_secret(name)
        if raw is None or not str(raw).strip():
            continue
        payload = parse_secret_payload(str(raw), name=name)
        for value in payload.values():
            register_secret_value(value)
        for key in keys:
            value = payload.get(key)
            if value is not None and value.strip():
                return value
        raise SecretPayloadError(
            f"{name}: no usable credential key (wanted {', '.join(keys)}; "
            f"found {', '.join(sorted(payload)) or 'none'}). "
            "DET does not assemble a DSN from parts."
        )

    raise SecretNotSetError(
        f"secret is not set: tried {', '.join(candidates)} "
        f"(DET_SECRETS_BACKEND={settings.secrets_backend})"
    )


def _payload_for(
    name: str,
    *,
    backend: SecretsBackend,
    env: Mapping[str, str] | None,
    project_root: Path | None,
) -> dict[str, str] | None:
    ttl = cache_ttl_sec(env)
    now = time.monotonic()
    cached = _CACHE.get(name)
    if cached is not None and now - cached[0] < ttl:
        # A caller pinned to env must not be served a value the file backend cached.
        if backend == "file" or cached[2] == "env":
            return cached[1]

    environ = os.environ if env is None else env
    raw = environ.get(name)
    source = "env"
    if raw is None or not str(raw).strip():
        if backend != "file":
            return None
        raw = _read_secrets_file(env=env, project_root=project_root).get(name)
        source = "file"
        if raw is None or not str(raw).strip():
            return None

    payload = parse_secret_payload(str(raw), name=name)
    for value in payload.values():
        register_secret_value(value)
    _CACHE[name] = (now, payload, source)
    logger.debug("secret resolved", secret_name=name, backend=source)
    return payload


def secrets_file_path(
    env: Mapping[str, str] | None = None,
    project_root: Path | None = None,
) -> Path:
    """``DET_SECRETS_FILE`` (absolute or project-relative), else ``.env.secrets``."""
    environ = os.environ if env is None else env
    root = (project_root or Path.cwd()).resolve()
    raw = (environ.get("DET_SECRETS_FILE") or "").strip()
    path = Path(raw) if raw else root / DEFAULT_SECRETS_FILENAME
    if not path.is_absolute():
        path = root / path
    return path


def _read_secrets_file(
    *,
    env: Mapping[str, str] | None,
    project_root: Path | None,
) -> dict[str, str]:
    path = secrets_file_path(env=env, project_root=project_root)
    key = str(path)
    ttl = cache_ttl_sec(env)
    now = time.monotonic()
    cached = _FILE_CACHE.get(key)
    if cached is not None and now - cached[0] < ttl:
        return cached[1]

    if not path.is_file():
        raise SecretError(
            f"DET_SECRETS_BACKEND=file but no secrets file at {path}. "
            "Set DET_SECRETS_FILE or create the file (keep it gitignored)."
        )
    if _is_gitignored(path) is False:
        raise SecretError(
            f"refusing to read {path}: it is inside a git repo and not gitignored. "
            "Add it to .gitignore or move it outside the repo."
        )
    mode = path.stat().st_mode
    if mode & 0o077:
        logger.warning(
            "secrets file is group/world readable",
            path=str(path),
            mode=oct(mode & 0o777),
        )

    values = _parse_dotenv(path.read_text(encoding="utf-8"), path=path)
    _FILE_CACHE[key] = (now, values)
    return values


def _is_gitignored(path: Path) -> bool | None:
    """
    True when git ignores the path, False when it does not.

    None when the answer is unknown (no repo, git missing) — the guard only
    protects against committing, which only matters inside a work tree.
    """
    try:
        completed = subprocess.run(
            ["git", "check-ignore", "-q", str(path)],
            cwd=str(path.parent),
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    return None


def _parse_dotenv(text: str, *, path: Path) -> dict[str, str]:
    """
    Strict subset: ``NAME=value``, ``#`` comments, blank lines.

    No ``export``, no multiline, no interpolation — a secrets file that needs
    shell semantics belongs in the environment instead.
    """
    values: dict[str, str] = {}
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            raise SecretError(f"{path}:{lineno}: 'export' is not supported (use NAME=value)")
        match = _DOTENV_LINE_RE.match(stripped)
        if match is None:
            raise SecretError(f"{path}:{lineno}: expected NAME=value")
        value = match.group("value").strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[match.group("name")] = value
    return values
