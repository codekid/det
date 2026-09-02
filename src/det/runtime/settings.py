"""Frozen process settings for embedders and the CLI.

Env is how ``from_env()`` populates defaults — not the deep transport forever.
Object-store lake credentials stay AWS_/GCP env conventions (not on this object).
Approvals, Cube, Airflow, analytics DuckDB, and log format stay product/CLI-only.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from det.runtime.lake import LakeMode, lake_mode_from_env
from det.runtime.lease import (
    DEFAULT_LOCK_BACKEND,
    DEFAULT_LOCK_MODE,
    DEFAULT_LOCK_PG_DSN_ENV,
    DEFAULT_LOCK_PG_SCHEMA,
    DEFAULT_LOCK_PG_TABLE,
    DEFAULT_LOCK_TTL_SEC,
    default_lock_owner,
    locks_enabled,
    resolve_lock_ttl_sec,
)
from det.runtime.lease.resolve import parse_lock_backend, parse_lock_mode
from det.runtime.pipelines import resolve_project_root
from det.runtime.secrets import (
    DEFAULT_CACHE_TTL_SEC,
    SecretsBackend,
    cache_ttl_sec,
    resolve_secrets_backend,
    secrets_file_path,
)

# name → raw secret string (or None when unset). Key selection stays in resolve_secret.
SecretLookup = Callable[[str], str | None]

_ACTIVE: ContextVar[DetSettings | None] = ContextVar("det_settings", default=None)
_MISSING = object()


@dataclass
class _SecretCache:
    """Mutable per-settings cache (DetSettings itself stays frozen)."""

    entries: dict[str, tuple[float, str | None]] = field(default_factory=dict)

    def get(self, name: str, *, ttl_sec: int) -> tuple[bool, str | None]:
        if ttl_sec <= 0:
            return False, None
        hit = self.entries.get(name)
        if hit is None:
            return False, None
        stamped, value = hit
        if time.monotonic() - stamped >= ttl_sec:
            self.entries.pop(name, None)
            return False, None
        return True, value

    def put(self, name: str, value: str | None) -> None:
        self.entries[name] = (time.monotonic(), value)

    def clear(self) -> None:
        self.entries.clear()


def _env_file_lookup(
    *,
    project_root: Path,
    backend: SecretsBackend,
    secrets_file: Path | None,
    ttl_sec: int,
    env: Mapping[str, str] | None,
    cache: _SecretCache,
) -> SecretLookup:
    """Default name→raw lookup: process env, then optional dotenv file."""

    def lookup(name: str) -> str | None:
        ok, cached = cache.get(name, ttl_sec=ttl_sec)
        if ok:
            return cached

        environ = os.environ if env is None else env
        raw = environ.get(name)
        if raw is not None and str(raw).strip():
            text = str(raw)
            cache.put(name, text)
            return text

        if backend != "file":
            cache.put(name, None)
            return None

        # Lazy import keeps secrets._read_secrets_file private to that module.
        from det.runtime import secrets as secrets_mod

        file_env = dict(environ)
        if secrets_file is not None:
            file_env["DET_SECRETS_FILE"] = str(secrets_file)
        try:
            values = secrets_mod._read_secrets_file(
                env=file_env, project_root=project_root
            )
        except secrets_mod.SecretError:
            cache.put(name, None)
            raise
        text = values.get(name)
        if text is None or not str(text).strip():
            cache.put(name, None)
            return None
        out = str(text)
        cache.put(name, out)
        return out

    return lookup


def _caching_lookup(inner: SecretLookup, *, ttl_sec: int, cache: _SecretCache) -> SecretLookup:
    def lookup(name: str) -> str | None:
        ok, cached = cache.get(name, ttl_sec=ttl_sec)
        if ok:
            return cached
        value = inner(name)
        cache.put(name, value)
        return value

    return lookup


@dataclass(frozen=True)
class DetSettings:
    """Embedder/CLI settings. Prefer ``from_env`` then ``with_overrides`` for flags."""

    project_root: Path
    lake_path: str | None
    lake_mode: LakeMode
    resolve_secret: SecretLookup
    secrets_backend: SecretsBackend
    secrets_file: Path | None
    secrets_ttl_sec: int
    locks_enabled: bool
    lock_ttl_sec: int
    lock_owner: str
    lock_backend: str
    lock_mode: str
    lock_pg_dsn_env: str
    lock_pg_schema: str
    lock_pg_table: str
    # Layout 1 unified root. CLI ``--lake-path`` wins via lake_override.
    lake_override: str | None = None
    # Layout 2 split roots (opaque URIs; embedders choose bucket names).
    lake_path_raw: str | None = None
    lake_path_bronze: str | None = None
    lake_path_ops: str | None = None
    lake_override_raw: str | None = None
    lake_override_bronze: str | None = None
    lake_override_ops: str | None = None
    _secret_cache: _SecretCache = field(default_factory=_SecretCache, compare=False, repr=False)

    @classmethod
    def from_env(
        cls,
        project_root: Path | str | None = None,
        *,
        env: Mapping[str, str] | None = None,
        resolve_secret: SecretLookup | None = None,
    ) -> DetSettings:
        """
        Populate settings from process env (or *env* map).

        ``project_root`` is resolved explicitly (``--project-root`` /
        ``DET_PROJECT_ROOT`` / cwd). Lake object-store credentials are **not**
        read here — use AWS_/GCP conventions at the lake edge.
        """
        environ = os.environ if env is None else env
        root = resolve_project_root(project_root)
        lake_raw = (environ.get("DET_LAKE_PATH") or "").strip() or None
        lake_path_raw = (environ.get("DET_LAKE_PATH_RAW") or "").strip() or None
        lake_path_bronze = (environ.get("DET_LAKE_PATH_BRONZE") or "").strip() or None
        lake_path_ops = (environ.get("DET_LAKE_PATH_OPS") or "").strip() or None
        backend = resolve_secrets_backend(environ)
        ttl = cache_ttl_sec(environ)
        secrets_path = (
            secrets_file_path(env=environ, project_root=root)
            if backend == "file" or (environ.get("DET_SECRETS_FILE") or "").strip()
            else None
        )
        cache = _SecretCache()
        if resolve_secret is None:
            lookup = _env_file_lookup(
                project_root=root,
                backend=backend,
                secrets_file=secrets_path,
                ttl_sec=ttl,
                env=environ,
                cache=cache,
            )
        else:
            lookup = _caching_lookup(resolve_secret, ttl_sec=ttl, cache=cache)

        return cls(
            project_root=root,
            lake_path=lake_raw,
            lake_mode=lake_mode_from_env(environ),
            resolve_secret=lookup,
            secrets_backend=backend,
            secrets_file=secrets_path,
            secrets_ttl_sec=ttl,
            locks_enabled=locks_enabled(environ),
            lock_ttl_sec=resolve_lock_ttl_sec(None, env=environ),
            lock_owner=default_lock_owner(environ),
            lock_backend=parse_lock_backend(environ.get("DET_LOCK_BACKEND"))
            or DEFAULT_LOCK_BACKEND,
            lock_mode=parse_lock_mode(environ.get("DET_LOCK_MODE")) or DEFAULT_LOCK_MODE,
            lock_pg_dsn_env=(environ.get("DET_LOCK_PG_DSN_ENV") or "").strip()
            or DEFAULT_LOCK_PG_DSN_ENV,
            lock_pg_schema=(environ.get("DET_LOCK_PG_SCHEMA") or "").strip()
            or DEFAULT_LOCK_PG_SCHEMA,
            lock_pg_table=(environ.get("DET_LOCK_PG_TABLE") or "").strip()
            or DEFAULT_LOCK_PG_TABLE,
            lake_override=None,
            lake_path_raw=lake_path_raw,
            lake_path_bronze=lake_path_bronze,
            lake_path_ops=lake_path_ops,
            lake_override_raw=None,
            lake_override_bronze=None,
            lake_override_ops=None,
            _secret_cache=cache,
        )

    def with_overrides(
        self,
        *,
        lake_path: Any = _MISSING,
        lake_override: Any = _MISSING,
        lake_path_raw: Any = _MISSING,
        lake_path_bronze: Any = _MISSING,
        lake_path_ops: Any = _MISSING,
        lake_override_raw: Any = _MISSING,
        lake_override_bronze: Any = _MISSING,
        lake_override_ops: Any = _MISSING,
        lake_mode: Any = _MISSING,
        locks_enabled: Any = _MISSING,
        lock_ttl_sec: Any = _MISSING,
        lock_owner: Any = _MISSING,
        lock_backend: Any = _MISSING,
        lock_mode: Any = _MISSING,
        lock_pg_dsn_env: Any = _MISSING,
        lock_pg_schema: Any = _MISSING,
        lock_pg_table: Any = _MISSING,
        secrets_backend: Any = _MISSING,
        secrets_file: Any = _MISSING,
        secrets_ttl_sec: Any = _MISSING,
        resolve_secret: Any = _MISSING,
    ) -> DetSettings:
        """Return a copy with CLI/embedder flag overrides applied."""
        kwargs: dict[str, Any] = {}
        if lake_path is not _MISSING:
            kwargs["lake_path"] = lake_path
        if lake_override is not _MISSING:
            kwargs["lake_override"] = lake_override
        if lake_path_raw is not _MISSING:
            kwargs["lake_path_raw"] = lake_path_raw
        if lake_path_bronze is not _MISSING:
            kwargs["lake_path_bronze"] = lake_path_bronze
        if lake_path_ops is not _MISSING:
            kwargs["lake_path_ops"] = lake_path_ops
        if lake_override_raw is not _MISSING:
            kwargs["lake_override_raw"] = lake_override_raw
        if lake_override_bronze is not _MISSING:
            kwargs["lake_override_bronze"] = lake_override_bronze
        if lake_override_ops is not _MISSING:
            kwargs["lake_override_ops"] = lake_override_ops
        if lake_mode is not _MISSING:
            kwargs["lake_mode"] = lake_mode
        if locks_enabled is not _MISSING:
            kwargs["locks_enabled"] = locks_enabled
        if lock_ttl_sec is not _MISSING:
            if lock_ttl_sec is not None and int(lock_ttl_sec) < 1:
                raise ValueError("lock TTL must be >= 1 second")
            kwargs["lock_ttl_sec"] = (
                DEFAULT_LOCK_TTL_SEC if lock_ttl_sec is None else int(lock_ttl_sec)
            )
        if lock_owner is not _MISSING:
            kwargs["lock_owner"] = str(lock_owner)
        if lock_backend is not _MISSING:
            parsed = parse_lock_backend(None if lock_backend is None else str(lock_backend))
            kwargs["lock_backend"] = parsed or DEFAULT_LOCK_BACKEND
        if lock_mode is not _MISSING:
            parsed_mode = parse_lock_mode(None if lock_mode is None else str(lock_mode))
            kwargs["lock_mode"] = parsed_mode or DEFAULT_LOCK_MODE
        if lock_pg_dsn_env is not _MISSING:
            kwargs["lock_pg_dsn_env"] = (
                DEFAULT_LOCK_PG_DSN_ENV
                if lock_pg_dsn_env is None or not str(lock_pg_dsn_env).strip()
                else str(lock_pg_dsn_env).strip()
            )
        if lock_pg_schema is not _MISSING:
            kwargs["lock_pg_schema"] = (
                DEFAULT_LOCK_PG_SCHEMA
                if lock_pg_schema is None or not str(lock_pg_schema).strip()
                else str(lock_pg_schema).strip()
            )
        if lock_pg_table is not _MISSING:
            kwargs["lock_pg_table"] = (
                DEFAULT_LOCK_PG_TABLE
                if lock_pg_table is None or not str(lock_pg_table).strip()
                else str(lock_pg_table).strip()
            )
        if secrets_backend is not _MISSING:
            kwargs["secrets_backend"] = secrets_backend
        if secrets_file is not _MISSING:
            kwargs["secrets_file"] = secrets_file
        if secrets_ttl_sec is not _MISSING:
            if secrets_ttl_sec is not None and int(secrets_ttl_sec) < 0:
                raise ValueError("secrets TTL must be >= 0")
            kwargs["secrets_ttl_sec"] = (
                DEFAULT_CACHE_TTL_SEC
                if secrets_ttl_sec is None
                else int(secrets_ttl_sec)
            )
        if resolve_secret is not _MISSING:
            cache = _SecretCache()
            ttl = kwargs.get("secrets_ttl_sec", self.secrets_ttl_sec)
            kwargs["_secret_cache"] = cache
            kwargs["resolve_secret"] = _caching_lookup(
                resolve_secret, ttl_sec=ttl, cache=cache
            )
        return replace(self, **kwargs)

    def clear_secret_cache(self) -> None:
        self._secret_cache.clear()

    def effective_lock_ttl(self, explicit: int | None = None) -> int:
        if explicit is not None:
            if explicit < 1:
                raise ValueError("lock TTL must be >= 1 second")
            return explicit
        return self.lock_ttl_sec


def get_active_settings() -> DetSettings | None:
    return _ACTIVE.get()


@contextmanager
def use_settings(settings: DetSettings) -> Iterator[DetSettings]:
    """Bind *settings* for secret resolution and nested library calls."""
    token = _ACTIVE.set(settings)
    try:
        yield settings
    finally:
        _ACTIVE.reset(token)
