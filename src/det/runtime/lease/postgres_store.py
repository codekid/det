"""Postgres lease store with SQL CAS (exact + overlap modes)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from det.logging import get_logger
from det.optional_deps import pip_extra_hint
from det.runtime.ids import require_sql_ident
from det.runtime.lease._common import (
    Lease,
    LeaseFencedError,
    LeaseHeldError,
    expires_at_iso,
    held_message,
    is_expired,
    lease_payload,
    lock_id,
    new_token,
)
from det.runtime.lease.store import LockMode
from det.runtime.secrets import DSN_KEYS
from det.runtime.sql_types import quote_ident

logger = get_logger(__name__)

SecretLookup = Callable[[str], str | None]


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            f"postgres lease backend requires the optional extra: {pip_extra_hint('postgres')}"
        ) from exc
    return psycopg


class PostgresLeaseStore:
    def __init__(
        self,
        *,
        resolve_secret: SecretLookup,
        dsn_env: str,
        schema: str,
        table: str,
        mode: LockMode = "exact",
    ) -> None:
        self._resolve_secret = resolve_secret
        self.dsn_env = dsn_env
        self.schema = require_sql_ident(schema, what="postgres lease schema")
        self.table = require_sql_ident(table, what="postgres lease table")
        self.mode = mode
        self._overlap_constraint = require_sql_ident(
            f"{self.table}_overlap_excl",
            what="postgres lease overlap constraint",
        )
        self._ensured = False

    def _dsn(self) -> str:
        raw = self._resolve_secret(self.dsn_env)
        if raw is None or not str(raw).strip():
            raise RuntimeError(
                f"postgres lease backend requires {self.dsn_env} to be set "
                f"(DET_LOCK_BACKEND=postgres / lease.backend: postgres)"
            )
        text = str(raw).strip()
        if text.startswith("{"):
            import json

            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return text
            if isinstance(payload, dict):
                for key in DSN_KEYS:
                    val = payload.get(key)
                    if val is not None and str(val).strip():
                        return str(val).strip()
        return text

    @property
    def _qual(self) -> str:
        return f"{quote_ident(self.schema)}.{quote_ident(self.table)}"

    def ensure(self) -> None:
        if self._ensured:
            return
        psycopg = _import_psycopg()
        dsn = self._dsn()
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                if self.mode == "overlap":
                    # DB-wide lock: extension install is not schema-scoped.
                    ek1, ek2 = _btree_gist_extension_lock_keys()
                    self._exec(
                        cur, "SELECT pg_advisory_xact_lock(%s, %s)", (ek1, ek2)
                    )
                    self._exec(cur, "CREATE EXTENSION IF NOT EXISTS btree_gist")
                    # Schema/table lock: serialize table bootstrap + constraint.
                    k1, k2 = _ensure_ddl_lock_keys(self.schema, self.table)
                    self._exec(cur, "SELECT pg_advisory_xact_lock(%s, %s)", (k1, k2))
                self._exec(
                    cur, f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}"
                )
                self._exec(
                    cur,
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._qual} (
                        pipeline TEXT NOT NULL,
                        interval_start TIMESTAMPTZ NOT NULL,
                        interval_end TIMESTAMPTZ NOT NULL,
                        owner TEXT NOT NULL,
                        command TEXT NOT NULL,
                        token TEXT NOT NULL,
                        expires_at TIMESTAMPTZ NOT NULL,
                        ttl_sec INTEGER NOT NULL,
                        held_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (pipeline, interval_start, interval_end)
                    )
                    """,
                )
                if self.mode == "overlap":
                    constraint = self._overlap_constraint
                    constraint_sql = quote_ident(constraint)
                    rel = self._qual
                    self._exec(
                        cur,
                        f"""
                        DO $$
                        BEGIN
                          IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint
                            WHERE conname = '{constraint}'
                              AND conrelid = '{rel}'::regclass
                          ) THEN
                            ALTER TABLE {rel}
                              ADD CONSTRAINT {constraint_sql}
                              EXCLUDE USING gist (
                                pipeline WITH =,
                                tstzrange(interval_start, interval_end, '[)') WITH &&
                              );
                          END IF;
                        END $$;
                        """,
                    )
            conn.commit()
        self._ensured = True

    def _connect(self):
        psycopg = _import_psycopg()
        self.ensure()
        return psycopg.connect(self._dsn())

    def _exec(self, cur: Any, sql: str, params: tuple[Any, ...] | None = None) -> None:
        if params is None:
            cur.execute(sql)  # type: ignore[arg-type]
        else:
            cur.execute(sql, params)  # type: ignore[arg-type]

    def acquire(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> Lease:
        token = new_token()
        body = lease_payload(
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            command=command,
            token=token,
            owner=owner,
            ttl_sec=ttl_sec,
        )
        start = _as_dt(interval_start)
        end = _as_dt(interval_end)
        expires = _as_dt(body["expires_at"])
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._clear_expired(cur, pipeline, start, end)
                try:
                    # Savepoint: constraint failure must not abort the outer txn.
                    with conn.transaction():
                        self._exec(
                            cur,
                            f"""
                            INSERT INTO {self._qual}
                              (pipeline, interval_start, interval_end, owner, command,
                               token, expires_at, ttl_sec)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                pipeline,
                                start,
                                end,
                                owner,
                                command,
                                token,
                                expires,
                                ttl_sec,
                            ),
                        )
                except Exception as exc:
                    if not _is_unique_or_exclusion(exc):
                        raise
                    held = self._fetch_blocking(cur, pipeline, start, end)
                    if held is None:
                        raise LeaseHeldError(
                            f"postgres lease conflict for {pipeline} "
                            f"[{interval_start}, {interval_end})",
                            payload={},
                        ) from exc
                    if is_expired(held):
                        # Steal via token CAS
                        self._exec(cur, 
                            f"""
                            UPDATE {self._qual}
                               SET owner = %s,
                                   command = %s,
                                   token = %s,
                                   expires_at = %s,
                                   ttl_sec = %s,
                                   held_at = NOW(),
                                   interval_start = %s,
                                   interval_end = %s
                             WHERE pipeline = %s
                               AND interval_start = %s
                               AND interval_end = %s
                               AND token = %s
                            RETURNING token
                            """,
                            (
                                owner,
                                command,
                                token,
                                expires,
                                ttl_sec,
                                start,
                                end,
                                pipeline,
                                _as_dt(str(held["interval_start"])),
                                _as_dt(str(held["interval_end"])),
                                held["token"],
                            ),
                        )
                        row = cur.fetchone()
                        if row is None:
                            raise LeaseHeldError(
                                held_message(f"postgres:{self._qual}", held),
                                payload=held,
                            ) from exc
                        logger.info(
                            "stole expired postgres lease",
                            pipeline=pipeline,
                            previous_owner=held.get("owner"),
                        )
                    else:
                        raise LeaseHeldError(
                            held_message(f"postgres:{self._qual}", held),
                            payload=held,
                        ) from exc
            conn.commit()
        logger.info(
            "acquired postgres lease",
            pipeline=pipeline,
            ttl_sec=ttl_sec,
            owner=owner,
            command=command,
            mode=self.mode,
        )
        return Lease(
            path=None,
            token=token,
            pipeline=pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            ttl_sec=ttl_sec,
            lock_id=lock_id(pipeline, interval_start, interval_end),
            version=None,
        )

    def _clear_expired(self, cur: Any, pipeline: str, start: datetime, end: datetime) -> None:
        if self.mode == "overlap":
            self._exec(cur, 
                f"""
                DELETE FROM {self._qual}
                 WHERE pipeline = %s
                   AND expires_at <= NOW()
                   AND tstzrange(interval_start, interval_end, '[)')
                       && tstzrange(%s, %s, '[)')
                """,
                (pipeline, start, end),
            )
        else:
            self._exec(cur, 
                f"""
                DELETE FROM {self._qual}
                 WHERE pipeline = %s
                   AND interval_start = %s
                   AND interval_end = %s
                   AND expires_at <= NOW()
                """,
                (pipeline, start, end),
            )

    def _fetch_blocking(
        self, cur: Any, pipeline: str, start: datetime, end: datetime
    ) -> dict[str, Any] | None:
        if self.mode == "overlap":
            self._exec(cur, 
                f"""
                SELECT pipeline, interval_start, interval_end, owner, command,
                       token, expires_at, ttl_sec
                  FROM {self._qual}
                 WHERE pipeline = %s
                   AND tstzrange(interval_start, interval_end, '[)')
                       && tstzrange(%s, %s, '[)')
                 ORDER BY expires_at DESC
                 LIMIT 1
                """,
                (pipeline, start, end),
            )
        else:
            self._exec(cur, 
                f"""
                SELECT pipeline, interval_start, interval_end, owner, command,
                       token, expires_at, ttl_sec
                  FROM {self._qual}
                 WHERE pipeline = %s
                   AND interval_start = %s
                   AND interval_end = %s
                 LIMIT 1
                """,
                (pipeline, start, end),
            )
        row = cur.fetchone()
        if row is None:
            return None
        return _row_payload(row)

    def refresh(self, lease: Lease) -> None:
        if not lease.token:
            return
        expires = _as_dt(expires_at_iso(lease.ttl_sec))
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._exec(cur, 
                    f"""
                    UPDATE {self._qual}
                       SET expires_at = %s, ttl_sec = %s
                     WHERE pipeline = %s
                       AND interval_start = %s
                       AND interval_end = %s
                       AND token = %s
                    """,
                    (
                        expires,
                        lease.ttl_sec,
                        lease.pipeline,
                        _as_dt(lease.interval_start),
                        _as_dt(lease.interval_end),
                        lease.token,
                    ),
                )
            conn.commit()

    def ensure_held(self, lease: Lease) -> None:
        if not lease.token:
            raise LeaseFencedError(
                "postgres lease fence requires a token",
                payload={},
            )
        expires = _as_dt(expires_at_iso(lease.ttl_sec))
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._exec(
                    cur,
                    f"""
                    UPDATE {self._qual}
                       SET expires_at = %s, ttl_sec = %s
                     WHERE pipeline = %s
                       AND interval_start = %s
                       AND interval_end = %s
                       AND token = %s
                       AND expires_at > NOW()
                    """,
                    (
                        expires,
                        lease.ttl_sec,
                        lease.pipeline,
                        _as_dt(lease.interval_start),
                        _as_dt(lease.interval_end),
                        lease.token,
                    ),
                )
                updated = cur.rowcount
            conn.commit()
        if updated == 0:
            held = self.inspect(
                pipeline=lease.pipeline,
                interval_start=lease.interval_start,
                interval_end=lease.interval_end,
            )
            raise LeaseFencedError(
                held_message(f"postgres:{self._qual}", held or {})
                if held
                else (
                    f"postgres lease lost for {lease.pipeline} "
                    f"[{lease.interval_start}, {lease.interval_end})"
                ),
                payload=dict(held) if held else {},
            )

    def release(self, lease: Lease) -> None:
        if not lease.token:
            return
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._exec(cur, 
                    f"""
                    DELETE FROM {self._qual}
                     WHERE pipeline = %s
                       AND interval_start = %s
                       AND interval_end = %s
                       AND token = %s
                    """,
                    (
                        lease.pipeline,
                        _as_dt(lease.interval_start),
                        _as_dt(lease.interval_end),
                        lease.token,
                    ),
                )
                deleted = cur.rowcount
            conn.commit()
        if deleted:
            logger.info(
                "released postgres lease",
                pipeline=lease.pipeline,
                lock_id=lease.lock_id,
            )

    def inspect(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                return self._fetch_blocking(
                    cur,
                    pipeline,
                    _as_dt(interval_start),
                    _as_dt(interval_end),
                )

    def force_release(
        self,
        *,
        pipeline: str,
        interval_start: str,
        interval_end: str,
    ) -> dict[str, Any] | None:
        start = _as_dt(interval_start)
        end = _as_dt(interval_end)
        with self._connect() as conn:
            with conn.cursor() as cur:
                if self.mode == "overlap":
                    self._exec(
                        cur,
                        f"""
                        DELETE FROM {self._qual}
                         WHERE pipeline = %s
                           AND tstzrange(interval_start, interval_end, '[)')
                               && tstzrange(%s, %s, '[)')
                         RETURNING pipeline, interval_start, interval_end, owner, command,
                                   token, expires_at, ttl_sec
                        """,
                        (pipeline, start, end),
                    )
                else:
                    self._exec(
                        cur,
                        f"""
                        DELETE FROM {self._qual}
                         WHERE pipeline = %s
                           AND interval_start = %s
                           AND interval_end = %s
                         RETURNING pipeline, interval_start, interval_end, owner, command,
                                   token, expires_at, ttl_sec
                        """,
                        (pipeline, start, end),
                    )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        payload = _row_payload(row)
        logger.info(
            "force-released postgres lease",
            pipeline=pipeline,
            owner=payload.get("owner"),
            expires_at=payload.get("expires_at"),
        )
        return payload


def _as_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _advisory_i32(text: str) -> int:
    import hashlib

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


def _btree_gist_extension_lock_keys() -> tuple[int, int]:
    """Database-wide keys for CREATE EXTENSION btree_gist serialization."""
    return _advisory_i32("det_lease_ensure:btree_gist"), _advisory_i32("extension")


def _ensure_ddl_lock_keys(schema: str, table: str) -> tuple[int, int]:
    """Schema/table keys for CREATE TABLE + overlap constraint ensure DDL."""
    return _advisory_i32(f"det_lease_ensure:{schema}"), _advisory_i32(table)


def _row_payload(row: Any) -> dict[str, Any]:
    (
        pipeline,
        interval_start,
        interval_end,
        owner,
        command,
        token,
        expires_at,
        ttl_sec,
    ) = row
    return {
        "pipeline": pipeline,
        "interval_start": interval_start.isoformat()
        if isinstance(interval_start, datetime)
        else str(interval_start),
        "interval_end": interval_end.isoformat()
        if isinstance(interval_end, datetime)
        else str(interval_end),
        "owner": owner,
        "command": command,
        "token": token,
        "expires_at": expires_at.isoformat()
        if isinstance(expires_at, datetime)
        else str(expires_at),
        "ttl_sec": int(ttl_sec),
    }


def _is_unique_or_exclusion(exc: BaseException) -> bool:
    # psycopg.errors.UniqueViolation / ExclusionViolation
    name = type(exc).__name__
    if name in {"UniqueViolation", "ExclusionViolation"}:
        return True
    sqlstate = getattr(exc, "sqlstate", None)
    return sqlstate in {"23505", "23P01"}
