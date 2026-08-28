"""Postgres bronze-dataset reader/writer lock store."""

# ruff: noqa: S608 — table/schema names are validated SQL identifiers (_locks_qual).

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from det.logging import get_logger
from det.runtime.ids import require_sql_ident, validate_canonical_id
from det.runtime.lease._common import (
    LeaseFencedError,
    LeaseHeldError,
    expires_at_iso,
    new_token,
)
from det.runtime.lease.dataset_lock import (
    DEFAULT_DATASET_LOCK_PG_TABLE,
    DEFAULT_DATASET_LOCK_SHARED_PG_TABLE,
    DatasetLockHandle,
    _dataset_held_message,
)
from det.runtime.lease.postgres_store import (
    _as_dt,
    _ensure_ddl_lock_keys,
    _import_psycopg,
)
from det.runtime.secrets import DSN_KEYS
from det.runtime.sql_types import quote_ident

logger = get_logger(__name__)

SecretLookup = Callable[[str], str | None]


class PostgresDatasetLockStore:
    def __init__(
        self,
        *,
        resolve_secret: SecretLookup,
        dsn_env: str,
        schema: str,
        locks_table: str = DEFAULT_DATASET_LOCK_PG_TABLE,
        shared_table: str = DEFAULT_DATASET_LOCK_SHARED_PG_TABLE,
    ) -> None:
        self._resolve_secret = resolve_secret
        self.dsn_env = dsn_env
        self.schema = require_sql_ident(schema, what="postgres dataset lock schema")
        self.locks_table = require_sql_ident(
            locks_table, what="postgres dataset locks table"
        )
        self.shared_table = require_sql_ident(
            shared_table, what="postgres dataset shared table"
        )
        self._ensured = False

    def _dsn(self) -> str:
        raw = self._resolve_secret(self.dsn_env)
        if raw is None or not str(raw).strip():
            raise RuntimeError(
                f"postgres dataset lock backend requires {self.dsn_env} to be set"
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
    def _locks_qual(self) -> str:
        return f"{quote_ident(self.schema)}.{quote_ident(self.locks_table)}"

    @property
    def _shared_qual(self) -> str:
        return f"{quote_ident(self.schema)}.{quote_ident(self.shared_table)}"

    def ensure(self) -> None:
        if self._ensured:
            return
        psycopg = _import_psycopg()
        dsn = self._dsn()
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                k1, k2 = _ensure_ddl_lock_keys(self.schema, self.locks_table)
                self._exec(cur, "SELECT pg_advisory_xact_lock(%s, %s)", (k1, k2))
                self._exec(
                    cur, f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}"
                )
                self._exec(
                    cur,
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._locks_qual} (
                        dataset_id TEXT PRIMARY KEY,
                        exclusive_token TEXT,
                        exclusive_owner TEXT,
                        exclusive_command TEXT,
                        exclusive_expires_at TIMESTAMPTZ,
                        exclusive_ttl_sec INTEGER,
                        exclusive_intent_token TEXT,
                        exclusive_intent_owner TEXT,
                        exclusive_intent_command TEXT,
                        exclusive_intent_expires_at TIMESTAMPTZ,
                        exclusive_intent_ttl_sec INTEGER
                    )
                    """,
                )
                for col, col_type in (
                    ("exclusive_intent_token", "TEXT"),
                    ("exclusive_intent_owner", "TEXT"),
                    ("exclusive_intent_command", "TEXT"),
                    ("exclusive_intent_expires_at", "TIMESTAMPTZ"),
                    ("exclusive_intent_ttl_sec", "INTEGER"),
                ):
                    self._exec(
                        cur,
                        f"ALTER TABLE {self._locks_qual} "
                        f"ADD COLUMN IF NOT EXISTS {col} {col_type}",
                    )
                self._exec(
                    cur,
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._shared_qual} (
                        dataset_id TEXT NOT NULL,
                        token TEXT NOT NULL,
                        owner TEXT NOT NULL,
                        command TEXT NOT NULL,
                        expires_at TIMESTAMPTZ NOT NULL,
                        ttl_sec INTEGER NOT NULL,
                        PRIMARY KEY (dataset_id, token),
                        FOREIGN KEY (dataset_id)
                          REFERENCES {self._locks_qual}(dataset_id)
                          ON DELETE CASCADE
                    )
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
            cur.execute(sql)
        else:
            cur.execute(sql, params)

    def _prune_expired(self, cur: Any, dataset_id: str) -> None:
        self._exec(
            cur,
            f"""
            DELETE FROM {self._shared_qual}
             WHERE dataset_id = %s
               AND expires_at <= NOW()
            """,
            (dataset_id,),
        )
        self._exec(
            cur,
            f"""
            UPDATE {self._locks_qual}
               SET exclusive_token = NULL,
                   exclusive_owner = NULL,
                   exclusive_command = NULL,
                   exclusive_expires_at = NULL,
                   exclusive_ttl_sec = NULL
             WHERE dataset_id = %s
               AND exclusive_expires_at IS NOT NULL
               AND exclusive_expires_at <= NOW()
            """,
            (dataset_id,),
        )
        self._exec(
            cur,
            f"""
            UPDATE {self._locks_qual}
               SET exclusive_intent_token = NULL,
                   exclusive_intent_owner = NULL,
                   exclusive_intent_command = NULL,
                   exclusive_intent_expires_at = NULL,
                   exclusive_intent_ttl_sec = NULL
             WHERE dataset_id = %s
               AND exclusive_intent_expires_at IS NOT NULL
               AND exclusive_intent_expires_at <= NOW()
            """,
            (dataset_id,),
        )

    def _clear_exclusive_intent_if_owned(self, cur: Any, dataset_id: str, token: str) -> None:
        self._exec(
            cur,
            f"""
            UPDATE {self._locks_qual}
               SET exclusive_intent_token = NULL,
                   exclusive_intent_owner = NULL,
                   exclusive_intent_command = NULL,
                   exclusive_intent_expires_at = NULL,
                   exclusive_intent_ttl_sec = NULL
             WHERE dataset_id = %s
               AND exclusive_intent_token = %s
            """,
            (dataset_id, token),
        )

    def _load_body(self, cur: Any, dataset_id: str) -> dict[str, Any]:
        self._exec(
            cur,
            f"""
            SELECT exclusive_token, exclusive_owner, exclusive_command,
                   exclusive_expires_at, exclusive_ttl_sec,
                   exclusive_intent_token, exclusive_intent_owner,
                   exclusive_intent_command, exclusive_intent_expires_at,
                   exclusive_intent_ttl_sec
              FROM {self._locks_qual}
             WHERE dataset_id = %s
            """,
            (dataset_id,),
        )
        row = cur.fetchone()
        body: dict[str, Any] = {
            "dataset_id": dataset_id,
            "exclusive": None,
            "exclusive_intent": None,
            "shared": [],
        }
        if row is not None:
            (
                token,
                owner,
                command,
                expires_at,
                ttl_sec,
                intent_token,
                intent_owner,
                intent_command,
                intent_expires_at,
                intent_ttl_sec,
            ) = row
            if token and expires_at and expires_at > datetime.now(UTC):
                body["exclusive"] = {
                    "token": token,
                    "owner": owner,
                    "command": command,
                    "expires_at": expires_at.isoformat(),
                    "ttl_sec": ttl_sec,
                }
            if (
                intent_token
                and intent_expires_at
                and intent_expires_at > datetime.now(UTC)
            ):
                body["exclusive_intent"] = {
                    "token": intent_token,
                    "owner": intent_owner,
                    "command": intent_command,
                    "expires_at": intent_expires_at.isoformat(),
                    "ttl_sec": intent_ttl_sec,
                }
        self._exec(
            cur,
            f"""
            SELECT token, owner, command, expires_at, ttl_sec
              FROM {self._shared_qual}
             WHERE dataset_id = %s
               AND expires_at > NOW()
            """,
            (dataset_id,),
        )
        body["shared"] = [
            {
                "token": r[0],
                "owner": r[1],
                "command": r[2],
                "expires_at": r[3].isoformat(),
                "ttl_sec": r[4],
            }
            for r in cur.fetchall()
        ]
        return body

    def _ensure_row(self, cur: Any, dataset_id: str) -> None:
        self._exec(
            cur,
            f"""
            INSERT INTO {self._locks_qual} (dataset_id)
            VALUES (%s)
            ON CONFLICT (dataset_id) DO NOTHING
            """,
            (dataset_id,),
        )

    def acquire_shared(
        self,
        *,
        dataset_id: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> DatasetLockHandle:
        cid = validate_canonical_id(dataset_id)
        token = new_token()
        expires = _as_dt(expires_at_iso(ttl_sec))
        location = f"postgres:{self._locks_qual}/{cid}"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (cid,))
                self._ensure_row(cur, cid)
                self._prune_expired(cur, cid)
                body = self._load_body(cur, cid)
                if body.get("exclusive") or body.get("exclusive_intent"):
                    raise LeaseHeldError(
                        _dataset_held_message(location, body),
                        payload=dict(body),
                    )
                self._exec(
                    cur,
                    f"""
                    INSERT INTO {self._shared_qual}
                        (dataset_id, token, owner, command, expires_at, ttl_sec)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dataset_id, token) DO UPDATE
                       SET owner = EXCLUDED.owner,
                           command = EXCLUDED.command,
                           expires_at = EXCLUDED.expires_at,
                           ttl_sec = EXCLUDED.ttl_sec
                    """,
                    (cid, token, owner, command, expires, ttl_sec),
                )
            conn.commit()
        logger.info(
            "acquired postgres dataset shared lock",
            dataset_id=cid,
            owner=owner,
            command=command,
        )
        return DatasetLockHandle(
            token=token,
            dataset_id=cid,
            mode="shared",
            ttl_sec=ttl_sec,
            command=command,
            owner=owner,
            store=self,
        )

    def acquire_exclusive(
        self,
        *,
        dataset_id: str,
        command: str,
        ttl_sec: int,
        owner: str,
        wait: bool = True,
        wait_sec: int | None = None,
        poll_interval: float = 0.2,
    ) -> DatasetLockHandle:
        cid = validate_canonical_id(dataset_id)
        token = new_token()
        expires = _as_dt(expires_at_iso(ttl_sec))
        location = f"postgres:{self._locks_qual}/{cid}"
        deadline = None if wait_sec is None else time.monotonic() + wait_sec
        while True:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (cid,))
                    self._ensure_row(cur, cid)
                    self._prune_expired(cur, cid)
                    body = self._load_body(cur, cid)
                    if body.get("shared"):
                        if wait:
                            intent_expires = _as_dt(expires_at_iso(ttl_sec))
                            self._exec(
                                cur,
                                f"""
                                UPDATE {self._locks_qual}
                                   SET exclusive_intent_token = %s,
                                       exclusive_intent_owner = %s,
                                       exclusive_intent_command = %s,
                                       exclusive_intent_expires_at = %s,
                                       exclusive_intent_ttl_sec = %s
                                 WHERE dataset_id = %s
                                """,
                                (
                                    token,
                                    owner,
                                    command,
                                    intent_expires,
                                    ttl_sec,
                                    cid,
                                ),
                            )
                            conn.commit()
                    elif body.get("exclusive"):
                        raise LeaseHeldError(
                            _dataset_held_message(location, body),
                            payload=dict(body),
                        )
                    else:
                        self._exec(
                            cur,
                            f"""
                            UPDATE {self._locks_qual}
                               SET exclusive_token = %s,
                                   exclusive_owner = %s,
                                   exclusive_command = %s,
                                   exclusive_expires_at = %s,
                                   exclusive_ttl_sec = %s,
                                   exclusive_intent_token = NULL,
                                   exclusive_intent_owner = NULL,
                                   exclusive_intent_command = NULL,
                                   exclusive_intent_expires_at = NULL,
                                   exclusive_intent_ttl_sec = NULL
                             WHERE dataset_id = %s
                            """,
                            (token, owner, command, expires, ttl_sec, cid),
                        )
                        conn.commit()
                        logger.info(
                            "acquired postgres dataset exclusive lock",
                            dataset_id=cid,
                            owner=owner,
                            command=command,
                        )
                        return DatasetLockHandle(
                            token=token,
                            dataset_id=cid,
                            mode="exclusive",
                            ttl_sec=ttl_sec,
                            command=command,
                            owner=owner,
                            store=self,
                        )
            if not wait:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))", (cid,)
                        )
                        self._clear_exclusive_intent_if_owned(cur, cid, token)
                    conn.commit()
                    with conn.cursor() as cur:
                        body = self._load_body(cur, cid)
                raise LeaseHeldError(
                    _dataset_held_message(location, body),
                    payload=dict(body),
                )
            if deadline is not None and time.monotonic() >= deadline:
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_xact_lock(hashtext(%s))", (cid,)
                        )
                        self._clear_exclusive_intent_if_owned(cur, cid, token)
                    conn.commit()
                    with conn.cursor() as cur:
                        body = self._load_body(cur, cid)
                raise LeaseHeldError(
                    _dataset_held_message(location, body),
                    payload=dict(body),
                )
            time.sleep(poll_interval)

    def refresh(self, handle: DatasetLockHandle) -> None:
        if not handle.token:
            return
        expires = _as_dt(expires_at_iso(handle.ttl_sec))
        with self._connect() as conn:
            with conn.cursor() as cur:
                if handle.mode == "exclusive":
                    self._exec(
                        cur,
                        f"""
                        UPDATE {self._locks_qual}
                           SET exclusive_expires_at = %s,
                               exclusive_ttl_sec = %s
                         WHERE dataset_id = %s
                           AND exclusive_token = %s
                        """,
                        (expires, handle.ttl_sec, handle.dataset_id, handle.token),
                    )
                else:
                    self._exec(
                        cur,
                        f"""
                        UPDATE {self._shared_qual}
                           SET expires_at = %s, ttl_sec = %s
                         WHERE dataset_id = %s
                           AND token = %s
                        """,
                        (expires, handle.ttl_sec, handle.dataset_id, handle.token),
                    )
            conn.commit()

    def ensure_held(self, handle: DatasetLockHandle) -> None:
        if not handle.token:
            raise LeaseFencedError(
                "postgres dataset lock fence requires a token",
                payload={},
            )
        expires = _as_dt(expires_at_iso(handle.ttl_sec))
        location = f"postgres:{self._locks_qual}/{handle.dataset_id}"
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (handle.dataset_id,),
                )
                self._prune_expired(cur, handle.dataset_id)
                if handle.mode == "exclusive":
                    self._exec(
                        cur,
                        f"""
                        UPDATE {self._locks_qual}
                           SET exclusive_expires_at = %s,
                               exclusive_ttl_sec = %s
                         WHERE dataset_id = %s
                           AND exclusive_token = %s
                           AND exclusive_expires_at > NOW()
                        """,
                        (
                            expires,
                            handle.ttl_sec,
                            handle.dataset_id,
                            handle.token,
                        ),
                    )
                    updated = cur.rowcount
                else:
                    self._exec(
                        cur,
                        f"""
                        UPDATE {self._shared_qual}
                           SET expires_at = %s, ttl_sec = %s
                         WHERE dataset_id = %s
                           AND token = %s
                           AND expires_at > NOW()
                        """,
                        (
                            expires,
                            handle.ttl_sec,
                            handle.dataset_id,
                            handle.token,
                        ),
                    )
                    updated = cur.rowcount
            conn.commit()
        if updated == 0:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    body = self._load_body(cur, handle.dataset_id)
            raise LeaseFencedError(
                _dataset_held_message(location, body),
                payload=dict(body),
            )

    def release(self, handle: DatasetLockHandle) -> None:
        if not handle.token:
            return
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (handle.dataset_id,),
                )
                if handle.mode == "exclusive":
                    self._exec(
                        cur,
                        f"""
                        UPDATE {self._locks_qual}
                           SET exclusive_token = NULL,
                               exclusive_owner = NULL,
                               exclusive_command = NULL,
                               exclusive_expires_at = NULL,
                               exclusive_ttl_sec = NULL
                         WHERE dataset_id = %s
                           AND exclusive_token = %s
                        """,
                        (handle.dataset_id, handle.token),
                    )
                else:
                    self._exec(
                        cur,
                        f"""
                        DELETE FROM {self._shared_qual}
                         WHERE dataset_id = %s
                           AND token = %s
                        """,
                        (handle.dataset_id, handle.token),
                    )
            conn.commit()

    def inspect(self, *, dataset_id: str) -> dict[str, Any] | None:
        cid = validate_canonical_id(dataset_id)
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._prune_expired(cur, cid)
                body = self._load_body(cur, cid)
        if (
            not body.get("exclusive")
            and not body.get("exclusive_intent")
            and not body.get("shared")
        ):
            return None
        return body

    def force_release(self, *, dataset_id: str) -> dict[str, Any] | None:
        cid = validate_canonical_id(dataset_id)
        with self._connect() as conn:
            with conn.cursor() as cur:
                body = self._load_body(cur, cid)
                if (
                    body.get("exclusive") is None
                    and not body.get("shared")
                    and body.get("exclusive_intent") is None
                ):
                    return None
                self._exec(
                    cur,
                    f"DELETE FROM {self._locks_qual} WHERE dataset_id = %s",
                    (cid,),
                )
            conn.commit()
        logger.info("force-released postgres dataset lock", dataset_id=cid)
        return body
