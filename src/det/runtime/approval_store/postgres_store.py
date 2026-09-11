"""Postgres approval store with transactional claim."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any

from det.logging import get_logger
from det.optional_deps import pip_extra_hint
from det.runtime.approval_store.enrich import DEFAULT_HEARTBEAT_INTERVAL_SEC
from det.runtime.ids import require_sql_ident
from det.runtime.secrets import DSN_KEYS
from det.runtime.sql_types import quote_ident

logger = get_logger(__name__)

SecretLookup = Callable[[str], str | None]


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            f"postgres approval backend requires the optional extra: {pip_extra_hint('postgres')}"
        ) from exc
    return psycopg


def _iso(dt: datetime) -> str:
    from det.runtime.approval import _iso as approval_iso

    return approval_iso(dt)


def _utcnow() -> datetime:
    from det.runtime.approval import utcnow

    return utcnow()


def _parse_iso(value: str) -> datetime:
    from det.runtime.approval import _parse_iso as approval_parse

    return approval_parse(value)


class PostgresApprovalStore:
    def __init__(
        self,
        *,
        resolve_secret: SecretLookup,
        dsn_env: str,
        schema: str,
        table: str,
    ) -> None:
        self._resolve_secret = resolve_secret
        self.dsn_env = dsn_env
        self.schema = require_sql_ident(schema, what="postgres approval schema")
        self.table = require_sql_ident(table, what="postgres approval table")
        self._ensured = False

    def _dsn(self) -> str:
        raw = self._resolve_secret(self.dsn_env)
        if raw is None or not str(raw).strip():
            raise RuntimeError(
                f"postgres approval backend requires {self.dsn_env} to be set "
                f"(DET_APPROVAL_BACKEND=postgres)"
            )
        text = str(raw).strip()
        if text.startswith("{"):
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
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._qual} (
                        id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        command TEXT NOT NULL,
                        argv JSONB NOT NULL,
                        plan_digest TEXT NOT NULL,
                        approved_by TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        expires_at TIMESTAMPTZ NOT NULL,
                        claimed_at TIMESTAMPTZ,
                        claimed_by TEXT,
                        consumed_at TIMESTAMPTZ,
                        heartbeat_at TIMESTAMPTZ,
                        heartbeat_interval_sec INTEGER,
                        runner_kind TEXT,
                        runner_ref TEXT,
                        released_at TIMESTAMPTZ,
                        released_by TEXT,
                        released_from_claim JSONB
                    )
                    """
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS "
                    f"{quote_ident(self.table + '_status_idx')} "
                    f"ON {self._qual} (status)"
                )
            conn.commit()
        self._ensured = True

    def _row_to_record(self, row: dict[str, Any]) -> dict[str, Any]:
        def _ts(val: Any) -> str | None:
            if val is None:
                return None
            if isinstance(val, datetime):
                return _iso(val)
            return str(val)

        argv = row.get("argv")
        if isinstance(argv, str):
            argv = json.loads(argv)
        released = row.get("released_from_claim")
        if isinstance(released, str):
            released = json.loads(released)
        rec: dict[str, Any] = {
            "id": row["id"],
            "status": row["status"],
            "command": row["command"],
            "argv": list(argv or []),
            "plan_digest": row["plan_digest"],
            "approved_by": row["approved_by"],
            "created_at": _ts(row["created_at"]),
            "expires_at": _ts(row["expires_at"]),
            "consumed_at": _ts(row.get("consumed_at")),
        }
        if row.get("claimed_at") is not None:
            rec["claimed_at"] = _ts(row["claimed_at"])
        if row.get("claimed_by") is not None:
            rec["claimed_by"] = row["claimed_by"]
        if row.get("heartbeat_at") is not None:
            rec["heartbeat_at"] = _ts(row["heartbeat_at"])
        if row.get("heartbeat_interval_sec") is not None:
            rec["heartbeat_interval_sec"] = int(row["heartbeat_interval_sec"])
        if row.get("runner_kind") is not None:
            rec["runner_kind"] = row["runner_kind"]
        if row.get("runner_ref") is not None:
            rec["runner_ref"] = row["runner_ref"]
        if row.get("released_at") is not None:
            rec["released_at"] = _ts(row["released_at"])
        if row.get("released_by") is not None:
            rec["released_by"] = row["released_by"]
        if released is not None:
            rec["released_from_claim"] = released
        return rec

    def create(self, record: dict[str, Any]) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        self.ensure()
        psycopg = _import_psycopg()
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                        INSERT INTO {self._qual} (
                            id, status, command, argv, plan_digest, approved_by,
                            created_at, expires_at, consumed_at
                        ) VALUES (
                            %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            record["id"],
                            record["status"],
                            record["command"],
                            json.dumps(list(record["argv"])),
                            record["plan_digest"],
                            record["approved_by"],
                            _parse_iso(str(record["created_at"])),
                            _parse_iso(str(record["expires_at"])),
                            None,
                        ),
                    )
                except Exception as exc:
                    if getattr(exc, "sqlstate", None) == "23505" or "unique" in str(exc).lower():
                        raise ApprovalError(
                            "approval_exists", f"approval {record['id']} already exists"
                        ) from exc
                    raise
            conn.commit()
        return record

    def load(self, approval_id: str) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        if not approval_id.startswith("apr_") or "/" in approval_id or "\\" in approval_id:
            raise ApprovalError(
                "approval_not_found", f"invalid approval id {approval_id!r}"
            )
        self.ensure()
        psycopg = _import_psycopg()
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self._qual} WHERE id = %s",
                    (approval_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ApprovalError(
                        "approval_not_found", f"no approval file for {approval_id}"
                    )
                cols = [d.name for d in cur.description]
                return self._row_to_record(dict(zip(cols, row, strict=True)))

    def list(
        self,
        *,
        statuses: Sequence[str] | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        from det.runtime.approval import effective_status

        self.ensure()
        psycopg = _import_psycopg()
        wanted = set(statuses) if statuses is not None else None
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {self._qual} ORDER BY id")
                cols = [d.name for d in cur.description]
                found: list[dict[str, Any]] = []
                for row in cur.fetchall():
                    rec = self._row_to_record(dict(zip(cols, row, strict=True)))
                    rec["status"] = effective_status(rec, now=now)
                    if wanted is None or rec["status"] in wanted:
                        found.append(rec)
                return found

    def claim(
        self,
        approval_id: str,
        *,
        claimed_by: str,
        now: datetime | None = None,
        heartbeat_interval_sec: int = DEFAULT_HEARTBEAT_INTERVAL_SEC,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError, effective_status

        self.ensure()
        stamp = now or _utcnow()
        psycopg = _import_psycopg()
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self._qual} WHERE id = %s FOR UPDATE",
                    (approval_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ApprovalError(
                        "approval_not_found", f"no approval file for {approval_id}"
                    )
                cols = [d.name for d in cur.description]
                rec = self._row_to_record(dict(zip(cols, row, strict=True)))
                status = effective_status(rec, now=stamp)
                if status != "unused":
                    raise ApprovalError(
                        "approval_in_flight"
                        if status == "claimed"
                        else f"approval_{status}",
                        f"approval {approval_id} is {status}",
                    )
                cur.execute(
                    f"""
                    UPDATE {self._qual}
                    SET status = 'claimed',
                        claimed_at = %s,
                        claimed_by = %s,
                        heartbeat_at = %s,
                        heartbeat_interval_sec = %s,
                        runner_kind = COALESCE(runner_kind, 'cli'),
                        runner_ref = %s
                    WHERE id = %s AND status = 'unused'
                    RETURNING *
                    """,
                    (
                        stamp,
                        claimed_by,
                        stamp,
                        int(heartbeat_interval_sec),
                        claimed_by,
                        approval_id,
                    ),
                )
                updated = cur.fetchone()
                if updated is None:
                    raise ApprovalError(
                        "approval_in_flight",
                        f"approval {approval_id} is already claimed by another run",
                    )
                cols = [d.name for d in cur.description]
                out = self._row_to_record(dict(zip(cols, updated, strict=True)))
            conn.commit()
        return out

    def consume(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError, effective_status

        self.ensure()
        stamp = now or _utcnow()
        psycopg = _import_psycopg()
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self._qual} WHERE id = %s FOR UPDATE",
                    (approval_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ApprovalError(
                        "approval_not_found", f"no approval file for {approval_id}"
                    )
                cols = [d.name for d in cur.description]
                rec = self._row_to_record(dict(zip(cols, row, strict=True)))
                status = effective_status(rec, now=stamp)
                if status not in {"unused", "claimed"}:
                    raise ApprovalError(
                        f"approval_{status}", f"approval {approval_id} is {status}"
                    )
                cur.execute(
                    f"""
                    UPDATE {self._qual}
                    SET status = 'consumed', consumed_at = %s
                    WHERE id = %s
                    RETURNING *
                    """,
                    (stamp, approval_id),
                )
                updated = cur.fetchone()
                cols = [d.name for d in cur.description]
                out = self._row_to_record(dict(zip(cols, updated, strict=True)))
            conn.commit()
        return out

    def release(
        self,
        approval_id: str,
        *,
        released_by: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError, effective_status

        self.ensure()
        stamp = now or _utcnow()
        psycopg = _import_psycopg()
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self._qual} WHERE id = %s FOR UPDATE",
                    (approval_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ApprovalError(
                        "approval_not_found", f"no approval file for {approval_id}"
                    )
                cols = [d.name for d in cur.description]
                rec = self._row_to_record(dict(zip(cols, row, strict=True)))
                status = effective_status(rec, now=stamp)
                if status != "claimed":
                    raise ApprovalError(
                        "approval_not_claimed",
                        f"approval {approval_id} is {status}, "
                        "only a claimed approval can be released",
                    )
                prior = {
                    "claimed_at": rec.get("claimed_at"),
                    "claimed_by": rec.get("claimed_by"),
                }
                cur.execute(
                    f"""
                    UPDATE {self._qual}
                    SET status = 'unused',
                        claimed_at = NULL,
                        claimed_by = NULL,
                        heartbeat_at = NULL,
                        heartbeat_interval_sec = NULL,
                        released_at = %s,
                        released_by = %s,
                        released_from_claim = %s::jsonb
                    WHERE id = %s
                    RETURNING *
                    """,
                    (stamp, released_by, json.dumps(prior), approval_id),
                )
                updated = cur.fetchone()
                cols = [d.name for d in cur.description]
                out = self._row_to_record(dict(zip(cols, updated, strict=True)))
            conn.commit()
        logger.warning(
            "released a claimed approval",
            approval_id=approval_id,
            released_by=released_by,
            prior_claim=prior,
            command=out.get("command"),
            expires_at=out.get("expires_at"),
        )
        return out

    def touch_heartbeat(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        self.ensure()
        stamp = now or _utcnow()
        psycopg = _import_psycopg()
        with psycopg.connect(self._dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._qual}
                    SET heartbeat_at = %s,
                        heartbeat_interval_sec = COALESCE(
                            heartbeat_interval_sec, %s
                        )
                    WHERE id = %s AND status = 'claimed'
                    RETURNING *
                    """,
                    (stamp, DEFAULT_HEARTBEAT_INTERVAL_SEC, approval_id),
                )
                updated = cur.fetchone()
                if updated is None:
                    raise ApprovalError(
                        "approval_not_claimed",
                        f"approval {approval_id} is not claimed; cannot heartbeat",
                    )
                cols = [d.name for d in cur.description]
                out = self._row_to_record(dict(zip(cols, updated, strict=True)))
            conn.commit()
        return out
