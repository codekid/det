"""Lake-backed approvals under ``{ops_root}/approvals/``."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from det.runtime.approval_store.enrich import DEFAULT_HEARTBEAT_INTERVAL_SEC
from det.runtime.lake import LakeRef, ObjectVersionConflict


def _iso(dt: datetime) -> str:
    from det.runtime.approval import _iso as approval_iso

    return approval_iso(dt)


def _utcnow() -> datetime:
    from det.runtime.approval import utcnow

    return utcnow()


def _validate_id(approval_id: str) -> None:
    from det.runtime.approval import ApprovalError

    if not approval_id.startswith("apr_") or "/" in approval_id or "\\" in approval_id:
        raise ApprovalError("approval_not_found", f"invalid approval id {approval_id!r}")


class LakeApprovalStore:
    def __init__(self, ops_lake: LakeRef) -> None:
        self.ops = ops_lake
        self.root = ops_lake / "approvals"

    def _record_ref(self, approval_id: str) -> LakeRef:
        _validate_id(approval_id)
        return self.root / f"{approval_id}.json"

    def _claim_ref(self, approval_id: str) -> LakeRef:
        _validate_id(approval_id)
        return self.root / f"{approval_id}.claim"

    def _write_record(self, record: dict[str, Any], *, expected_version: str) -> None:
        """CAS update against the version observed at load (never soft-overwrite)."""
        from det.runtime.approval import ApprovalError

        ref = self._record_ref(str(record["id"]))
        payload = (json.dumps(record, indent=2) + "\n").encode("utf-8")
        try:
            ref.replace_if_match(expected_version, payload)
        except ObjectVersionConflict as exc:
            raise ApprovalError(
                "approval_conflict",
                f"approval {record['id']} changed concurrently; retry",
            ) from exc

    def _load_versioned(self, approval_id: str) -> tuple[dict[str, Any], str]:
        """Load record + opaque version; version is taken before bytes (fail closed)."""
        from det.runtime.approval import ApprovalError

        ref = self._record_ref(approval_id)
        version = ref.object_version()
        if version is None or not ref.is_file():
            raise ApprovalError(
                "approval_not_found", f"no approval file for {approval_id}"
            )
        try:
            rec = json.loads(ref.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ApprovalError(
                "approval_not_found", f"no approval file for {approval_id}"
            ) from exc
        return rec, version

    def create(self, record: dict[str, Any]) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError

        ref = self._record_ref(str(record["id"]))
        payload = (json.dumps(record, indent=2) + "\n").encode("utf-8")
        try:
            ref.create_exclusive(payload)
        except FileExistsError as exc:
            raise ApprovalError(
                "approval_exists", f"approval {record['id']} already exists"
            ) from exc
        return record

    def load(self, approval_id: str) -> dict[str, Any]:
        rec, _version = self._load_versioned(approval_id)
        return rec
    def list(
        self,
        *,
        statuses: Sequence[str] | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        from det.runtime.approval import effective_status

        if not self.root.exists():
            return []
        wanted = set(statuses) if statuses is not None else None
        found: list[dict[str, Any]] = []
        for path in sorted(self.root.glob("apr_*.json")):
            if not path.is_file():
                continue
            try:
                rec = dict(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                continue
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

        rec, version = self._load_versioned(approval_id)
        status = effective_status(rec, now=now)
        if status != "unused":
            raise ApprovalError(
                "approval_in_flight" if status == "claimed" else f"approval_{status}",
                f"approval {approval_id} is {status}",
            )
        stamp = _iso(now or _utcnow())
        claim = self._claim_ref(approval_id)
        body = json.dumps(
            {"approval_id": approval_id, "claimed_at": stamp, "claimed_by": claimed_by}
        ).encode("utf-8")
        try:
            claim.create_exclusive(body)
        except FileExistsError as exc:
            raise ApprovalError(
                "approval_in_flight",
                f"approval {approval_id} is already claimed by another run",
            ) from exc
        rec["status"] = "claimed"
        rec["claimed_at"] = stamp
        rec["claimed_by"] = claimed_by
        rec["runner_kind"] = rec.get("runner_kind") or "cli"
        rec["runner_ref"] = claimed_by
        rec["heartbeat_at"] = stamp
        rec["heartbeat_interval_sec"] = int(heartbeat_interval_sec)
        try:
            self._write_record(rec, expected_version=version)
        except Exception:
            claim.unlink(missing_ok=True)
            raise
        return rec

    def consume(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError, effective_status

        for _ in range(2):
            rec, version = self._load_versioned(approval_id)
            status = effective_status(rec, now=now)
            if status not in {"unused", "claimed"}:
                raise ApprovalError(
                    f"approval_{status}", f"approval {approval_id} is {status}"
                )
            rec["status"] = "consumed"
            rec["consumed_at"] = _iso(now or _utcnow())
            try:
                self._write_record(rec, expected_version=version)
                break
            except ApprovalError as exc:
                if exc.code != "approval_conflict":
                    raise
        else:
            raise ApprovalError(
                "approval_conflict",
                f"approval {approval_id} changed concurrently; consume failed",
            )
        claim = self._claim_ref(approval_id)
        if claim.exists():
            claim.unlink(missing_ok=True)
        return rec

    def release(
        self,
        approval_id: str,
        *,
        released_by: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.logging import get_logger
        from det.runtime.approval import ApprovalError, effective_status

        for _ in range(2):
            rec, version = self._load_versioned(approval_id)
            status = effective_status(rec, now=now)
            claim = self._claim_ref(approval_id)
            claim_exists = claim.exists()
            # Orphan .claim after crash between exclusive create and claimed JSON:
            # drop sidecar only (do not CAS-write — avoids racing a live claimer).
            if status == "unused" and claim_exists:
                claim.unlink(missing_ok=True)
                return rec
            if status != "claimed":
                raise ApprovalError(
                    "approval_not_claimed",
                    f"approval {approval_id} is {status}, only a claimed approval can be released",
                )
            prior = {
                "claimed_at": rec.pop("claimed_at", None),
                "claimed_by": rec.pop("claimed_by", None),
            }
            rec["status"] = "unused"
            rec["released_at"] = _iso(now or _utcnow())
            rec["released_by"] = released_by
            rec["released_from_claim"] = prior
            rec.pop("heartbeat_at", None)
            rec.pop("heartbeat_interval_sec", None)
            try:
                # Write first so a CAS conflict leaves the .claim sidecar intact.
                self._write_record(rec, expected_version=version)
                break
            except ApprovalError as exc:
                if exc.code != "approval_conflict":
                    raise
        else:
            raise ApprovalError(
                "approval_conflict",
                f"approval {approval_id} changed concurrently; release failed",
            )
        if claim.exists():
            claim.unlink(missing_ok=True)
        get_logger(__name__).warning(
            "released a claimed approval",
            approval_id=approval_id,
            released_by=released_by,
            prior_claim=rec["released_from_claim"],
            command=rec.get("command"),
            expires_at=rec.get("expires_at"),
        )
        return rec

    def touch_heartbeat(
        self,
        approval_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        from det.runtime.approval import ApprovalError, effective_status

        # Retry once on CAS conflict; never overwrite a consume/release that won.
        for _ in range(2):
            rec, version = self._load_versioned(approval_id)
            if effective_status(rec, now=now) != "claimed":
                raise ApprovalError(
                    "approval_not_claimed",
                    f"approval {approval_id} is not claimed; cannot heartbeat",
                )
            rec["heartbeat_at"] = _iso(now or _utcnow())
            if not rec.get("heartbeat_interval_sec"):
                rec["heartbeat_interval_sec"] = DEFAULT_HEARTBEAT_INTERVAL_SEC
            try:
                self._write_record(rec, expected_version=version)
                return rec
            except ApprovalError as exc:
                if exc.code != "approval_conflict":
                    raise
        raise ApprovalError(
            "approval_conflict",
            f"approval {approval_id} changed concurrently; heartbeat skipped",
        )
