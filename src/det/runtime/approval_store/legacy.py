"""Read-only legacy ``{project_root}/.det/approvals`` fallback."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from det.logging import get_logger

logger = get_logger(__name__)

_DIR_REL = Path(".det") / "approvals"
_legacy_warned = False


def legacy_approvals_dir(project_root: Path) -> Path:
    return project_root.resolve() / _DIR_REL


def _validate_id(approval_id: str) -> None:
    from det.runtime.approval import ApprovalError

    if not approval_id.startswith("apr_") or "/" in approval_id or "\\" in approval_id:
        raise ApprovalError("approval_not_found", f"invalid approval id {approval_id!r}")


class LegacyApprovalReader:
    """Load/list only — never create or mutate."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root.resolve()

    def _warn_once(self) -> None:
        global _legacy_warned
        if not _legacy_warned:
            _legacy_warned = True
            logger.warning(
                "serving approval from legacy .det/approvals; "
                "new approvals are written to the lake/postgres store"
            )

    def load(self, approval_id: str) -> dict[str, Any] | None:
        from det.runtime.approval import ApprovalError

        _validate_id(approval_id)
        path = legacy_approvals_dir(self.project_root) / f"{approval_id}.json"
        if not path.is_file():
            return None
        self._warn_once()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ApprovalError(
                "approval_not_found", f"no approval file for {approval_id}"
            ) from exc

    def list(
        self,
        *,
        statuses: Sequence[str] | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        from det.runtime.approval import effective_status

        folder = legacy_approvals_dir(self.project_root)
        if not folder.is_dir():
            return []
        wanted = set(statuses) if statuses is not None else None
        found: list[dict[str, Any]] = []
        for path in sorted(folder.glob("apr_*.json")):
            try:
                rec = dict(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                continue
            rec["status"] = effective_status(rec, now=now)
            if wanted is None or rec["status"] in wanted:
                found.append(rec)
        if found:
            self._warn_once()
        return found
