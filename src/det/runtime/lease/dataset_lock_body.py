"""Lake dataset lock JSON body helpers."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from det.runtime.lake import LakeRef, ObjectVersionConflict
from det.runtime.lease._common import LeaseHeldError, expires_at_iso, is_expired
from det.runtime.lease.dataset_lock_types import DEFAULT_CAS_RETRIES

__all__ = [
    "_dataset_held_message",
    "_empty_body",
    "_holder",
    "_live_exclusive",
    "_live_exclusive_intent",
    "_live_shared",
    "_load_body_for_acquire",
    "_prune_body",
    "_serialize",
    "cas_mutate_lock",
    "read_dataset_lock",
    "replace_lock_if_match",
]


def _empty_body(dataset_id: str) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "exclusive": None,
        "exclusive_intent": None,
        "shared": [],
    }


def _holder(*, token: str, owner: str, command: str, ttl_sec: int) -> dict[str, Any]:
    return {
        "token": token,
        "owner": owner,
        "command": command,
        "ttl_sec": ttl_sec,
        "expires_at": expires_at_iso(ttl_sec),
    }


def _prune_body(body: dict[str, Any]) -> None:
    ex = body.get("exclusive")
    if isinstance(ex, dict) and is_expired(ex):
        body["exclusive"] = None
    intent = body.get("exclusive_intent")
    if isinstance(intent, dict) and is_expired(intent):
        body["exclusive_intent"] = None
    shared = body.get("shared")
    if not isinstance(shared, list):
        body["shared"] = []
        return
    body["shared"] = [
        row
        for row in shared
        if isinstance(row, dict) and not is_expired(row)
    ]


def _live_shared(body: Mapping[str, Any]) -> list[dict[str, Any]]:
    shared = body.get("shared")
    if not isinstance(shared, list):
        return []
    return [row for row in shared if isinstance(row, dict) and not is_expired(row)]


def _live_exclusive(body: Mapping[str, Any]) -> dict[str, Any] | None:
    ex = body.get("exclusive")
    if not isinstance(ex, dict) or is_expired(ex):
        return None
    return ex


def _live_exclusive_intent(body: Mapping[str, Any]) -> dict[str, Any] | None:
    intent = body.get("exclusive_intent")
    if not isinstance(intent, dict) or is_expired(intent):
        return None
    return intent


def _serialize(body: dict[str, Any]) -> bytes:
    return json.dumps(body, indent=2, sort_keys=True).encode("utf-8")


def _dataset_held_message(location: str, body: Mapping[str, Any]) -> str:
    ex = _live_exclusive(body)
    if ex is not None:
        owner = ex.get("owner") or "unknown"
        expires = ex.get("expires_at") or "unknown"
        return (
            f"dataset exclusive lock held by {owner} until {expires} at {location}; "
            f"after the worker is dead: det lock-release --dataset-id … --force"
        )
    intent = _live_exclusive_intent(body)
    if intent is not None:
        owner = intent.get("owner") or "unknown"
        expires = intent.get("expires_at") or "unknown"
        return (
            f"dataset exclusive lock pending by {owner} until {expires} at {location}"
        )
    count = len(_live_shared(body))
    return f"dataset lock busy ({count} shared holder(s)) at {location}"


def read_dataset_lock(path: LakeRef) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _load_body_for_acquire(
    path: LakeRef, dataset_id: str, *, exists: bool
) -> dict[str, Any]:
    if not exists:
        return _empty_body(dataset_id)
    body = read_dataset_lock(path)
    if body is None:
        raise LeaseHeldError(
            f"dataset lock exists but is unreadable at {path}",
            payload={},
        )
    return body


def replace_lock_if_match(
    path: LakeRef,
    *,
    version: str | None,
    body: dict[str, Any],
    exists: bool,
) -> str:
    """CAS write lock JSON. Raises ObjectVersionConflict or FileExistsError on retry."""
    raw = _serialize(body)
    if not exists:
        return path.create_exclusive(raw)
    if version is None:
        raise LeaseHeldError(
            _dataset_held_message(str(path), body),
            payload=dict(body),
        )
    return path.replace_if_match(version, raw)


def cas_mutate_lock(
    path: LakeRef,
    *,
    dataset_id: str,
    mutate: Callable[[dict[str, Any], bool], dict[str, Any] | None],
    max_retries: int = DEFAULT_CAS_RETRIES,
) -> tuple[str, dict[str, Any]] | None:
    """Read-modify-write with retry. Returns (version, body) or None if exhausted."""

    for _ in range(max_retries):
        exists = path.exists()
        body = _load_body_for_acquire(path, dataset_id, exists=exists)
        _prune_body(body)
        updated = mutate(body, exists)
        if updated is None:
            return None
        body = updated
        version = path.object_version() if exists else None
        try:
            new_version = replace_lock_if_match(
                path, version=version, body=body, exists=exists
            )
        except (FileExistsError, ObjectVersionConflict):
            continue
        return new_version, body
    return None
