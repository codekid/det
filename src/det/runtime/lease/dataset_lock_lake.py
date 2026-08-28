"""Lake-native bronze dataset lock store (object-store CAS)."""

from __future__ import annotations

import time
from typing import Any

from det.logging import get_logger
from det.runtime.lake import LakeRef, ObjectVersionConflict
from det.runtime.lease._common import (
    LeaseFencedError,
    LeaseHeldError,
    expires_at_iso,
    new_token,
)
from det.runtime.lease.dataset_lock_body import (
    _dataset_held_message,
    _empty_body,
    _holder,
    _live_exclusive,
    _live_exclusive_intent,
    _live_shared,
    _load_body_for_acquire,
    _prune_body,
    _serialize,
    cas_mutate_lock,
    read_dataset_lock,
    replace_lock_if_match,
)
from det.runtime.lease.dataset_lock_types import (
    DEFAULT_CAS_RETRIES,
    DatasetLockHandle,
    dataset_lock_path,
)

logger = get_logger(__name__)


class LakeDatasetLockStore:
    def __init__(self, lake: LakeRef) -> None:
        self.lake = lake

    def _path(self, dataset_id: str) -> LakeRef:
        return dataset_lock_path(self.lake, dataset_id)

    def _clear_exclusive_intent_if_owned(
        self, dataset_id: str, token: str
    ) -> None:
        path = self._path(dataset_id)
        token_ref = token

        def mutate(body: dict[str, Any], _exists: bool) -> dict[str, Any] | None:
            intent = _live_exclusive_intent(body)
            if intent is None or str(intent.get("token") or "") != token_ref:
                return None
            body["exclusive_intent"] = None
            return body

        cas_mutate_lock(path, dataset_id=dataset_id, mutate=mutate)

    def acquire_shared(
        self,
        *,
        dataset_id: str,
        command: str,
        ttl_sec: int,
        owner: str,
    ) -> DatasetLockHandle:
        path = self._path(dataset_id)
        token = new_token()
        holder = _holder(token=token, owner=owner, command=command, ttl_sec=ttl_sec)
        for _ in range(DEFAULT_CAS_RETRIES):
            exists = path.exists()
            body = _load_body_for_acquire(path, dataset_id, exists=exists)
            _prune_body(body)
            if _live_exclusive(body) is not None or _live_exclusive_intent(body) is not None:
                raise LeaseHeldError(
                    _dataset_held_message(str(path), body),
                    payload=dict(body),
                )
            shared = _live_shared(body)
            if not any(str(row.get("token") or "") == token for row in shared):
                shared = list(shared)
                shared.append(holder)
                body["shared"] = shared
            version = path.object_version() if exists else None
            try:
                new_version = replace_lock_if_match(
                    path, version=version, body=body, exists=exists
                )
            except (FileExistsError, ObjectVersionConflict):
                continue
            logger.info(
                "acquired dataset shared lock",
                dataset_id=dataset_id,
                path=str(path),
                owner=owner,
                command=command,
            )
            return DatasetLockHandle(
                token=token,
                dataset_id=dataset_id,
                mode="shared",
                ttl_sec=ttl_sec,
                command=command,
                owner=owner,
                path=path,
                version=new_version,
                store=self,
            )
        raise LeaseHeldError(
            f"dataset shared lock CAS retries exhausted at {path}",
            payload={},
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
        path = self._path(dataset_id)
        token = new_token()
        deadline = None if wait_sec is None else time.monotonic() + wait_sec
        while True:
            for _ in range(DEFAULT_CAS_RETRIES):
                exists = path.exists()
                body = _load_body_for_acquire(path, dataset_id, exists=exists)
                _prune_body(body)
                if _live_exclusive(body) is not None:
                    break
                if _live_shared(body):
                    if wait:
                        body["exclusive_intent"] = _holder(
                            token=token,
                            owner=owner,
                            command=command,
                            ttl_sec=ttl_sec,
                        )
                        version = path.object_version() if exists else None
                        try:
                            replace_lock_if_match(
                                path, version=version, body=body, exists=exists
                            )
                        except (FileExistsError, ObjectVersionConflict):
                            continue
                    break
                body["exclusive"] = _holder(
                    token=token, owner=owner, command=command, ttl_sec=ttl_sec
                )
                body["shared"] = []
                body["exclusive_intent"] = None
                version = path.object_version() if exists else None
                try:
                    new_version = replace_lock_if_match(
                        path, version=version, body=body, exists=exists
                    )
                except (FileExistsError, ObjectVersionConflict):
                    continue
                logger.info(
                    "acquired dataset exclusive lock",
                    dataset_id=dataset_id,
                    path=str(path),
                    owner=owner,
                    command=command,
                )
                return DatasetLockHandle(
                    token=token,
                    dataset_id=dataset_id,
                    mode="exclusive",
                    ttl_sec=ttl_sec,
                    command=command,
                    owner=owner,
                    path=path,
                    version=new_version,
                    store=self,
                )
            if not wait:
                self._clear_exclusive_intent_if_owned(dataset_id, token)
                path = self._path(dataset_id)
                body = read_dataset_lock(path) or _empty_body(dataset_id)
                raise LeaseHeldError(
                    _dataset_held_message(str(path), body),
                    payload=dict(body),
                )
            if deadline is not None and time.monotonic() >= deadline:
                self._clear_exclusive_intent_if_owned(dataset_id, token)
                path = self._path(dataset_id)
                body = read_dataset_lock(path) or _empty_body(dataset_id)
                raise LeaseHeldError(
                    _dataset_held_message(str(path), body),
                    payload=dict(body),
                )
            time.sleep(poll_interval)

    def refresh(self, handle: DatasetLockHandle) -> None:
        if not handle.token or handle.path is None:
            return
        path = handle.path
        for _ in range(DEFAULT_CAS_RETRIES):
            body = read_dataset_lock(path)
            if body is None:
                return
            _prune_body(body)
            if handle.mode == "exclusive":
                ex = _live_exclusive(body)
                if ex is None or str(ex.get("token") or "") != handle.token:
                    return
                ex["expires_at"] = expires_at_iso(handle.ttl_sec)
                ex["ttl_sec"] = handle.ttl_sec
                body["exclusive"] = ex
            else:
                shared = _live_shared(body)
                updated = False
                for row in shared:
                    if str(row.get("token") or "") == handle.token:
                        row["expires_at"] = expires_at_iso(handle.ttl_sec)
                        row["ttl_sec"] = handle.ttl_sec
                        updated = True
                if not updated:
                    return
                body["shared"] = shared
            if handle.version is None:
                return
            try:
                handle.version = path.replace_if_match(handle.version, _serialize(body))
            except ObjectVersionConflict:
                logger.warning(
                    "dataset lock refresh CAS conflict",
                    path=str(path),
                    token=handle.token[:8],
                    dataset_id=handle.dataset_id,
                )
                handle.version = path.object_version()
                continue
            return
        logger.warning(
            "dataset lock refresh CAS retries exhausted",
            path=str(path),
            token=handle.token[:8],
            dataset_id=handle.dataset_id,
        )

    def ensure_held(self, handle: DatasetLockHandle) -> None:
        if not handle.token or handle.path is None:
            raise LeaseFencedError(
                "dataset lock fence requires a token and lock path",
                payload={},
            )
        path = handle.path
        for _ in range(DEFAULT_CAS_RETRIES):
            body = read_dataset_lock(path)
            if body is None:
                raise LeaseFencedError(
                    f"dataset lock missing at {path} (lost or released)",
                    payload={},
                )
            _prune_body(body)
            if handle.mode == "exclusive":
                ex = _live_exclusive(body)
                if ex is None or str(ex.get("token") or "") != handle.token:
                    raise LeaseFencedError(
                        _dataset_held_message(str(path), body),
                        payload=dict(body),
                    )
                ex["expires_at"] = expires_at_iso(handle.ttl_sec)
                ex["ttl_sec"] = handle.ttl_sec
                body["exclusive"] = ex
            else:
                shared = _live_shared(body)
                match = next(
                    (
                        row
                        for row in shared
                        if str(row.get("token") or "") == handle.token
                    ),
                    None,
                )
                if match is None:
                    raise LeaseFencedError(
                        _dataset_held_message(str(path), body),
                        payload=dict(body),
                    )
                match["expires_at"] = expires_at_iso(handle.ttl_sec)
                match["ttl_sec"] = handle.ttl_sec
                body["shared"] = shared
            if handle.version is None:
                raise LeaseFencedError(
                    f"dataset lock fence requires a CAS version at {path}",
                    payload=dict(body),
                )
            try:
                handle.version = path.replace_if_match(handle.version, _serialize(body))
            except ObjectVersionConflict:
                handle.version = path.object_version()
                continue
            return
        body = read_dataset_lock(path) or {}
        raise LeaseFencedError(
            _dataset_held_message(str(path), body),
            payload=dict(body),
        )

    def release(self, handle: DatasetLockHandle) -> None:
        if not handle.token or handle.path is None:
            return
        path = handle.path
        for _ in range(DEFAULT_CAS_RETRIES):
            body = read_dataset_lock(path)
            if body is None:
                return
            _prune_body(body)
            changed = False
            if handle.mode == "exclusive":
                ex = _live_exclusive(body)
                if ex is not None and str(ex.get("token") or "") == handle.token:
                    body["exclusive"] = None
                    changed = True
            else:
                shared = _live_shared(body)
                new_shared = [
                    row
                    for row in shared
                    if str(row.get("token") or "") != handle.token
                ]
                if len(new_shared) != len(shared):
                    body["shared"] = new_shared
                    changed = True
            if not changed:
                return
            version = handle.version or path.object_version()
            if (
                not _live_exclusive(body)
                and not _live_shared(body)
                and not _live_exclusive_intent(body)
            ):
                if version is None:
                    return
                if path.delete_if_match(version):
                    logger.info("released dataset lock", path=str(path))
                    handle.version = None
                    return
                handle.version = path.object_version()
                continue
            if version is None:
                return
            try:
                handle.version = path.replace_if_match(version, _serialize(body))
            except ObjectVersionConflict:
                handle.version = path.object_version()
                continue
            logger.info(
                "released dataset lock holder",
                path=str(path),
                mode=handle.mode,
            )
            return

    def inspect(self, *, dataset_id: str) -> dict[str, Any] | None:
        path = self._path(dataset_id)
        body = read_dataset_lock(path)
        if body is None:
            return None
        _prune_body(body)
        if (
            not _live_exclusive(body)
            and not _live_shared(body)
            and not _live_exclusive_intent(body)
        ):
            return None
        return body

    def force_release(self, *, dataset_id: str) -> dict[str, Any] | None:
        path = self._path(dataset_id)
        payload = read_dataset_lock(path)
        if payload is None:
            return None
        path.unlink(missing_ok=True)
        logger.info(
            "force-released dataset lock",
            path=str(path),
            dataset_id=dataset_id,
        )
        return payload
