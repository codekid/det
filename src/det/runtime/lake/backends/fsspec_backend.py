"""fsspec-backed object-store lake (s3://, gs://)."""

from __future__ import annotations

import io
from typing import Any
from urllib.parse import quote

from det.runtime.lake.backends.base import _Backend
from det.runtime.lake.errors import ObjectCasUnsupported, ObjectVersionConflict


class _FsspecBackend(_Backend):
    kind = "object"

    def __init__(self, fs: Any, scheme: str) -> None:
        self.fs = fs
        self.scheme = scheme

    def identity(self) -> object:
        return (self.kind, self.scheme, id(self.fs))

    def join(self, key: str, part: str) -> str:
        base = key.rstrip("/")
        return f"{base}/{part}" if base else part

    def parent(self, key: str) -> str:
        if "/" not in key.rstrip("/"):
            return ""
        return key.rstrip("/").rsplit("/", 1)[0]

    def name(self, key: str) -> str:
        return key.rstrip("/").rsplit("/", 1)[-1] if key else ""

    def display(self, key: str) -> str:
        return f"{self.scheme}://{key}"

    def mkdir(self, key: str, *, parents: bool, exist_ok: bool) -> None:
        if not key:
            return
        self.fs.makedirs(key, exist_ok=True)

    def exists(self, key: str) -> bool:
        try:
            return bool(self.fs.exists(key))
        except FileNotFoundError:
            return False

    def is_file(self, key: str) -> bool:
        try:
            return bool(self.fs.isfile(key))
        except FileNotFoundError:
            return False

    def is_dir(self, key: str) -> bool:
        try:
            if self.fs.isdir(key):
                return True
        except FileNotFoundError:
            return False
        # Prefix with children counts as a dir even without a marker object.
        return bool(self.iterdir(key))

    def iterdir(self, key: str) -> list[str]:
        prefix = key.rstrip("/")
        try:
            listing = self.fs.ls(prefix, detail=False) if prefix else []
        except FileNotFoundError:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for item in listing:
            text = str(item).rstrip("/")
            if text == prefix:
                continue
            if text not in seen:
                seen.add(text)
                out.append(text)
        return sorted(out)

    def iter_files(self, key: str) -> list[str]:
        prefix = key.rstrip("/")
        try:
            found = self.fs.find(prefix)
        except FileNotFoundError:
            return []
        return sorted(str(p) for p in found)

    def open(self, key: str, mode: str, **kwargs: Any) -> Any:
        encoding = kwargs.get("encoding")
        errors = kwargs.get("errors")
        newline = kwargs.get("newline")
        binary_mode = mode.replace("t", "")
        if "b" not in binary_mode:
            binary_mode = binary_mode + "b"
        fh = self.fs.open(key, binary_mode)
        if "b" in mode and "t" not in mode:
            return fh
        return io.TextIOWrapper(
            fh, encoding=encoding or "utf-8", errors=errors, newline=newline
        )

    def unlink(self, key: str, *, missing_ok: bool) -> None:
        try:
            self.fs.rm(key)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def rmtree(self, key: str, *, ignore_errors: bool) -> None:
        try:
            self.fs.rm(key, recursive=True)
        except FileNotFoundError:
            if not ignore_errors:
                raise
        except Exception:
            if not ignore_errors:
                raise

    def size(self, key: str) -> int:
        info = self.fs.info(key)
        return int(info.get("size") or 0)

    def create_exclusive(self, key: str, data: bytes) -> str:
        """Strong exclusive create on s3/gs. No soft exists+wb fallback."""
        parent = self.parent(key)
        if parent:
            self.mkdir(parent, parents=True, exist_ok=True)
        if self.scheme == "s3":
            return self._s3_put(key, data, if_none_match="*")
        if self.scheme == "gs":
            return self._gcs_put(key, data, if_generation_match=0)
        raise ObjectCasUnsupported(
            f"lake exclusive create requires s3:// or gs://, got scheme={self.scheme!r}"
        )

    def object_version(self, key: str) -> str | None:
        if not self.exists(key):
            return None
        try:
            info = self.fs.info(key)
        except FileNotFoundError:
            return None
        if self.scheme == "s3":
            etag = info.get("ETag") or info.get("etag")
            if etag is None:
                return None
            return f"etag:{str(etag).strip(chr(34))}"
        if self.scheme == "gs":
            gen = info.get("generation")
            if gen is None:
                # gcsfs sometimes nests under custom metadata keys
                gen = info.get("Generation")
            if gen is None:
                return None
            return f"gen:{gen}"
        raise ObjectCasUnsupported(
            f"object_version unsupported for scheme={self.scheme!r}"
        )

    def replace_if_match(self, key: str, expected_version: str, data: bytes) -> str:
        if self.scheme == "s3":
            etag = _strip_version_prefix(expected_version, "etag:")
            return self._s3_put(key, data, if_match=etag)
        if self.scheme == "gs":
            gen = int(_strip_version_prefix(expected_version, "gen:"))
            return self._gcs_put(key, data, if_generation_match=gen)
        raise ObjectCasUnsupported(
            f"replace_if_match unsupported for scheme={self.scheme!r}"
        )

    def delete_if_match(self, key: str, expected_version: str) -> bool:
        if self.scheme == "s3":
            etag = _strip_version_prefix(expected_version, "etag:")
            return self._s3_delete(key, if_match=etag)
        if self.scheme == "gs":
            gen = int(_strip_version_prefix(expected_version, "gen:"))
            return self._gcs_delete(key, if_generation_match=gen)
        raise ObjectCasUnsupported(
            f"delete_if_match unsupported for scheme={self.scheme!r}"
        )

    def _s3_put(
        self,
        key: str,
        data: bytes,
        *,
        if_none_match: str | None = None,
        if_match: str | None = None,
    ) -> str:
        bucket, obj = _split_s3_key(key)
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": obj,
            "Body": data,
        }
        if if_none_match is not None:
            kwargs["IfNoneMatch"] = if_none_match
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        try:
            out = self.fs.call_s3("put_object", **kwargs)
        except FileExistsError:
            raise
        except Exception as exc:
            _raise_s3_cas(exc, key, create=(if_none_match is not None))
            raise  # pragma: no cover
        self.fs.invalidate_cache(key)
        etag = (out or {}).get("ETag") if isinstance(out, dict) else None
        if not etag:
            version = self.object_version(key)
            if version is None:
                raise ObjectCasUnsupported(
                    f"s3 put succeeded but etag missing for {key}"
                )
            return version
        return f"etag:{str(etag).strip(chr(34))}"

    def _s3_delete(self, key: str, *, if_match: str) -> bool:
        bucket, obj = _split_s3_key(key)
        try:
            self.fs.call_s3(
                "delete_object",
                Bucket=bucket,
                Key=obj,
                IfMatch=if_match,
            )
        except FileNotFoundError:
            return False
        except Exception as exc:
            if _is_precondition_failed(exc):
                return False
            if _is_not_found(exc):
                return False
            raise ObjectCasUnsupported(
                f"s3 conditional delete failed for {key}: {exc}"
            ) from exc
        self.fs.invalidate_cache(key)
        return True

    def _gcs_put(
        self,
        key: str,
        data: bytes,
        *,
        if_generation_match: int,
    ) -> str:
        try:
            if if_generation_match == 0:
                self.fs.pipe(key, data, mode="create")
            else:
                self._gcs_upload_generation_match(key, data, if_generation_match)
        except Exception as exc:
            if if_generation_match == 0 and (
                _is_precondition_failed(exc) or self.exists(key)
            ):
                raise FileExistsError(key) from exc
            if _is_precondition_failed(exc):
                raise ObjectVersionConflict(key) from exc
            raise ObjectCasUnsupported(
                f"gcs conditional put failed for {key}: {exc}"
            ) from exc
        version = self.object_version(key)
        if version is None:
            raise ObjectCasUnsupported(f"gcs put succeeded but generation missing for {key}")
        return version

    def _gcs_upload_generation_match(
        self, key: str, data: bytes, generation: int
    ) -> None:
        bucket, obj = _split_gcs_key(key)
        base = getattr(self.fs, "_location", "https://storage.googleapis.com")
        upload_path = f"{base}/upload/storage/v1/b/{quote(bucket)}/o"
        self.fs.call(
            "POST",
            upload_path,
            uploadType="media",
            name=obj,
            ifGenerationMatch=str(generation),
            data=data,
            json_out=True,
        )
        self.fs.invalidate_cache(key)

    def _gcs_delete(self, key: str, *, if_generation_match: int) -> bool:
        try:
            bucket, obj = _split_gcs_key(key)
            self.fs.call(
                "DELETE",
                "b/{}/o/{}",
                bucket,
                obj,
                ifGenerationMatch=str(if_generation_match),
            )
        except FileNotFoundError:
            return False
        except Exception as exc:
            if _is_precondition_failed(exc):
                return False
            if _is_not_found(exc):
                return False
            raise ObjectCasUnsupported(
                f"gcs conditional delete failed for {key}: {exc}"
            ) from exc
        self.fs.invalidate_cache(key)
        return True

    def rel_key(self, root: str, child: str) -> str | None:
        root_n = root.rstrip("/")
        child_n = child.rstrip("/")
        if not root_n:
            return child_n
        if child_n == root_n:
            return "."
        prefix = root_n + "/"
        if child_n.startswith(prefix):
            return child_n[len(prefix) :]
        return None


def _split_s3_key(key: str) -> tuple[str, str]:
    text = key.lstrip("/")
    if "/" not in text:
        raise ValueError(f"s3 key must be bucket/object, got {key!r}")
    bucket, obj = text.split("/", 1)
    return bucket, obj


def _strip_version_prefix(version: str, prefix: str) -> str:
    if version.startswith(prefix):
        return version[len(prefix) :]
    return version


def _split_gcs_key(key: str) -> tuple[str, str]:
    return _split_s3_key(key)


def _is_precondition_failed(exc: BaseException) -> bool:
    """True only for explicit 412 / precondition conflict signals (no message heuristics)."""
    names = {"PreconditionFailed", "Conflict", "ObjectVersionConflict"}
    codes = {"PreconditionFailed", "412", "ConditionalRequestConflict"}
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if type(cur).__name__ in names:
            return True
        response = getattr(cur, "response", None)
        if isinstance(response, dict):
            err = response.get("Error") or {}
            if str(err.get("Code") or "") in codes:
                return True
            status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
            if status == 412:
                return True
        status = getattr(cur, "status_code", None)
        if status is None:
            status = getattr(cur, "code", None)
        if status == 412:
            return True
        cur = cur.__cause__  # type: ignore[assignment]
    return False


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, FileNotFoundError):
        return True
    name = type(exc).__name__
    if name in {"NoSuchKey", "NotFound"}:
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        err = response.get("Error") or {}
        if str(err.get("Code") or "") in {"NoSuchKey", "404", "NotFound"}:
            return True
    return False


def _raise_s3_cas(exc: BaseException, key: str, *, create: bool) -> None:
    if _is_precondition_failed(exc):
        if create:
            raise FileExistsError(key) from exc
        raise ObjectVersionConflict(key) from exc
    raise ObjectCasUnsupported(f"s3 conditional put failed for {key}: {exc}") from exc
