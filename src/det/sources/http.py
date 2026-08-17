from __future__ import annotations

import gzip
import random
import tempfile
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

from det.logging import get_logger
from det.runtime.lake import LakeRef

logger = get_logger(__name__)

DEFAULT_MAX_ATTEMPTS = 4
BACKOFF_CAP_SECONDS = 60.0
_STREAM_CHUNK = 256 * 1024


class HttpError(Exception):
    """Retry exhausted or non-retryable HTTP failure."""


class HttpIntegrityError(HttpError):
    """Content-Length mismatch or truncated/corrupt gzip."""


def _is_retryable_status(status: int) -> bool:
    return status == 429 or status >= 500


def _backoff_seconds(attempt: int, retry_after: str | None) -> float:
    if retry_after is not None and retry_after.strip():
        text = retry_after.strip()
        try:
            return min(BACKOFF_CAP_SECONDS, max(0.0, float(text)))
        except ValueError:
            try:
                when = parsedate_to_datetime(text)
                delay = (when.timestamp() - time.time())
                return min(BACKOFF_CAP_SECONDS, max(0.0, delay))
            except (TypeError, ValueError, OverflowError):
                pass
    base = min(BACKOFF_CAP_SECONDS, float(2**attempt))
    return min(BACKOFF_CAP_SECONDS, base + random.uniform(0.0, 0.5 * base))


def _unlink(path: Path | LakeRef) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _want_gzip(dest: Path | LakeRef, encoding: str | None) -> bool:
    if encoding == "gzip":
        return True
    return dest.name.endswith(".gz")


def verify_gzip(path: Path) -> None:
    """Stream-decompress *path*; raise HttpIntegrityError if truncated or corrupt."""
    try:
        with path.open("rb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                while gz.read(_STREAM_CHUNK):
                    pass
    except (EOFError, gzip.BadGzipFile, OSError, ValueError) as exc:
        raise HttpIntegrityError(f"truncated or corrupt gzip: {path.name}") from exc


def _raise_or_retry_status(response: requests.Response) -> None:
    if _is_retryable_status(response.status_code):
        retry_after = response.headers.get("Retry-After")
        raise _RetryableStatus(response.status_code, retry_after=retry_after)
    response.raise_for_status()


class _RetryableStatus(Exception):
    def __init__(self, status: int, retry_after: str | None = None) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


def _rotated_headers(
    exc: requests.HTTPError,
    refresh_headers: Callable[[], dict[str, str]] | None,
) -> dict[str, str] | None:
    """
    New headers when a 401/403 may just be a rotated credential, else None.

    Callers invalidate their cached secret inside *refresh_headers*, so the retry
    fetches the current value. 401 stays non-retryable without a refresher.
    """
    if refresh_headers is None:
        return None
    status = exc.response.status_code if exc.response is not None else None
    if status not in (401, 403):
        return None
    logger.info("http auth refresh after rejected credential", status=status)
    return refresh_headers()


def http_get(
    url: str,
    *,
    timeout: float | tuple[float, float],
    headers: dict[str, str] | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    session: requests.Session | None = None,
    refresh_headers: Callable[[], dict[str, str]] | None = None,
) -> requests.Response:
    """
    GET a small body with retry on 429/5xx and disconnects.

    ``refresh_headers`` re-resolves the credential once on a 401/403 so a rotation
    mid-backfill does not fail the run.
    """
    own_session = session is None
    sess = session or requests.Session()
    last_exc: BaseException | None = None
    refreshed = False
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                response = sess.get(url, headers=headers, timeout=timeout)
                _raise_or_retry_status(response)
                return response
            except _RetryableStatus as exc:
                last_exc = HttpError(f"{url}: {exc}")
                retry_after = exc.retry_after
            except requests.RequestException as exc:
                if isinstance(exc, requests.HTTPError):
                    last_exc = exc
                    rotated = None if refreshed else _rotated_headers(exc, refresh_headers)
                    if rotated is None:
                        raise
                    refreshed = True
                    headers = rotated
                    continue
                last_exc = exc
                retry_after = None
            if attempt >= max_attempts:
                break
            delay = _backoff_seconds(attempt, retry_after)
            logger.info(
                "http retry",
                url=url,
                attempt=attempt,
                max_attempts=max_attempts,
                sleep_s=round(delay, 3),
            )
            time.sleep(delay)
    finally:
        if own_session:
            sess.close()
    if last_exc is not None:
        raise last_exc
    raise HttpError(f"{url}: retry exhausted")


def http_get_file(
    url: str,
    dest: Path | LakeRef,
    *,
    timeout: float | tuple[float, float],
    headers: dict[str, str] | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    log_every: int = 1024 * 1024,
    encoding: str | None = None,
    session: requests.Session | None = None,
    refresh_headers: Callable[[], dict[str, str]] | None = None,
) -> int:
    """
    Stream GET to *dest* with retry. Unlink dest before each attempt.

    After 200: require Content-Length match when the header is present; verify
    gzip members when *encoding* is gzip or dest ends with ``.gz``.

    Object-store dests download to a local tempfile, verify, then upload so a
    truncated gzip is never PUT.
    """
    if isinstance(dest, LakeRef) and not dest.is_local:
        return _http_get_file_remote(
            url,
            dest,
            timeout=timeout,
            headers=headers,
            max_attempts=max_attempts,
            log_every=log_every,
            encoding=encoding,
            session=session,
            refresh_headers=refresh_headers,
        )
    local = dest.to_path() if isinstance(dest, LakeRef) else Path(dest)
    return _http_get_file_local(
        url,
        local,
        timeout=timeout,
        headers=headers,
        max_attempts=max_attempts,
        log_every=log_every,
        encoding=encoding,
        session=session,
        refresh_headers=refresh_headers,
    )


def _http_get_file_local(
    url: str,
    dest: Path,
    *,
    timeout: float | tuple[float, float],
    headers: dict[str, str] | None,
    max_attempts: int,
    log_every: int,
    encoding: str | None,
    session: requests.Session | None,
    refresh_headers: Callable[[], dict[str, str]] | None = None,
) -> int:
    own_session = session is None
    sess = session or requests.Session()
    last_exc: BaseException | None = None
    refreshed = False
    try:
        for attempt in range(1, max_attempts + 1):
            _unlink(dest)
            try:
                downloaded = _stream_once(
                    sess,
                    url,
                    dest,
                    timeout=timeout,
                    headers=headers,
                    log_every=log_every,
                )
                if _want_gzip(dest, encoding):
                    verify_gzip(dest)
                return downloaded
            except HttpIntegrityError as exc:
                last_exc = exc
                retry_after = None
            except _RetryableStatus as exc:
                last_exc = HttpError(f"{url}: {exc}")
                retry_after = exc.retry_after
            except requests.RequestException as exc:
                if isinstance(exc, requests.HTTPError):
                    last_exc = exc
                    rotated = None if refreshed else _rotated_headers(exc, refresh_headers)
                    if rotated is None:
                        raise
                    refreshed = True
                    headers = rotated
                    _unlink(dest)
                    continue
                last_exc = exc
                retry_after = None
            _unlink(dest)
            if attempt >= max_attempts:
                break
            delay = _backoff_seconds(attempt, retry_after)
            logger.info(
                "http file retry",
                url=url,
                dest=dest.name,
                attempt=attempt,
                max_attempts=max_attempts,
                sleep_s=round(delay, 3),
            )
            time.sleep(delay)
    finally:
        if own_session:
            sess.close()
    _unlink(dest)
    if last_exc is not None:
        raise last_exc
    raise HttpError(f"{url}: retry exhausted")


def _http_get_file_remote(
    url: str,
    dest: LakeRef,
    *,
    timeout: float | tuple[float, float],
    headers: dict[str, str] | None,
    max_attempts: int,
    log_every: int,
    encoding: str | None,
    session: requests.Session | None,
    refresh_headers: Callable[[], dict[str, str]] | None = None,
) -> int:
    own_session = session is None
    sess = session or requests.Session()
    last_exc: BaseException | None = None
    refreshed = False
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        for attempt in range(1, max_attempts + 1):
            dest.unlink(missing_ok=True)
            handle = tempfile.NamedTemporaryFile(delete=False)
            tmp_path = Path(handle.name)
            handle.close()
            try:
                try:
                    downloaded = _stream_once(
                        sess,
                        url,
                        tmp_path,
                        timeout=timeout,
                        headers=headers,
                        log_every=log_every,
                    )
                    if _want_gzip(dest, encoding):
                        verify_gzip(tmp_path)
                    with tmp_path.open("rb") as src, dest.open("wb") as out:
                        while True:
                            chunk = src.read(_STREAM_CHUNK)
                            if not chunk:
                                break
                            out.write(chunk)
                    return downloaded
                except HttpIntegrityError as exc:
                    last_exc = exc
                    retry_after = None
                except _RetryableStatus as exc:
                    last_exc = HttpError(f"{url}: {exc}")
                    retry_after = exc.retry_after
                except requests.RequestException as exc:
                    if isinstance(exc, requests.HTTPError):
                        last_exc = exc
                        rotated = (
                            None if refreshed else _rotated_headers(exc, refresh_headers)
                        )
                        if rotated is None:
                            raise
                        refreshed = True
                        headers = rotated
                        dest.unlink(missing_ok=True)
                        continue
                    last_exc = exc
                    retry_after = None
                dest.unlink(missing_ok=True)
                if attempt >= max_attempts:
                    break
                delay = _backoff_seconds(attempt, retry_after)
                logger.info(
                    "http file retry",
                    url=url,
                    dest=dest.name,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    sleep_s=round(delay, 3),
                )
                time.sleep(delay)
            finally:
                tmp_path.unlink(missing_ok=True)
    finally:
        if own_session:
            sess.close()
    dest.unlink(missing_ok=True)
    if last_exc is not None:
        raise last_exc
    raise HttpError(f"{url}: retry exhausted")


def _stream_once(
    sess: requests.Session,
    url: str,
    dest: Path,
    *,
    timeout: float | tuple[float, float],
    headers: dict[str, str] | None,
    log_every: int,
) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    last_logged = 0
    with sess.get(url, stream=True, headers=headers, timeout=timeout) as response:
        _raise_or_retry_status(response)
        length_header = response.headers.get("Content-Length")
        expected: int | None = None
        if length_header is not None and str(length_header).strip():
            try:
                expected = int(str(length_header).strip())
            except ValueError as exc:
                raise HttpIntegrityError(
                    f"{url}: invalid Content-Length {length_header!r}"
                ) from exc
            logger.info("http download size", url=url, bytes=expected, dest=dest.name)
        with dest.open("wb") as out:
            for chunk in response.iter_content(chunk_size=_STREAM_CHUNK):
                if not chunk:
                    continue
                out.write(chunk)
                downloaded += len(chunk)
                if log_every >= 1 and downloaded - last_logged >= log_every:
                    logger.info(
                        "http download progress",
                        dest=dest.name,
                        bytes=downloaded,
                        total_bytes=expected,
                    )
                    last_logged = downloaded
    if expected is not None and downloaded != expected:
        raise HttpIntegrityError(
            f"{url}: Content-Length {expected} != downloaded {downloaded}"
        )
    logger.info("http download finished", dest=dest.name, bytes=downloaded)
    return downloaded
