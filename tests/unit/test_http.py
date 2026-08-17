from __future__ import annotations

import gzip
from pathlib import Path

import pytest
import requests

from det.sources.http import HttpError, HttpIntegrityError, http_get, http_get_file


class FakeResponse:
    def __init__(
        self,
        status: int,
        *,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status
        self.headers = headers or {}
        self._content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def iter_content(self, chunk_size: int = 1):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def text(self) -> str:
        return self._content.decode("utf-8")


def _patch_gets(monkeypatch, responses: list[FakeResponse]):
    leftover = list(responses)

    def fake_get(self, url, **kwargs):
        if not leftover:
            raise AssertionError(f"unexpected GET {url}")
        return leftover.pop(0)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr("det.sources.http.time.sleep", lambda s: None)


def _gz(payload: bytes) -> bytes:
    return gzip.compress(payload)


def test_http_get_retries_503_then_ok(monkeypatch):
    body = b"<html>ok</html>"
    _patch_gets(
        monkeypatch,
        [FakeResponse(503), FakeResponse(200, content=body)],
    )
    response = http_get("https://example/index", timeout=1, max_attempts=3)
    assert response.status_code == 200
    assert response.content == body


def test_http_get_file_retries_503_then_ok(monkeypatch, tmp_path: Path):
    payload = _gz(b"a,b\n1,2\n")
    dest = tmp_path / "file.csv.gz"
    _patch_gets(
        monkeypatch,
        [
            FakeResponse(503),
            FakeResponse(
                200,
                content=payload,
                headers={"Content-Length": str(len(payload))},
            ),
        ],
    )
    n = http_get_file(
        "https://example/file.csv.gz",
        dest,
        timeout=1,
        max_attempts=3,
        encoding="gzip",
    )
    assert n == len(payload)
    assert dest.read_bytes() == payload


def test_http_get_file_exhausted_503_removes_dest(monkeypatch, tmp_path: Path):
    dest = tmp_path / "file.csv.gz"
    dest.write_bytes(b"stale")
    _patch_gets(monkeypatch, [FakeResponse(503), FakeResponse(503)])
    with pytest.raises(HttpError, match="503"):
        http_get_file(
            "https://example/file.csv.gz",
            dest,
            timeout=1,
            max_attempts=2,
            encoding="gzip",
        )
    assert not dest.exists()


def test_http_get_file_short_body_raises_and_removes_dest(monkeypatch, tmp_path: Path):
    dest = tmp_path / "file.csv.gz"
    _patch_gets(
        monkeypatch,
        [
            FakeResponse(
                200,
                content=b"short",
                headers={"Content-Length": "100"},
            )
        ],
    )
    with pytest.raises(HttpIntegrityError, match="Content-Length"):
        http_get_file(
            "https://example/file.csv.gz",
            dest,
            timeout=1,
            max_attempts=1,
        )
    assert not dest.exists()


def test_http_get_file_truncated_gzip_raises(monkeypatch, tmp_path: Path):
    dest = tmp_path / "file.csv.gz"
    truncated = _gz(b"a,b\n1,2\n")[:-8]
    _patch_gets(
        monkeypatch,
        [
            FakeResponse(
                200,
                content=truncated,
                headers={"Content-Length": str(len(truncated))},
            )
        ],
    )
    with pytest.raises(HttpIntegrityError, match="gzip"):
        http_get_file(
            "https://example/file.csv.gz",
            dest,
            timeout=1,
            max_attempts=1,
            encoding="gzip",
        )
    assert not dest.exists()


def test_http_get_file_valid_gzip_matching_length(monkeypatch, tmp_path: Path):
    dest = tmp_path / "file.csv.gz"
    payload = _gz(b"col\n1\n")
    _patch_gets(
        monkeypatch,
        [
            FakeResponse(
                200,
                content=payload,
                headers={"Content-Length": str(len(payload))},
            )
        ],
    )
    n = http_get_file(
        "https://example/file.csv.gz",
        dest,
        timeout=1,
        max_attempts=1,
        encoding="gzip",
    )
    assert n == len(payload)
    assert dest.exists()
    assert gzip.decompress(dest.read_bytes()) == b"col\n1\n"


def test_http_get_does_not_retry_401_without_a_refresher(monkeypatch):
    calls = {"n": 0}

    def fake_get(self, url, **kwargs):
        calls["n"] += 1
        return FakeResponse(401)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        http_get("https://example/events", timeout=1, max_attempts=4)
    assert calls["n"] == 1


def test_http_get_retries_401_once_with_rotated_credential(monkeypatch):
    sent: list[dict[str, str] | None] = []

    def fake_get(self, url, **kwargs):
        sent.append(kwargs.get("headers"))
        return FakeResponse(401) if len(sent) == 1 else FakeResponse(200, content=b"ok")

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr("det.sources.http.time.sleep", lambda s: None)
    response = http_get(
        "https://example/events",
        timeout=1,
        headers={"Authorization": "Bearer stale"},
        max_attempts=4,
        refresh_headers=lambda: {"Authorization": "Bearer rotated"},
    )
    assert response.status_code == 200
    assert sent == [
        {"Authorization": "Bearer stale"},
        {"Authorization": "Bearer rotated"},
    ]


def test_http_get_refreshes_the_credential_only_once(monkeypatch):
    calls = {"n": 0}

    def fake_get(self, url, **kwargs):
        calls["n"] += 1
        return FakeResponse(401)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr("det.sources.http.time.sleep", lambda s: None)
    with pytest.raises(requests.HTTPError):
        http_get(
            "https://example/events",
            timeout=1,
            max_attempts=4,
            refresh_headers=lambda: {"Authorization": "Bearer rotated"},
        )
    assert calls["n"] == 2


def test_http_get_file_retries_401_once_with_rotated_credential(
    monkeypatch, tmp_path: Path
):
    payload = _gz(b"col\n1\n")
    dest = tmp_path / "file.csv.gz"
    sent: list[dict[str, str] | None] = []

    def fake_get(self, url, **kwargs):
        sent.append(kwargs.get("headers"))
        if len(sent) == 1:
            return FakeResponse(403)
        return FakeResponse(
            200, content=payload, headers={"Content-Length": str(len(payload))}
        )

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr("det.sources.http.time.sleep", lambda s: None)
    n = http_get_file(
        "https://example/file.csv.gz",
        dest,
        timeout=1,
        headers={"Authorization": "Bearer stale"},
        max_attempts=3,
        encoding="gzip",
        refresh_headers=lambda: {"Authorization": "Bearer rotated"},
    )
    assert n == len(payload)
    assert sent[-1] == {"Authorization": "Bearer rotated"}


def test_http_get_does_not_retry_404(monkeypatch):
    calls = {"n": 0}

    def fake_get(self, url, **kwargs):
        calls["n"] += 1
        return FakeResponse(404)

    monkeypatch.setattr(requests.Session, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        http_get("https://example/missing", timeout=1, max_attempts=4)
    assert calls["n"] == 1
