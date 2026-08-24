from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from det.logging import get_logger
from det.optional_deps import require_dlt_rest
from det.runtime.lake import LakeRef
from det.sources.base import Interval, SourceRow
from det.sources.http_json import dig, write_json_page

logger = get_logger(__name__)


def _subject_slug(subject: str) -> str:
    text = (subject or "").strip().strip("/")
    if text.startswith("subjects/"):
        text = text[len("subjects/") :]
    if text.endswith(".json"):
        text = text[: -len(".json")]
    if not text:
        raise ValueError("openlibrary.subjects requires a non-empty subject")
    return text


def _subject_path(subject: str) -> str:
    return f"/subjects/{_subject_slug(subject)}.json"


def _subject_key(subject: str) -> str:
    return f"/subjects/{_subject_slug(subject)}"


def _page_body(rows: list[dict[str, Any]], *, subject_key: str, record_path: str) -> dict[str, Any]:
    return {
        "key": subject_key,
        "name": subject_key.rsplit("/", 1)[-1],
        "subject_type": "subject",
        record_path: rows,
    }


class OpenLibrarySubjectsSource:
    """
    Open Library Subjects API → works for one configured subject key.

    Docs: https://openlibrary.org/dev/docs/api/subjects
    Default subject: /subjects/love (offset/limit pagination).

    Interval mode: ``partition_only`` — the Subjects API is a snapshot feed; start/end
    label the lake partition and are not sent as API filters.

    Wire: land API work objects as-is (no field allowlist). ``records_from_raw`` only
    injects ``subject_key``; unexpected properties fail JSON Schema validation so
    contract drift is loud. Analytics adapts belong in ``dbt.stg``.
    """

    name = "openlibrary.subjects"

    def defaults(self) -> dict[str, Any]:
        return {
            "base_url": "https://openlibrary.org",
            # Public API: declared so no credential is looked up or required.
            "auth_env": None,
            "subject": "love",
            "record_path": "works",
            "page_size": 50,
            # Cap pages so a default run does not pull the full subject catalog.
            "max_pages": 2,
            "details": False,
            "ebooks": False,
            "published_in": None,
            "fixture_records": None,
        }

    def extract_to_raw(
        self,
        *,
        config: dict[str, Any],
        interval: Interval,
        data_dir: Path | LakeRef,
    ) -> list[dict[str, Any]]:
        pages_dir = data_dir / "pages"
        pages_dir.mkdir(parents=True, exist_ok=True)
        subject = str(config.get("subject") or "love")
        subject_key = _subject_key(subject)
        record_path = str(config.get("record_path") or "works")
        fixtures = config.get("fixture_records")
        if fixtures is not None:
            rows = [dict(row) for row in fixtures if isinstance(row, dict)]
            return [
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=1,
                    body=_page_body(rows, subject_key=subject_key, record_path=record_path),
                    origin="fixture_records",
                )
            ]

        page_size = int(config.get("page_size") or 50)
        max_pages = config.get("max_pages")
        max_offset = None
        if max_pages is not None:
            max_offset = page_size * int(max_pages)

        params: dict[str, Any] = {}
        if config.get("details"):
            params["details"] = "true"
        if config.get("ebooks"):
            params["ebooks"] = "true"
        published_in = config.get("published_in")
        if published_in:
            params["published_in"] = str(published_in)

        path = _subject_path(subject)
        RESTClient, _rest_auth, rest_paginators = require_dlt_rest()
        client = RESTClient(
            base_url=str(config["base_url"]).rstrip("/"),
            paginator=rest_paginators.OffsetPaginator(
                limit=page_size,
                total_path="work_count",
                maximum_offset=max_offset,
                stop_after_empty_page=True,
            ),
            headers={
                "User-Agent": "det/0.1 (+https://github.com/local/det; openlibrary subjects)",
                "Accept": "application/json",
            },
        )
        logger.info(
            "Fetching Open Library subjects",
            path=path,
            subject_key=subject_key,
            params=params,
            page_size=page_size,
            max_pages=max_pages,
            interval_start=interval.start,
            interval_end=interval.end,
        )
        artifacts: list[dict[str, Any]] = []
        page_num = 0
        for page in client.paginate(
            path, params=params or None, data_selector=record_path
        ):
            rows = [dict(row) for row in page if isinstance(row, dict)]
            page_num += 1
            artifacts.append(
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=page_num,
                    body=_page_body(rows, subject_key=subject_key, record_path=record_path),
                    origin="openlibrary",
                )
            )
        if not artifacts:
            artifacts.append(
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=1,
                    body=_page_body([], subject_key=subject_key, record_path=record_path),
                    origin="openlibrary",
                )
            )
        return artifacts

    def records_from_raw(
        self,
        *,
        config: dict[str, Any],
        raw_dir: Path | LakeRef,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        record_path = str(config.get("record_path") or "works")
        fallback_subject = _subject_key(str(config.get("subject") or "love"))
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            works = dig(payload, record_path)
            if works is None and isinstance(payload, list):
                works = payload
            if not isinstance(works, list):
                raise ValueError(f"No works list at {record_path!r} in {path}")
            page_subject = (
                payload.get("key")
                if isinstance(payload, dict) and isinstance(payload.get("key"), str)
                else fallback_subject
            )
            for row in works:
                if isinstance(row, dict):
                    # Enrich only — do not strip unknown API fields; schema owns the contract.
                    data = dict(row)
                    data["subject_key"] = page_subject
                    yield SourceRow(data=data, filename=Path(art["path"]).name)
