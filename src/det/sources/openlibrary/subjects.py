from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator

from det.logging import get_logger
from det.sources.base import Interval, SourceRow
from det.sources.http_json import dig, write_json_page

logger = get_logger(__name__)

# Curated bronze contract keys (wire names). Open Library responses are open-ended;
# we project once at extract so raw pages *are* the declared wire (see reshape policy
# in README / det-new-source). Do not re-project in records_from_raw.
_WORK_FIELDS = (
    "key",
    "title",
    "edition_count",
    "cover_id",
    "cover_edition_key",
    "first_publish_year",
    "has_fulltext",
    "public_scan",
    "printdisabled",
    "ia",
    "lending_edition",
    "lending_identifier",
    "authors",
    "subject",
    "ia_collection",
    "availability",
)

_AVAILABILITY_FIELDS = (
    "status",
    "available_to_browse",
    "available_to_borrow",
    "available_to_waitlist",
    "is_printdisabled",
    "is_readable",
    "is_lendable",
    "is_previewable",
    "identifier",
    "isbn",
    "oclc",
    "openlibrary_work",
    "openlibrary_edition",
    "is_restricted",
    "is_browseable",
)


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


def _project_work(work: dict[str, Any], *, subject_key: str) -> dict[str, Any]:
    out: dict[str, Any] = {"subject_key": subject_key}
    for field in _WORK_FIELDS:
        if field not in work:
            continue
        value = work[field]
        if field == "availability" and isinstance(value, dict):
            out[field] = {
                k: value[k] for k in _AVAILABILITY_FIELDS if k in value
            }
        elif field == "authors" and isinstance(value, list):
            authors: list[dict[str, Any]] = []
            for item in value:
                if not isinstance(item, dict):
                    continue
                authors.append(
                    {k: item[k] for k in ("key", "name") if k in item}
                )
            out[field] = authors
        else:
            out[field] = value
    return out


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

    Reshape: curated-contract exception — project works onto the bronze schema once
    when writing raw pages; ``records_from_raw`` yields those rows as-is.
    Grain: silver unique on ``(key, subject_key)`` so the same work under two subjects
    keeps both rows.
    """

    name = "openlibrary.subjects"

    def defaults(self) -> dict[str, Any]:
        return {
            "base_url": "https://openlibrary.org",
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
        data_dir: Path,
    ) -> list[dict[str, Any]]:
        pages_dir = data_dir / "pages"
        pages_dir.mkdir(parents=True, exist_ok=True)
        subject = str(config.get("subject") or "love")
        subject_key = _subject_key(subject)
        record_path = str(config.get("record_path") or "works")
        fixtures = config.get("fixture_records")
        if fixtures is not None:
            rows = [
                _project_work(dict(row), subject_key=subject_key)
                for row in fixtures
                if isinstance(row, dict)
            ]
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
        client = RESTClient(
            base_url=str(config["base_url"]).rstrip("/"),
            paginator=OffsetPaginator(
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
            rows = [
                _project_work(row, subject_key=subject_key)
                for row in page
                if isinstance(row, dict)
            ]
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
        raw_dir: Path,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        # Rows were projected at extract; yield as-is (curated wire = raw page works).
        record_path = str(config.get("record_path") or "works")
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            works = dig(payload, record_path)
            if works is None and isinstance(payload, list):
                works = payload
            if not isinstance(works, list):
                raise ValueError(f"No works list at {record_path!r} in {path}")
            for row in works:
                if isinstance(row, dict):
                    yield SourceRow(data=dict(row), filename=Path(art["path"]).name)
