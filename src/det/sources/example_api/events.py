from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from det.logging import get_logger
from det.runtime.manifest import sha256_file
from det.sources.base import Interval, SourceRow

logger = get_logger(__name__)


def _dig(payload: Any, path: str) -> Any:
    cur = payload
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


class ExampleApiSource:
    """
    Sample HTTP JSON source. Connection details live in code defaults;
    YAML may override base_url / path / etc.
    """

    name = "example_api.events"

    def defaults(self) -> dict[str, Any]:
        return {
            "base_url": "https://api.example.com",
            "path": "/v1/events",
            "record_path": "data.events",
            "auth_env": "EXAMPLE_API_TOKEN",
            "next_url_path": "meta.next",
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
        fixtures = config.get("fixture_records")
        if fixtures is not None:
            return [
                self._write_page(
                    pages_dir, data_dir, 1, list(fixtures), origin="fixture_records"
                )
            ]

        token_env = config.get("auth_env")
        token = os.environ.get(token_env) if token_env else None
        client = RESTClient(
            base_url=config["base_url"],
            auth=BearerTokenAuth(token) if token else None,
            paginator=JSONLinkPaginator(next_url_path=config.get("next_url_path")),
            data_selector=config.get("record_path") or None,
        )
        params = {"start": interval.start, "end": interval.end}
        logger.info("Fetching example API", path=config["path"], params=params)
        artifacts: list[dict[str, Any]] = []
        page_num = 0
        for page in client.paginate(config["path"], params=params):
            rows = [row for row in page if isinstance(row, dict)]
            page_num += 1
            artifacts.append(
                self._write_page(pages_dir, data_dir, page_num, rows, origin="example_api")
            )
        if not artifacts:
            artifacts.append(
                self._write_page(pages_dir, data_dir, 1, [], origin="example_api")
            )
        return artifacts

    def records_from_raw(
        self,
        *,
        config: dict[str, Any],
        raw_dir: Path,
        manifest: dict[str, Any],
    ) -> Iterator[SourceRow]:
        record_path = config.get("record_path") or "data.events"
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            events = _dig(payload, record_path)
            if events is None and isinstance(payload, list):
                events = payload
            if not isinstance(events, list):
                raise ValueError(f"No event list at {record_path!r} in {path}")
            for row in events:
                if isinstance(row, dict):
                    yield SourceRow(data=dict(row), filename=Path(art["path"]).name)

    def _write_page(
        self,
        pages_dir: Path,
        data_dir: Path,
        page_num: int,
        rows: list[dict[str, Any]],
        *,
        origin: str,
    ) -> dict[str, Any]:
        dest = pages_dir / f"{page_num:04d}.json"
        body = {"data": {"events": rows}}
        text = json.dumps(body)
        json.loads(text)
        dest.write_text(text, encoding="utf-8")
        return {
            "path": dest.relative_to(data_dir.parent).as_posix(),
            "origin": origin,
            "sha256": sha256_file(dest),
            "bytes": dest.stat().st_size,
            "format": "json_page",
            "content_encoding": "identity",
            "format_check": "ok",
        }


def example_api_v1_to_v2(row: dict[str, Any]) -> dict[str, Any]:
    """Contract change for example_api: rename severity to level."""
    out = dict(row)
    if "severity" in out and "level" not in out:
        out["level"] = out.pop("severity")
    return out
