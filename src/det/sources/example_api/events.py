from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from det.logging import get_logger
from det.optional_deps import require_dlt_rest
from det.runtime.lake import LakeRef
from det.sources.base import Interval, SourceRow, mapper
from det.sources.http_json import (
    dig,
    nest_under_path,
    paginate_capped,
    source_bearer_token,
    write_json_page,
)

logger = get_logger(__name__)


class ExampleApiSource:
    """
    Sample HTTP JSON source. Connection details live in code defaults;
    YAML may override base_url / path / etc.

    Interval mode: ``query_params`` (start/end on the request).
    Raw pages are wire-shaped ``{"data": {"events": [...]}}``; no reshape at extract.
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
        data_dir: Path | LakeRef,
    ) -> list[dict[str, Any]]:
        pages_dir = data_dir / "pages"
        pages_dir.mkdir(parents=True, exist_ok=True)
        record_path = config.get("record_path") or "data.events"
        fixtures = config.get("fixture_records")
        if fixtures is not None:
            return [
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=1,
                    body=nest_under_path(list(fixtures), record_path=record_path),
                    origin="fixture_records",
                )
            ]

        RESTClient, rest_auth, rest_paginators = require_dlt_rest()
        token = source_bearer_token(config, source_name=self.name)
        client = RESTClient(
            base_url=config["base_url"],
            auth=rest_auth.BearerTokenAuth(token) if token else None,
            paginator=rest_paginators.JSONLinkPaginator(
                next_url_path=config.get("next_url_path")
            ),
            data_selector=record_path or None,
        )
        params = {"start": interval.start, "end": interval.end}
        logger.info("Fetching example API", path=config["path"], params=params)
        artifacts: list[dict[str, Any]] = []
        max_pages = int(config.get("max_pages") or 2000)
        for page_num, page in paginate_capped(
            client.paginate(config["path"], params=params),
            max_pages=max_pages,
            source_name=self.name,
        ):
            rows = [row for row in page if isinstance(row, dict)]
            artifacts.append(
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=page_num,
                    body=nest_under_path(rows, record_path=record_path),
                    origin="example_api",
                )
            )
        if not artifacts:
            artifacts.append(
                write_json_page(
                    pages_dir=pages_dir,
                    data_dir=data_dir,
                    page_num=1,
                    body=nest_under_path([], record_path=record_path),
                    origin="example_api",
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
        record_path = config.get("record_path") or "data.events"
        for art in manifest.get("artifacts") or []:
            path = raw_dir / art["path"]
            payload = json.loads(path.read_text(encoding="utf-8"))
            events = dig(payload, record_path)
            if events is None and isinstance(payload, list):
                events = payload
            if not isinstance(events, list):
                raise ValueError(f"No event list at {record_path!r} in {path}")
            for row in events:
                if isinstance(row, dict):
                    yield SourceRow(data=dict(row), filename=Path(art["path"]).name)


@mapper("example_api_v1_to_v2")
def example_api_v1_to_v2(row: dict[str, Any]) -> dict[str, Any]:
    """Contract change for example_api: rename severity to level."""
    out = dict(row)
    if "severity" in out and "level" not in out:
        out["level"] = out.pop("severity")
    return out
