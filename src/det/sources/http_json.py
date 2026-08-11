"""Shared helpers for JSON HTTP page sources (example_api, openlibrary, …)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from det.runtime.manifest import sha256_file


def dig(payload: Any, path: str) -> Any:
    """Walk a dotted path through nested dicts; return None if any step is missing."""
    cur = payload
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def nest_under_path(rows: list[dict[str, Any]], *, record_path: str) -> dict[str, Any]:
    """Nest a row list under a dotted path (e.g. ``data.events`` → ``{data: {events: …}}``)."""
    body: Any = rows
    for part in reversed(record_path.split(".")):
        body = {part: body}
    return body


def write_json_page(
    *,
    pages_dir: Path,
    data_dir: Path,
    page_num: int,
    body: Any,
    origin: str,
) -> dict[str, Any]:
    """
    Write ``pages/NNNN.json`` under ``data_dir`` and return a raw artifact descriptor.

    ``data_dir`` is the partition ``…/data`` folder; artifact ``path`` is relative to
    its parent (the raw partition root).
    """
    dest = pages_dir / f"{page_num:04d}.json"
    text = json.dumps(body)
    json.loads(text)  # fail closed on non-JSON-serializable bodies
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
