"""Shared helpers for JSON HTTP page sources (example_api, openlibrary, …)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from det.runtime.ids import parse_canonical_id
from det.runtime.lake import LakeRef
from det.runtime.manifest import sha256_file
from det.runtime.secrets import HTTP_TOKEN_KEYS, resolve_secret, source_secret_names


def source_bearer_token(config: dict[str, Any], *, source_name: str) -> str | None:
    """
    Resolve a source credential, or None when the source declares itself public.

    ``auth_env: null`` is the explicit public declaration (NOAA, Open Library).
    Anything else must resolve: a source that declares auth and cannot resolve it
    fails the run rather than quietly fetching the unauthenticated subset.
    """
    if "auth_env" in config and config.get("auth_env") is None:
        return None
    provider, _ = parse_canonical_id(source_name)
    return resolve_secret(
        source_secret_names(provider, config.get("auth_env")),
        keys=HTTP_TOKEN_KEYS,
    )


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
    pages_dir: Path | LakeRef,
    data_dir: Path | LakeRef,
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
