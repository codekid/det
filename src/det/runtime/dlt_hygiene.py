"""Refuse dlt-shaped raw/bronze at the DET boundary.

Hard guarantee: DET never lands bronze via ``dlt.pipeline``. Soft / fail-closed:
if extract output or lake prefixes look like a full dlt landing, raise
``DetContractError`` (or emit ``det check`` findings).

Does not police arbitrary Python inside plugins — only what enters DET's lake.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from det.errors import DetContractError

DLT_KEY_PREFIX = "_dlt"
# Classic dlt filesystem destination state / version tables beside data.
DLT_STATE_NAMES = frozenset(
    {
        "_dlt_loads",
        "_dlt_pipeline_state",
        "_dlt_version",
    }
)

_HINT = (
    "DET expects wire artifacts only; use dlt helpers inside extract_to_raw, "
    "not dlt.pipeline / destinations."
)


def dlt_hygiene_message(detail: str, *, surface: str = "Raw") -> str:
    return f"{surface} looks like a dlt pipeline landing ({detail}). {_HINT}"


def first_dlt_key(obj: Any) -> str | None:
    """Return the first ``_dlt*`` key found in a nested JSON-like value."""
    if isinstance(obj, Mapping):
        for key, value in obj.items():
            if isinstance(key, str) and key.startswith(DLT_KEY_PREFIX):
                return key
            found = first_dlt_key(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = first_dlt_key(item)
            if found is not None:
                return found
    return None


def refuse_dlt_keys(obj: Any, *, surface: str = "Row") -> None:
    """Raise ``DetContractError`` if *obj* contains any ``_dlt*`` key."""
    key = first_dlt_key(obj)
    if key is not None:
        raise DetContractError(dlt_hygiene_message(f"found {key!r}", surface=surface))


def iter_dlt_named_paths(root: Path | Any, *, max_hits: int = 50) -> Iterator[str]:
    """
    Yield display paths whose basename looks dlt-managed (``_dlt*``).

    Walks directories (not file-only rglob) so empty ``_dlt_loads`` dirs are found.
    Works with ``Path`` and local ``LakeRef``. Caps results for check output.
    """
    if not getattr(root, "exists", lambda: False)():
        return
    if not hasattr(root, "iterdir"):
        return
    n = 0
    stack: list[Any] = [root]
    while stack:
        current = stack.pop()
        try:
            children = list(current.iterdir())
        except OSError:
            continue
        for child in children:
            name = getattr(child, "name", "") or ""
            if name.startswith(DLT_KEY_PREFIX):
                yield str(child)
                n += 1
                if n >= max_hits:
                    return
            if getattr(child, "is_dir", lambda: False)():
                stack.append(child)


def scan_json_file_for_dlt_keys(path: Path | Any) -> str | None:
    """Return offending key from a ``.json`` / ``.jsonl`` file, or None."""
    name = getattr(path, "name", "") or ""
    suffix = Path(name).suffix.lower() if name else ""
    if hasattr(path, "suffix"):
        suffix = path.suffix.lower()
    if suffix not in {".json", ".jsonl"}:
        return None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    if suffix == ".jsonl":
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            found = first_dlt_key(obj)
            if found is not None:
                return found
        return None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return None
    return first_dlt_key(obj)


def check_raw_hygiene(
    raw_dir: Path | Any,
    artifacts: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    """
    Fail closed if a raw extract prefix looks dlt-managed.

    Call after ``extract_to_raw`` and **before** committing ``meta/manifest.json``.
    Also safe to call at load start on an already-committed partition.
    """
    for path_str in iter_dlt_named_paths(raw_dir):
        raise DetContractError(
            dlt_hygiene_message(f"found path {path_str}", surface="Raw")
        )

    for path in _json_paths_to_scan(raw_dir, artifacts):
        if not getattr(path, "is_file", lambda: False)():
            continue
        found = scan_json_file_for_dlt_keys(path)
        if found is not None:
            raise DetContractError(
                dlt_hygiene_message(f"found {found!r} in {path}", surface="Raw")
            )


def _json_paths_to_scan(
    root: Path | Any,
    artifacts: Sequence[Mapping[str, Any]] | None,
) -> list[Any]:
    paths: list[Any] = []
    if artifacts:
        for art in artifacts:
            rel = art.get("path") if isinstance(art, Mapping) else None
            if isinstance(rel, str) and rel.strip():
                paths.append(root / rel)
        return paths
    if not hasattr(root, "rglob"):
        return paths
    if not getattr(root, "exists", lambda: False)():
        return paths
    for path in root.rglob("*"):
        if not getattr(path, "is_file", lambda: False)():
            continue
        name = getattr(path, "name", "") or ""
        suffix = Path(name).suffix.lower()
        if suffix in {".json", ".jsonl"}:
            paths.append(path)
    return paths


def lake_dlt_path_hits(
    *prefixes: Path | Any,
    max_hits: int = 20,
) -> list[str]:
    """Collect dlt-named paths under lake dataset prefixes (for ``det check``)."""
    hits: list[str] = []
    for prefix in prefixes:
        for path_str in iter_dlt_named_paths(prefix, max_hits=max_hits - len(hits)):
            hits.append(path_str)
            if len(hits) >= max_hits:
                return hits
    return hits
