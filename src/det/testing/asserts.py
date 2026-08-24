"""Contract assertions for raw (and optional bronze) lake prefixes."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from det.runtime.layout import LAKE_LAYOUT, lake_layout_of
from det.runtime.manifest import is_committed_raw_dir, read_manifest

_DLT_KEY_PREFIX = "_dlt"


def assert_raw_contract(raw_dir: Path | Any) -> dict[str, Any]:
    """
    Assert a committed raw extract prefix looks like DET's contract.

    Checks: published ``meta/manifest.json``, required keys, ``lake_layout``,
    artifact paths exist under the partition, no ``manifest.json.tmp``.
    Returns the loaded manifest for further asserts.
    """
    root = raw_dir
    if not is_committed_raw_dir(root):
        raise AssertionError(f"raw dir is not committed (missing/invalid manifest): {root}")

    tmp = root / "meta" / "manifest.json.tmp"
    if tmp.exists():
        raise AssertionError(f"incomplete commit sibling still present: {tmp}")

    data_dir = root / "data"
    if not data_dir.is_dir():
        raise AssertionError(f"missing data/ under raw partition: {root}")

    manifest = read_manifest(root)
    for key in (
        "source",
        "interval_start",
        "interval_end",
        "extract_run_datetime",
        "artifacts",
    ):
        if key not in manifest:
            raise AssertionError(f"manifest missing {key!r}: {root}")

    layout = lake_layout_of(manifest)
    if layout > LAKE_LAYOUT:
        raise AssertionError(
            f"manifest lake_layout={layout} newer than this DET install "
            f"(LAKE_LAYOUT={LAKE_LAYOUT})"
        )
    if "lake_layout" in manifest and int(manifest["lake_layout"]) != layout:
        raise AssertionError(f"invalid lake_layout in manifest: {manifest.get('lake_layout')!r}")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise AssertionError(f"manifest.artifacts must be a list, got {type(artifacts)}")
    for i, art in enumerate(artifacts):
        if not isinstance(art, dict):
            raise AssertionError(f"artifact[{i}] must be an object")
        rel = art.get("path")
        if not rel or not isinstance(rel, str):
            raise AssertionError(f"artifact[{i}] missing path")
        path = root / rel
        if not path.is_file():
            raise AssertionError(f"artifact[{i}] path missing on disk: {path}")

    return manifest


def assert_no_dlt_artifacts(
    root: Path | Any,
    *,
    scan_json: bool = True,
) -> None:
    """
    Fail if a raw or bronze prefix looks dlt-managed.

    Scans directory names / files for ``_dlt*`` and optionally JSON object keys
    under ``data/`` (and ``*.jsonl`` bronze files). Full refuse-at-boundary
    hygiene lands in #32; this helper is the author-facing check today.
    """
    base = _as_local_path(root)
    if base is None:
        # Non-local LakeRef: light check on immediate children only.
        if not root.exists():
            raise AssertionError(f"path does not exist: {root}")
        for child in root.iterdir():
            if child.name.startswith(_DLT_KEY_PREFIX):
                raise AssertionError(f"dlt-shaped path under lake prefix: {child}")
        return

    if not base.exists():
        raise AssertionError(f"path does not exist: {base}")

    for path in base.rglob("*"):
        if path.name.startswith(_DLT_KEY_PREFIX):
            raise AssertionError(f"dlt-shaped path under lake prefix: {path}")

    if not scan_json:
        return

    for path in _iter_json_files(base):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise AssertionError(f"cannot read {path}: {exc}") from exc
        if path.suffix == ".jsonl":
            for line_no, line in enumerate(text.splitlines(), 1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise AssertionError(f"invalid jsonl {path}:{line_no}: {exc}") from exc
                _reject_dlt_keys(obj, where=f"{path}:{line_no}")
        else:
            try:
                obj = json.loads(text)
            except json.JSONDecodeError as exc:
                raise AssertionError(f"invalid json {path}: {exc}") from exc
            _reject_dlt_keys(obj, where=str(path))


def _as_local_path(root: Any) -> Path | None:
    if isinstance(root, Path):
        return root
    if hasattr(root, "to_path") and getattr(root, "is_local", False):
        return root.to_path()
    try:
        return Path(root)
    except TypeError:
        return None


def _iter_json_files(base: Path) -> Iterable[Path]:
    if not base.is_dir():
        if base.suffix in {".json", ".jsonl"}:
            yield base
        return
    for path in base.rglob("*"):
        if path.is_file() and path.suffix in {".json", ".jsonl"}:
            yield path


def _reject_dlt_keys(obj: Any, *, where: str) -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(key, str) and key.startswith(_DLT_KEY_PREFIX):
                raise AssertionError(f"dlt-shaped key {key!r} in {where}")
            _reject_dlt_keys(value, where=where)
    elif isinstance(obj, list):
        for item in obj:
            _reject_dlt_keys(item, where=where)
