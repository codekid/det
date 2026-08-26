"""Contract assertions for raw (and optional bronze) lake prefixes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.errors import DetContractError
from det.runtime.dlt_hygiene import check_raw_hygiene, lake_dlt_path_hits
from det.runtime.layout import LAKE_LAYOUT, lake_layout_of
from det.runtime.manifest import is_committed_raw_dir, read_manifest


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

    return dict(manifest)


def assert_no_dlt_artifacts(
    root: Path | Any,
    *,
    scan_json: bool = True,
) -> None:
    """
    Fail if a raw or bronze prefix looks dlt-managed.

    Shares the same rules as extract/load/``det check`` (``det.runtime.dlt_hygiene``).
    """
    try:
        if scan_json:
            check_raw_hygiene(root)
        else:
            hits = lake_dlt_path_hits(root)
            if hits:
                raise DetContractError(
                    f"dlt-shaped path under lake prefix: {hits[0]}"
                )
    except DetContractError as exc:
        raise AssertionError(str(exc)) from exc
