"""Bronze ↔ silver catch-up: latest-per-interval diff, ops manifest, dbt vars.

Correctness grain: for each interval, the latest bronze ``__extract_run_datetime``
must appear in silver for that same ``(interval_start, interval_end)``. Coverage
keys are ``(interval_start, interval_end, extract_run_datetime)`` — run timestamps
alone are not unique across intervals. Older siblings are informational only.
Catch-up heals via an **immutable** ops manifest pair
(``ops/silver_catchup/<manifest_id>.json`` + ``.runs.jsonl``) and one
``det dbt --catchup`` build (DuckDB ``read_json`` or BigQuery external table on
GCS; not full-refresh).
"""

from __future__ import annotations

from pathlib import Path

from det.runtime.lake import LakeRef, relpath, resolve_lake_roots
from det.runtime.settings import DetSettings, get_active_settings
from det.runtime.silver_catchup.ids import validate_catchup_manifest_id

CATCHUP_DIR = ("ops", "silver_catchup")


def catchup_manifest_ref(ops_lake: LakeRef, manifest_id: str) -> LakeRef:
    mid = validate_catchup_manifest_id(manifest_id)
    ref = ops_lake
    for part in CATCHUP_DIR:
        ref = ref / part
    return ref / f"{mid}.json"


def catchup_runs_ref(ops_lake: LakeRef, manifest_id: str) -> LakeRef:
    """Sibling NDJSON (``<mid>.runs.jsonl``) for BigQuery external-table heal."""
    mid = validate_catchup_manifest_id(manifest_id)
    ref = ops_lake
    for part in CATCHUP_DIR:
        ref = ref / part
    return ref / f"{mid}.runs.jsonl"


def catchup_runs_ref_from_manifest(manifest_ref: LakeRef) -> LakeRef:
    """Derive ``<mid>.runs.jsonl`` from an ``<mid>.json`` lake ref."""
    name = manifest_ref.name
    if not name.endswith(".json"):
        raise ValueError(f"catch-up manifest path must end with .json, got {name!r}")
    return manifest_ref.parent / f"{name[:-5]}.runs.jsonl"


def resolve_ops_lake(
    *,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    active = settings if settings is not None else get_active_settings()
    if active is None:
        active = DetSettings.from_env(project_root=project_root)
    if lake_path is not None and str(lake_path).strip():
        active = active.with_overrides(lake_override=str(lake_path).strip())
    roots = resolve_lake_roots(active, project_root=project_root)
    return roots.ops

def catchup_manifest_file_path(
    *,
    manifest_id: str,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    """Absolute/URI path to ``ops/silver_catchup/<manifest_id>.json``."""
    mid = validate_catchup_manifest_id(manifest_id)
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    return catchup_manifest_ref(ops, mid)


def catchup_runs_file_path(
    *,
    manifest_id: str,
    project_root: Path,
    settings: DetSettings | None = None,
    lake_path: str | None = None,
) -> LakeRef:
    """Absolute/URI path to ``ops/silver_catchup/<manifest_id>.runs.jsonl``."""
    mid = validate_catchup_manifest_id(manifest_id)
    ops = resolve_ops_lake(
        project_root=project_root, settings=settings, lake_path=lake_path
    )
    return catchup_runs_ref(ops, mid)

def manifest_relpath_for_root(project_root: Path, path: LakeRef) -> str:
    return relpath(path, project_root)

