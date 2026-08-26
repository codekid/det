from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
from typing import Any, cast

from det.runtime.lake import LakeRef
from det.runtime.manifest_types import ManifestPayload, ManifestValidation

META_DIR = "meta"
MANIFEST_NAME = "manifest.json"

LakePath = Path | LakeRef


def meta_dir(raw_dir: LakePath) -> LakePath:
    return raw_dir / META_DIR


def manifest_path(raw_dir: LakePath) -> LakePath:
    return meta_dir(raw_dir) / MANIFEST_NAME


def sha256_file(path: LakePath) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_committed_raw_dir(raw_dir: LakePath) -> bool:
    """
    True when this extract-run prefix has a published manifest.

    Data files may already sit at final keys (one write, no copy). Visibility
    for load/migrate/list is the tiny commit object ``meta/manifest.json``.
    A ``.tmp`` sibling is never a commit.
    """
    path = manifest_path(raw_dir)
    if not path.is_file():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    return isinstance(data, dict)


def committed_extract_run_dirs(interval_end_dir: LakePath) -> list[LakePath]:
    """
    Committed ``__extract_run_datetime=…`` leaves under one interval end dir.

    Sorted by hive name (same order as load's latest-pick: ``runs[-1]``).
    """
    if not interval_end_dir.exists():
        return []
    return sorted(
        (
            p
            for p in interval_end_dir.iterdir()
            if p.is_dir()
            and p.name.startswith("__extract_run_datetime=")
            and is_committed_raw_dir(p)
        ),
        key=lambda p: p.name,
    )


def extract_run_datetime_from_raw(
    manifest: ManifestPayload | dict[str, Any], raw_dir: LakePath
) -> str:
    """Bronze/load identity: manifest extract_run, else hive leaf value."""
    from det.runtime.meta import from_partition_value, to_interval_datetime

    raw = manifest.get("extract_run_datetime")
    if raw is not None and str(raw).strip():
        return to_interval_datetime(str(raw))
    name = getattr(raw_dir, "name", "") or ""
    if name.startswith("__extract_run_datetime="):
        return from_partition_value(name.split("=", 1)[1])
    raise ValueError(
        f"raw partition {raw_dir} has no extract_run_datetime in manifest or path"
    )


def write_manifest(raw_dir: LakePath, payload: ManifestPayload) -> LakePath:
    """
    Publish the extract commit object.

    Local: ``manifest.json.tmp`` then ``os.replace`` onto ``manifest.json``.
    Object storage: a single PUT of ``meta/manifest.json`` is the commit.
    """
    dest = manifest_path(raw_dir)
    dest.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if isinstance(dest, LakeRef) and not dest.is_local:
        dest.write_text(text, encoding="utf-8")
        return dest
    tmp = dest.with_name(dest.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except (OSError, AttributeError, io.UnsupportedOperation):
            pass
    os.replace(tmp, dest)
    return dest


def read_manifest(raw_dir: LakePath) -> ManifestPayload:
    path = manifest_path(raw_dir)
    if not path.exists():
        raise FileNotFoundError(f"No {META_DIR}/{MANIFEST_NAME} in {raw_dir}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid manifest in {path}")
    return cast(ManifestPayload, data)


def stamp_validation_success(
    raw_dir: LakePath,
    *,
    schema_path: str,
    schema_sha256: str,
    row_count: int,
    wire_version: int,
    validated_at: str,
) -> ManifestPayload:
    """
    Merge a validation success receipt into the raw partition manifest.

    Receipt only — callers must invoke this after bronze write succeeds. Missing
    ``validation`` never gates load; absence means not yet proven under a recorded schema.
    """
    payload = read_manifest(raw_dir)
    validation: ManifestValidation = {
        "ok": True,
        "validated_at": validated_at,
        "schema_path": schema_path,
        "schema_sha256": schema_sha256,
        "row_count": int(row_count),
        "wire_version": int(wire_version),
    }
    payload["validation"] = validation
    write_manifest(raw_dir, payload)
    return payload
