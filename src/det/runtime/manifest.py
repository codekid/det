from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

META_DIR = "meta"
MANIFEST_NAME = "manifest.json"


def meta_dir(raw_dir: Path) -> Path:
    return raw_dir / META_DIR


def manifest_path(raw_dir: Path) -> Path:
    return meta_dir(raw_dir) / MANIFEST_NAME


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(raw_dir: Path, payload: dict[str, Any]) -> Path:
    dest = manifest_path(raw_dir)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return dest


def read_manifest(raw_dir: Path) -> dict[str, Any]:
    path = manifest_path(raw_dir)
    if not path.exists():
        raise FileNotFoundError(f"No {META_DIR}/{MANIFEST_NAME} in {raw_dir}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid manifest in {path}")
    return data


def stamp_validation_success(
    raw_dir: Path,
    *,
    schema_path: str,
    schema_sha256: str,
    row_count: int,
    wire_version: int,
    validated_at: str,
) -> dict[str, Any]:
    """
    Merge a validation success receipt into the raw partition manifest.

    Receipt only — callers must invoke this after bronze write succeeds. Missing
    ``validation`` never gates load; absence means not yet proven under a recorded schema.
    """
    payload = read_manifest(raw_dir)
    payload["validation"] = {
        "ok": True,
        "validated_at": validated_at,
        "schema_path": schema_path,
        "schema_sha256": schema_sha256,
        "row_count": int(row_count),
        "wire_version": int(wire_version),
    }
    write_manifest(raw_dir, payload)
    return payload
