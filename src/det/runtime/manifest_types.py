"""TypedDict contracts for raw ``meta/manifest.json`` (publication commit object)."""

from __future__ import annotations

from typing import TypedDict


class ManifestArtifact(TypedDict, total=False):
    """One data/ artifact descriptor stamped on the extract manifest."""

    path: str
    origin: str
    sha256: str
    bytes: int
    format: str
    content_encoding: str
    format_check: str


class ManifestValidation(TypedDict):
    """Post-load success receipt merged into the manifest (optional on raw)."""

    ok: bool
    validated_at: str
    schema_path: str
    schema_sha256: str
    row_count: int
    wire_version: int


class ManifestPayload(TypedDict, total=False):
    """Raw extract commit object under ``meta/manifest.json``."""

    source: str
    interval_start: str
    interval_end: str
    extract_run_datetime: str
    wire_version: int
    lake_layout: int
    artifacts: list[ManifestArtifact]
    validation: ManifestValidation
