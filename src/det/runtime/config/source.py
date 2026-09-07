"""Source, validation, and ingestion config models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from det.runtime.ids import validate_canonical_id


class SourceConfig(BaseModel):
    type: str
    overrides: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type")
    @classmethod
    def type_is_canonical(cls, v: str) -> str:
        return validate_canonical_id(v)


class ValidationConfig(BaseModel):
    engine: Literal["jsonschema"] = "jsonschema"


class IngestionConfig(BaseModel):
    # ``det`` is the multi-destination bronze writer. ``thin`` is filesystem-only.
    # ``dlt`` was never a writer — only a removed alias for ``det``.
    library: Literal["det", "thin"] = "det"
    # SQL INSERT batch size and JSONL flush cadence. Coerce/validate stay per-row.
    chunk_rows: int = Field(default=10_000, ge=1)

    @field_validator("library", mode="before")
    @classmethod
    def reject_dlt_writer_alias(cls, v: Any) -> Any:
        if isinstance(v, str) and v.strip().lower() == "dlt":
            raise ValueError(
                "ingestion.library 'dlt' was removed; use 'det' "
                "(dlt HTTP helpers in extract_to_raw remain allowed)"
            )
        return v
