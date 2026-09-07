"""PipelineConfig composition root."""

from __future__ import annotations

from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)

from det.runtime.config.dbt import DbtConfig
from det.runtime.config.destination import DestinationConfig, MedallionConfig
from det.runtime.config.lease import LeaseConfig
from det.runtime.config.source import (
    IngestionConfig,
    SourceConfig,
    ValidationConfig,
)
from det.runtime.ids import (
    default_schema_path,
    fs_dataset_relpath,
    lake_dataset_id,
    validate_canonical_id,
)
from det.runtime.naming import BronzeConfig
from det.runtime.slo import SloConfig


class PipelineConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    source: SourceConfig
    # YAML key remains `schema:`; omit to use default_schema_path(name).
    schema_path: str = Field(alias="schema")
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    destination: DestinationConfig = Field(default_factory=DestinationConfig)
    medallion: MedallionConfig = Field(default_factory=MedallionConfig)
    bronze: BronzeConfig = Field(default_factory=BronzeConfig)
    dbt: DbtConfig = Field(default_factory=DbtConfig)
    lease: LeaseConfig | None = None
    # Opt-in fleet SLOs (dbt tests). Omit → pipeline is not in ops_slo_expected.
    slo: SloConfig | None = None
    # Rejected if set: lake ids are always ``{name}_v{wire_version}``. Use
    # ``wire_version`` (and ``det migrate``) for cutovers.
    dataset: str | None = None
    # Wire-era stamp and lake id suffix. Lake paths/SQL use ``{name}_vN`` always
    # (including ``_v1``). Bump when the extract payload shape changes
    # incompatibly; rebuild older raw with ``det migrate --from-raw …_vN``.
    wire_version: int = 1
    # Programmatic lake id override (e.g. ``det migrate --to-bronze``). Not YAML.
    _lake_id: str | None = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def fill_default_schema_path(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        name = data.get("name")
        schema = data.get("schema")
        if schema is None or schema == "":
            schema = data.get("schema_path")
        if (schema is None or schema == "") and isinstance(name, str) and name.strip():
            data = {**data, "schema": default_schema_path(name)}
        return data

    @field_validator("name")
    @classmethod
    def name_is_canonical(cls, v: str) -> str:
        return validate_canonical_id(v)

    @field_validator("schema_path")
    @classmethod
    def schema_nonempty(cls, v: str) -> str:
        if not str(v).strip():
            raise ValueError("schema path must be non-empty")
        return v

    @model_validator(mode="after")
    def name_matches_source(self) -> PipelineConfig:
        if self.name != self.source.type:
            raise ValueError(
                f"pipeline name {self.name!r} must equal source.type "
                f"{self.source.type!r}"
            )
        if self.dataset is not None:
            raise ValueError(
                "pipeline top-level 'dataset:' is no longer supported; lake ids are "
                f"derived as {{name}}_v{{wire_version}} "
                f"(e.g. {self.name}_v{self.wire_version}). "
                "Bump wire_version for a cutover."
            )
        if self.wire_version < 1:
            raise ValueError("wire_version must be a positive integer (>= 1)")
        return self

    @property
    def canonical_id(self) -> str:
        """Lake / SQL dataset id: ``{name}_v{wire_version}`` (or migrate override)."""
        if self._lake_id is not None:
            return validate_canonical_id(self._lake_id)
        return lake_dataset_id(self.name, self.wire_version)

    def bronze_dataset(self) -> str:
        """Lake dataset id (``provider.source_vN``). Prefer path helpers."""
        return self.canonical_id

    def fs_dataset_relpath(self) -> str:
        return fs_dataset_relpath(self.canonical_id)
