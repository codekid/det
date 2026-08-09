from __future__ import annotations

from pathlib import Path

from det.runtime.config import DestinationConfig, MedallionConfig, PipelineConfig
from det.runtime.ids import (
    fs_dataset_parts,
    medallion_prefix,
    parse_canonical_id,
    sql_names_for_config,
    sql_schema_name,
    validate_canonical_id,
)
from det.runtime.meta import to_partition_value


def lake_root(destination: DestinationConfig, project_root: Path) -> Path:
    path = Path(destination.path)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def duckdb_connection_path(destination: DestinationConfig, project_root: Path) -> Path:
    """Resolve destination.connection to an absolute DuckDB file path."""
    if not destination.connection:
        raise ValueError("destination.connection is required for duckdb")
    path = Path(destination.connection)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def duckdb_schema_name(
    destination: DestinationConfig,
    *,
    provider: str | None = None,
    config: PipelineConfig | None = None,
) -> str:
    """
    SQL schema for DuckDB bronze.

    Prefer ``sql_names_for_config(config)``. When only a destination is available,
    ``provider`` is required so the schema is ``{medallion}_{provider}``.
    """
    if config is not None:
        schema, _ = sql_names_for_config(config)
        return schema
    if not provider:
        raise ValueError(
            "duckdb_schema_name requires provider= or config= "
            "(destination.dataset is the medallion prefix only)"
        )
    return sql_schema_name(medallion_prefix(destination.dataset), provider)


def postgres_schema_name(
    destination: DestinationConfig,
    *,
    provider: str | None = None,
    config: PipelineConfig | None = None,
) -> str:
    """SQL schema for Postgres bronze — same rules as duckdb_schema_name."""
    return duckdb_schema_name(destination, provider=provider, config=config)


def _dataset_dir(
    config: PipelineConfig,
    project_root: Path,
    *,
    prefix: str,
    dataset: str | None = None,
) -> Path:
    root = lake_root(config.destination, project_root)
    canonical = validate_canonical_id(dataset) if dataset else config.canonical_id
    return root.joinpath(prefix, *fs_dataset_parts(canonical))


def bronze_dataset_dir(
    config: PipelineConfig,
    project_root: Path,
    *,
    dataset: str | None = None,
) -> Path:
    return _dataset_dir(
        config,
        project_root,
        prefix=config.medallion.bronze_prefix,
        dataset=dataset,
    )


def raw_dataset_dir(
    config: PipelineConfig,
    project_root: Path,
    *,
    dataset: str | None = None,
) -> Path:
    return _dataset_dir(
        config,
        project_root,
        prefix=config.medallion.raw_prefix,
        dataset=dataset,
    )


def hive_partition_dir(
    bronze_dataset: Path,
    *,
    interval_start_datetime: str,
    interval_end_datetime: str,
    extract_run_datetime: str,
) -> Path:
    """
    Hive-style partition path for one landed run:

        __interval_start_datetime=20260801T000000Z/
          __interval_end_datetime=20260802T000000Z/
            __extract_run_datetime=20260806T231344Z/
              data.jsonl
    """
    return (
        bronze_dataset
        / f"__interval_start_datetime={to_partition_value(interval_start_datetime)}"
        / f"__interval_end_datetime={to_partition_value(interval_end_datetime)}"
        / f"__extract_run_datetime={to_partition_value(extract_run_datetime)}"
    )


def ensure_medallion(config: MedallionConfig | None = None) -> MedallionConfig:
    return config or MedallionConfig()


def provider_of(config: PipelineConfig) -> str:
    provider, _ = parse_canonical_id(config.canonical_id)
    return provider
