from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from det.logging import register_secret_value
from det.runtime.config import DestinationConfig, MedallionConfig, PipelineConfig
from det.runtime.ids import (
    fs_dataset_parts,
    medallion_prefix,
    parse_canonical_id,
    sql_names_for_config,
    sql_schema_name,
    validate_canonical_id,
)
from det.runtime.lake import LakeRef, open_lake, pick_lake_spec
from det.runtime.meta import to_partition_value
from det.runtime.secrets import DSN_KEYS, SecretsBackend, resolve_secret

if TYPE_CHECKING:
    from det.runtime.settings import DetSettings


def lake_root(
    destination: DestinationConfig,
    project_root: Path,
    *,
    cli_lake_path: str | None = None,
    settings: DetSettings | None = None,
) -> LakeRef:
    active = settings
    if active is None:
        from det.runtime.settings import get_active_settings

        active = get_active_settings()
    override = cli_lake_path
    settings_lake: str | None = None
    mode = None
    if active is not None:
        if override is None or not str(override).strip():
            override = active.lake_override
        settings_lake = active.lake_path
        mode = active.lake_mode
        project_root = active.project_root
    spec = pick_lake_spec(
        cli_lake_path=override,
        destination_path=destination.path,
        settings_lake_path=settings_lake,
    )
    return open_lake(spec, project_root, lake_mode=mode)


def duckdb_connection_path(destination: DestinationConfig, project_root: Path) -> Path:
    """Resolve destination.connection to an absolute DuckDB file path."""
    if not destination.connection:
        raise ValueError("destination.connection is required for duckdb")
    path = Path(destination.connection)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def postgres_dsn(
    destination: DestinationConfig,
    *,
    backend: SecretsBackend | None = None,
) -> str:
    """
    Resolve the Postgres DSN from ``connection_env`` (preferred) or ``connection``.

    Raises when the declared secret is unset: a run must fail loudly rather than
    fall back to some other database. MCP passes ``backend="env"``.
    """
    name = (destination.connection_env or "").strip()
    if name:
        return resolve_secret(name, keys=DSN_KEYS, backend=backend)
    literal = (destination.connection or "").strip()
    if not literal:
        raise ValueError(
            "destination.connection_env (env var name holding the DSN) is required "
            "for postgres"
        )
    # A DSN in YAML still deserves scrubbing if a driver echoes it in an error.
    register_secret_value(literal)
    return literal


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
    cli_lake_path: str | None = None,
) -> LakeRef:
    root = lake_root(
        config.destination, project_root, cli_lake_path=cli_lake_path
    )
    canonical = validate_canonical_id(dataset) if dataset else config.canonical_id
    out = root / prefix
    for part in fs_dataset_parts(canonical):
        out = out / part
    return out


def bronze_dataset_dir(
    config: PipelineConfig,
    project_root: Path,
    *,
    dataset: str | None = None,
    cli_lake_path: str | None = None,
) -> LakeRef:
    return _dataset_dir(
        config,
        project_root,
        prefix=config.medallion.bronze_prefix,
        dataset=dataset,
        cli_lake_path=cli_lake_path,
    )


def raw_dataset_dir(
    config: PipelineConfig,
    project_root: Path,
    *,
    dataset: str | None = None,
    cli_lake_path: str | None = None,
) -> LakeRef:
    return _dataset_dir(
        config,
        project_root,
        prefix=config.medallion.raw_prefix,
        dataset=dataset,
        cli_lake_path=cli_lake_path,
    )


def hive_partition_dir(
    bronze_dataset: LakeRef,
    *,
    interval_start_datetime: str,
    interval_end_datetime: str,
    extract_run_datetime: str,
) -> LakeRef:
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
