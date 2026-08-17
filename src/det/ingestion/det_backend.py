from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from det.destinations.models import duckdb_connection_path
from det.ingestion.duckdb_writer import write_duckdb_table
from det.ingestion.jsonl import write_jsonl_partition
from det.ingestion.postgres_writer import write_postgres_table
from det.logging import get_logger
from det.runtime.config import DestinationConfig, PipelineConfig, resolve_path
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import LakeRef
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)


class DetBackend:
    """
    Default DET ingestion backend (filesystem / DuckDB / Postgres / Iceberg bronze writers).

    dlt is extraction-only on the source side (RESTClient, retries, resources as
    iterators). This backend never hands rows to ``dlt.pipeline``: no dlt state
    files, no dlt-managed load ids, and no normalizer unnesting or meta-column
    renaming.
    """

    name = "det"

    def write(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        partition_dir: Path | LakeRef,
        destination: DestinationConfig,
        chunk_rows: int | None = None,
    ) -> Path | LakeRef:
        size = config.ingestion.chunk_rows if chunk_rows is None else chunk_rows
        if destination.type == "filesystem":
            return self._write_filesystem(
                records, config=config, partition_dir=partition_dir, chunk_rows=size
            )
        if destination.type == "duckdb":
            return self._write_duckdb(
                records,
                config=config,
                project_root=project_root,
                destination=destination,
                chunk_rows=size,
            )
        if destination.type == "postgres":
            return self._write_postgres(
                records,
                config=config,
                project_root=project_root,
                destination=destination,
                chunk_rows=size,
            )
        if destination.type == "iceberg":
            return self._write_iceberg(
                records,
                config=config,
                project_root=project_root,
                chunk_rows=size,
            )
        raise ValueError(f"Unsupported destination type: {destination.type}")

    def _write_filesystem(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        partition_dir: Path | LakeRef,
        chunk_rows: int,
    ) -> Path | LakeRef:
        write_jsonl_partition(records, partition_dir, chunk_rows=chunk_rows)
        logger.info(
            "filesystem load finished",
            partition=str(partition_dir),
            pipeline=config.name,
        )
        return partition_dir

    def _write_duckdb(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        destination: DestinationConfig,
        chunk_rows: int,
    ) -> Path:
        db_path = duckdb_connection_path(destination, project_root)
        schema, table = sql_names_for_config(config)
        json_schema = load_json_schema(resolve_path(project_root, config.schema_path))
        return write_duckdb_table(
            records,
            connection_path=db_path,
            schema=schema,
            table=table,
            json_schema=json_schema,
            chunk_rows=chunk_rows,
        )

    def _write_postgres(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        destination: DestinationConfig,
        chunk_rows: int,
    ) -> Path:
        if not destination.connection:
            raise ValueError("destination.connection (Postgres DSN) is required")
        schema, table = sql_names_for_config(config)
        json_schema = load_json_schema(resolve_path(project_root, config.schema_path))
        write_postgres_table(
            records,
            dsn=destination.connection,
            schema=schema,
            table=table,
            json_schema=json_schema,
            chunk_rows=chunk_rows,
        )
        # Logical identity only — Path() would mangle DSN schemes (:// → :/).
        return Path("postgres") / schema / table

    def _write_iceberg(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        chunk_rows: int,
    ) -> LakeRef:
        from det.destinations.models import bronze_dataset_dir, lake_root
        from det.ingestion.iceberg_writer import write_iceberg_table

        json_schema = load_json_schema(resolve_path(project_root, config.schema_path))
        schema, table = sql_names_for_config(config)
        written = write_iceberg_table(
            records,
            lake=lake_root(config.destination, project_root),
            table_location=bronze_dataset_dir(config, project_root),
            namespace=schema,
            table=table,
            json_schema=json_schema,
            chunk_rows=chunk_rows,
        )
        logger.info(
            "iceberg load finished",
            table=f"{schema}.{table}",
            location=str(written),
            pipeline=config.name,
        )
        return written


# Deprecated alias — prefer DetBackend / library: det
DltBackend = DetBackend
