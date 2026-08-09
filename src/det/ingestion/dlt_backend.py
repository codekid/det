from __future__ import annotations

from pathlib import Path
from typing import Any

from det.destinations.models import duckdb_connection_path
from det.ingestion.duckdb_writer import write_duckdb_table
from det.ingestion.jsonl import write_jsonl_partition
from det.ingestion.postgres_writer import write_postgres_table
from det.logging import get_logger
from det.runtime.config import DestinationConfig, PipelineConfig
from det.runtime.ids import sql_names_for_config

logger = get_logger(__name__)


class DltBackend:
    """
    Default ingestion backend.

    Despite the name, this backend does not hand rows to a dlt pipeline. dlt is used
    only for extraction helpers on the source side (RESTClient, retries, resources as
    iterators). Landing bronze is DET's job: no dlt state files, no dlt-managed load
    ids, and no normalizer unnesting or meta-column renaming.
    """

    name = "dlt"

    def write(
        self,
        records: list[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        partition_dir: Path,
        destination: DestinationConfig,
    ) -> Path:
        if destination.type == "filesystem":
            return self._write_filesystem(records, config=config, partition_dir=partition_dir)
        if destination.type == "duckdb":
            return self._write_duckdb(
                records, config=config, project_root=project_root, destination=destination
            )
        if destination.type == "postgres":
            return self._write_postgres(
                records, config=config, destination=destination
            )
        raise ValueError(f"Unsupported destination type: {destination.type}")

    def _write_filesystem(
        self,
        records: list[dict[str, Any]],
        *,
        config: PipelineConfig,
        partition_dir: Path,
    ) -> Path:
        out = write_jsonl_partition(records, partition_dir)
        logger.info(
            "filesystem load finished",
            path=str(out),
            rows=len(records),
            pipeline=config.name,
        )
        return partition_dir

    def _write_duckdb(
        self,
        records: list[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        destination: DestinationConfig,
    ) -> Path:
        db_path = duckdb_connection_path(destination, project_root)
        schema, table = sql_names_for_config(config)
        return write_duckdb_table(
            records,
            connection_path=db_path,
            schema=schema,
            table=table,
        )

    def _write_postgres(
        self,
        records: list[dict[str, Any]],
        *,
        config: PipelineConfig,
        destination: DestinationConfig,
    ) -> Path:
        if not destination.connection:
            raise ValueError("destination.connection (Postgres DSN) is required")
        schema, table = sql_names_for_config(config)
        write_postgres_table(
            records,
            dsn=destination.connection,
            schema=schema,
            table=table,
        )
        # Logical identity only — Path() would mangle DSN schemes (:// → :/).
        return Path("postgres") / schema / table
