from __future__ import annotations

from collections.abc import Iterable
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


class DetBackend:
    """
    Default DET ingestion backend (filesystem / DuckDB / Postgres bronze writers).

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
        partition_dir: Path,
        destination: DestinationConfig,
        chunk_rows: int | None = None,
    ) -> Path:
        size = config.ingestion.chunk_rows if chunk_rows is None else chunk_rows
        if destination.type == "filesystem":
            return self._write_filesystem(
                records, config=config, partition_dir=partition_dir, chunk_rows=size
            )
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
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        partition_dir: Path,
        chunk_rows: int,
    ) -> Path:
        out = write_jsonl_partition(records, partition_dir, chunk_rows=chunk_rows)
        logger.info(
            "filesystem load finished",
            path=str(out),
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


# Deprecated alias — prefer DetBackend / library: det
DltBackend = DetBackend
