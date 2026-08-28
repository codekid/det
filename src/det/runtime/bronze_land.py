"""Shared bronze landing: raw partition → typed rows → backend write → validation stamp."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.destinations.models import bronze_dataset_dir, hive_partition_dir
from det.errors import reraise_as_plugin
from det.ingestion.base import IngestionBackend
from det.logging import get_logger
from det.runtime.config import PipelineConfig
from det.runtime.lake import LakeRef
from det.runtime.lease import Lease, LeaseStore, assert_lease_held
from det.runtime.lease.dataset_lock import DatasetLockHandle, assert_dataset_lock_held
from det.runtime.load_rows import CountingIter, iter_bronze_rows
from det.runtime.manifest import LakePath, sha256_file, stamp_validation_success
from det.runtime.manifest_types import ManifestPayload
from det.runtime.meta import format_extract_run_datetime
from det.sources.base import SourcePlugin

logger = get_logger(__name__)


@dataclass(frozen=True)
class BronzeLandParams:
    raw_dir: LakePath
    manifest: ManifestPayload
    effective_config: Any
    bronze_config: PipelineConfig
    schema: dict[str, Any]
    schema_path: str
    schema_resolved: Path
    extract_ts: str
    start_iso: str
    end_iso: str
    bronze_dataset: str | None = None
    mapper: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    plugin_name: str = ""
    command: str = "load"


@dataclass
class BronzeLandResult:
    partition_dir: Path | LakeRef
    rows: int
    schema_sha256: str


def land_bronze_partition(
    *,
    source: SourcePlugin,
    backend: IngestionBackend,
    project_root: Path,
    params: BronzeLandParams,
    pipeline_lease: Lease | None,
    dataset_lock: DatasetLockHandle | None,
    lease_store: LeaseStore | None = None,
    on_progress: Callable[[], None] | None = None,
    on_chunk: Callable[[], None] | None = None,
) -> BronzeLandResult:
    """Coerce, validate, write one bronze hive partition, and stamp raw validation."""
    bronze_loaded_at = format_extract_run_datetime()
    chunk_rows = params.bronze_config.ingestion.chunk_rows

    try:
        progress: Callable[[int], None] | None
        if on_progress is None:
            progress = None
        else:
            def progress(_n: int) -> None:
                on_progress()

        counted = CountingIter(
            iter_bronze_rows(
                source.records_from_raw(
                    config=params.effective_config,
                    raw_dir=params.raw_dir,
                    manifest=params.manifest,
                ),
                schema=params.schema,
                naming=params.bronze_config.bronze.naming,
                extract_run_datetime=params.extract_ts,
                interval_start_datetime=params.start_iso,
                interval_end_datetime=params.end_iso,
                bronze_loaded_at=bronze_loaded_at,
                mapper=params.mapper,
                log_every=chunk_rows,
                on_progress=progress,
            )
        )

        partition = hive_partition_dir(
            bronze_dataset_dir(
                params.bronze_config,
                project_root,
                dataset=params.bronze_dataset,
            ),
            interval_start_datetime=params.start_iso,
            interval_end_datetime=params.end_iso,
            extract_run_datetime=params.extract_ts,
        )
        logger.info(
            "writing bronze partition",
            command=params.command,
            backend=params.bronze_config.ingestion.library,
            chunk_rows=chunk_rows,
            partition=str(partition),
        )
        assert_lease_held(pipeline_lease, store=lease_store)
        assert_dataset_lock_held(dataset_lock)
        if on_progress is not None:
            on_progress()
        written = backend.write(
            counted,
            config=params.bronze_config,
            project_root=project_root,
            partition_dir=partition,
            destination=params.bronze_config.destination,
            chunk_rows=chunk_rows,
            run_identity=(params.start_iso, params.end_iso, params.extract_ts),
            on_chunk=on_chunk,
        )
    except Exception as exc:
        if params.plugin_name:
            reraise_as_plugin(exc, plugin=params.plugin_name, action=params.command)
        raise

    schema_sha256 = sha256_file(params.schema_resolved)
    assert_lease_held(pipeline_lease, store=lease_store)
    stamp_validation_success(
        params.raw_dir,
        schema_path=params.schema_path,
        schema_sha256=schema_sha256,
        row_count=counted.n,
        wire_version=params.bronze_config.wire_version,
        validated_at=format_extract_run_datetime(),
    )
    return BronzeLandResult(
        partition_dir=written,
        rows=counted.n,
        schema_sha256=schema_sha256,
    )
