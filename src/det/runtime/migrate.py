from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.destinations.models import bronze_dataset_dir, hive_partition_dir, raw_dataset_dir
from det.logging import get_logger
from det.plugins import load_plugins
from det.runtime.coerce import CoerceError, coerce_record
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
    ValidationConfig,
    load_pipeline_config,
    resolve_path,
)
from det.runtime.ids import validate_canonical_id
from det.runtime.manifest import read_manifest
from det.runtime.meta import (
    attach_meta,
    format_extract_run_datetime,
    resolve_interval,
    to_partition_value,
)
from det.runtime.naming import apply_naming
from det.runtime.registry import get_ingestion, get_mapper, get_source
from det.sources.base import merge_source_config
from det.validation.jsonschema_validator import (
    SchemaValidationError,
    load_json_schema,
    validate_records,
)

logger = get_logger(__name__)


@dataclass
class MigrateResult:
    from_raw: str
    to_bronze: str
    partitions: int
    rows: int


def _raw_partitions_in_window(raw_dataset: Path, start: str, end: str) -> list[Path]:
    """
    Leaf raw dirs (…/__extract_run_datetime=…) whose interval start is in [start, end).
    """
    start_key, end_key = to_partition_value(start), to_partition_value(end)
    parts: list[Path] = []
    if not raw_dataset.exists():
        return parts
    for start_dir in sorted(raw_dataset.iterdir()):
        if not start_dir.is_dir() or not start_dir.name.startswith(
            "__interval_start_datetime="
        ):
            continue
        key = start_dir.name.split("=", 1)[1]
        if not (start_key <= key < end_key):
            continue
        for end_dir in sorted(start_dir.iterdir()):
            if not end_dir.is_dir() or not end_dir.name.startswith(
                "__interval_end_datetime="
            ):
                continue
            for run_dir in sorted(end_dir.iterdir()):
                if run_dir.is_dir() and run_dir.name.startswith("__extract_run_datetime="):
                    parts.append(run_dir)
    return parts


class BronzeMigrator:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = (project_root or Path.cwd()).resolve()
        load_plugins()

    def migrate(
        self,
        *,
        pipeline: Path | str | PipelineConfig,
        to_bronze: str,
        schema_path: Path | str,
        mapper_name: str,
        interval_start: str,
        interval_end: str | None = None,
        from_raw: str | None = None,
        lake_path: str | None = None,
        bronze_prefix: str | None = None,
        raw_prefix: str | None = None,
        ingestion_library: str = "thin",
        overrides: list[str] | None = None,
    ) -> MigrateResult:
        """
        Rebuild bronze from raw wire using the pipeline's source parser + naming,
        then apply a mapper and the target schema.
        """
        extract_ts = format_extract_run_datetime()
        config = (
            pipeline
            if isinstance(pipeline, PipelineConfig)
            else load_pipeline_config(
                resolve_path(self.project_root, str(pipeline)), overrides=overrides
            )
        )
        if lake_path is not None:
            config.destination = DestinationConfig(
                type=config.destination.type,
                path=lake_path,
                dataset=config.destination.dataset,
                connection=config.destination.connection,
            )
        if bronze_prefix is not None or raw_prefix is not None:
            config.medallion = MedallionConfig(
                bronze_prefix=bronze_prefix or config.medallion.bronze_prefix,
                raw_prefix=raw_prefix or config.medallion.raw_prefix,
            )

        schema = load_json_schema(
            Path(schema_path)
            if Path(schema_path).is_absolute()
            else (self.project_root / schema_path).resolve()
        )
        mapper = get_mapper(mapper_name)
        source = get_source(config.source.type)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        window_start, window_end = resolve_interval(interval_start, interval_end)

        raw_name = validate_canonical_id(from_raw or config.bronze_dataset())
        to_bronze = validate_canonical_id(to_bronze)
        raw_dataset = raw_dataset_dir(
            config, self.project_root, dataset=raw_name
        )
        source_parts = _raw_partitions_in_window(raw_dataset, window_start, window_end)

        # Keep name == source.type; dataset overrides the target lake/SQL identity.
        to_config = PipelineConfig(
            name=config.name,
            source=SourceConfig(type=config.source.type, overrides=config.source.overrides),
            schema_path=str(schema_path),
            validation=ValidationConfig(),
            ingestion=IngestionConfig(library=ingestion_library),  # type: ignore[arg-type]
            destination=config.destination,
            medallion=config.medallion,
            bronze=config.bronze,
            dataset=to_bronze,
        )
        backend = get_ingestion(ingestion_library)
        total_rows = 0
        written = 0

        for raw_dir in source_parts:
            manifest = read_manifest(raw_dir)
            named_rows: list[tuple[dict[str, Any], str | None]] = []
            for source_row in source.records_from_raw(
                config=effective, raw_dir=raw_dir, manifest=manifest
            ):
                named = apply_naming(source_row.data, config.bronze.naming)
                mapped = mapper(named)
                try:
                    typed = coerce_record(mapped, schema)
                except CoerceError as exc:
                    raise SchemaValidationError(str(exc), errors=[str(exc)]) from exc
                named_rows.append((typed, source_row.filename))
            if not named_rows:
                continue
            validate_records([row for row, _ in named_rows], schema)

            # Preserve the source interval from the manifest when present.
            start_iso = str(manifest.get("interval_start") or window_start)
            end_iso = str(manifest.get("interval_end") or window_end)
            start_iso, end_iso = resolve_interval(start_iso, end_iso)
            enriched = [
                attach_meta(
                    row,
                    filename=filename,
                    extract_run_datetime=extract_ts,
                    interval_start_datetime=start_iso,
                    interval_end_datetime=end_iso,
                )
                for row, filename in named_rows
            ]
            out_part = hive_partition_dir(
                bronze_dataset_dir(to_config, self.project_root, dataset=to_bronze),
                interval_start_datetime=start_iso,
                interval_end_datetime=end_iso,
                extract_run_datetime=extract_ts,
            )
            backend.write(
                enriched,
                config=to_config,
                project_root=self.project_root,
                partition_dir=out_part,
                destination=to_config.destination,
            )
            total_rows += len(enriched)
            written += 1
            logger.info("migrated raw partition", raw_dir=str(raw_dir), rows=len(enriched))

        return MigrateResult(
            from_raw=raw_name,
            to_bronze=to_bronze,
            partitions=written,
            rows=total_rows,
        )
