from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.destinations.models import (
    bronze_dataset_dir,
    hive_partition_dir,
    raw_dataset_dir,
)
from det.logging import get_logger
from det.plugins import load_plugins
from det.runtime.coerce import CoerceError, coerce_record
from det.runtime.config import PipelineConfig, load_pipeline_config, resolve_path
from det.runtime.manifest import read_manifest, write_manifest
from det.runtime.meta import (
    attach_meta,
    data_interval_date,
    format_extract_run_datetime,
    resolve_interval,
    to_partition_value,
)
from det.runtime.naming import apply_naming
from det.runtime.registry import get_ingestion, get_source
from det.sources.base import Interval, merge_source_config
from det.validation.jsonschema_validator import (
    SchemaValidationError,
    load_json_schema,
    validate_records,
)

logger = get_logger(__name__)


@dataclass
class ExtractResult:
    pipeline: str
    raw_dir: Path
    extract_run_datetime: str
    artifacts: int
    interval_start: str
    interval_end: str


@dataclass
class RunResult:
    pipeline: str
    partition_dir: Path
    rows: int
    data_interval_date: str
    raw_dir: Path | None = None
    extract_run_datetime: str | None = None


class PipelineRunner:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = (project_root or Path.cwd()).resolve()
        load_plugins()

    def extract(
        self,
        pipeline: PipelineConfig | Path | str,
        *,
        interval_start: str,
        interval_end: str | None = None,
        overrides: Sequence[str] | None = None,
        extract_run_datetime: str | None = None,
    ) -> ExtractResult:
        extract_ts = extract_run_datetime or format_extract_run_datetime()
        config = self._load_config(pipeline, overrides)
        source = get_source(config.source.type)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)
        interval = Interval(start=start_iso, end=end_iso)

        raw_dir = hive_partition_dir(
            raw_dataset_dir(config, self.project_root),
            interval_start_datetime=start_iso,
            interval_end_datetime=end_iso,
            extract_run_datetime=extract_ts,
        )
        if raw_dir.exists():
            import shutil

            shutil.rmtree(raw_dir)
        data_dir = raw_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "extract starting",
            pipeline=config.name,
            source=source.name,
            interval_start=start_iso,
            interval_end=end_iso,
            raw_dir=str(raw_dir),
        )
        artifacts = source.extract_to_raw(
            config=effective, interval=interval, data_dir=data_dir
        )
        logger.info("writing raw manifest", artifacts=len(artifacts), raw_dir=str(raw_dir))
        write_manifest(
            raw_dir,
            {
                "source": source.name,
                "interval_start": start_iso,
                "interval_end": end_iso,
                "extract_run_datetime": extract_ts,
                "wire_version": config.wire_version,
                "artifacts": artifacts,
            },
        )
        logger.info(
            "extract complete",
            pipeline=config.name,
            artifacts=len(artifacts),
            raw_dir=str(raw_dir),
        )
        return ExtractResult(
            pipeline=config.name,
            raw_dir=raw_dir,
            extract_run_datetime=extract_ts,
            artifacts=len(artifacts),
            interval_start=start_iso,
            interval_end=end_iso,
        )

    def load(
        self,
        pipeline: PipelineConfig | Path | str,
        *,
        interval_start: str,
        interval_end: str | None = None,
        overrides: Sequence[str] | None = None,
        extract_run_datetime: str | None = None,
    ) -> RunResult:
        config = self._load_config(pipeline, overrides)
        schema = load_json_schema(resolve_path(self.project_root, config.schema_path))
        source = get_source(config.source.type)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)

        logger.info(
            "load starting",
            pipeline=config.name,
            interval_start=start_iso,
            interval_end=end_iso,
            extract_run_datetime=extract_run_datetime,
        )
        raw_dir = self._resolve_raw_dir(
            config,
            interval_start=start_iso,
            interval_end=end_iso,
            extract_run_datetime=extract_run_datetime,
        )
        logger.info("resolved raw partition", raw_dir=str(raw_dir))
        manifest = read_manifest(raw_dir)
        extract_ts = str(manifest.get("extract_run_datetime") or extract_run_datetime)

        logger.info("parsing + naming + coercing rows from raw")
        named_rows: list[tuple[dict[str, Any], str | None]] = []
        for source_row in source.records_from_raw(
            config=effective, raw_dir=raw_dir, manifest=manifest
        ):
            named = apply_naming(source_row.data, config.bronze.naming)
            try:
                typed = coerce_record(named, schema)
            except CoerceError as exc:
                raise SchemaValidationError(str(exc), errors=[str(exc)]) from exc
            named_rows.append((typed, source_row.filename))
            if len(named_rows) == 1 or len(named_rows) % 50_000 == 0:
                logger.info("naming/coerce progress", rows=len(named_rows))

        logger.info("validating rows against schema", rows=len(named_rows))
        validate_records([row for row, _ in named_rows], schema)
        logger.info("attaching meta columns", rows=len(named_rows))
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

        partition = hive_partition_dir(
            bronze_dataset_dir(config, self.project_root),
            interval_start_datetime=start_iso,
            interval_end_datetime=end_iso,
            extract_run_datetime=extract_ts,
        )
        backend = get_ingestion(config.ingestion.library)
        logger.info(
            "writing bronze partition",
            backend=config.ingestion.library,
            rows=len(enriched),
            partition=str(partition),
        )
        written = backend.write(
            enriched,
            config=config,
            project_root=self.project_root,
            partition_dir=partition,
            destination=config.destination,
        )
        logger.info(
            "load complete",
            pipeline=config.name,
            rows=len(enriched),
            partition=str(written),
        )
        return RunResult(
            pipeline=config.name,
            partition_dir=written,
            rows=len(enriched),
            data_interval_date=data_interval_date(start_iso),
            raw_dir=raw_dir,
            extract_run_datetime=extract_ts,
        )

    def run(
        self,
        pipeline: PipelineConfig | Path | str,
        *,
        interval_start: str,
        interval_end: str | None = None,
        overrides: Sequence[str] | None = None,
    ) -> RunResult:
        extract_ts = format_extract_run_datetime()
        extracted = self.extract(
            pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            overrides=overrides,
            extract_run_datetime=extract_ts,
        )
        return self.load(
            pipeline,
            interval_start=extracted.interval_start,
            interval_end=extracted.interval_end,
            overrides=overrides,
            extract_run_datetime=extracted.extract_run_datetime,
        )

    def _load_config(
        self,
        pipeline: PipelineConfig | Path | str,
        overrides: Sequence[str] | None,
    ) -> PipelineConfig:
        if isinstance(pipeline, PipelineConfig):
            return pipeline
        return load_pipeline_config(
            resolve_path(self.project_root, str(pipeline)), overrides=overrides
        )

    def _resolve_raw_dir(
        self,
        config: PipelineConfig,
        *,
        interval_start: str,
        interval_end: str,
        extract_run_datetime: str | None,
    ) -> Path:
        base = (
            raw_dataset_dir(config, self.project_root)
            / f"__interval_start_datetime={to_partition_value(interval_start)}"
            / f"__interval_end_datetime={to_partition_value(interval_end)}"
        )
        if extract_run_datetime:
            return base / f"__extract_run_datetime={to_partition_value(extract_run_datetime)}"
        if not base.exists():
            raise FileNotFoundError(f"No raw partitions under {base}")
        runs = sorted(
            (
                p
                for p in base.iterdir()
                if p.is_dir() and p.name.startswith("__extract_run_datetime=")
            ),
            key=lambda p: p.name,
        )
        if not runs:
            raise FileNotFoundError(f"No extract runs under {base}")
        return runs[-1]
