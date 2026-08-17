from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from det.destinations.models import (
    bronze_dataset_dir,
    hive_partition_dir,
    lake_root,
    raw_dataset_dir,
)
from det.logging import bound_run_context, get_logger, sanitize_lake_uri, update_run_context
from det.plugins import load_plugins
from det.runtime.config import PipelineConfig, load_pipeline_config, resolve_path
from det.runtime.lake import LakeRef
from det.runtime.layout import LAKE_LAYOUT
from det.runtime.lease import pipeline_lease, refresh_lease
from det.runtime.load_rows import CountingIter, iter_bronze_rows
from det.runtime.manifest import (
    is_committed_raw_dir,
    read_manifest,
    sha256_file,
    stamp_validation_success,
    write_manifest,
)
from det.runtime.meta import (
    data_interval_date,
    format_extract_run_datetime,
    resolve_interval,
    to_partition_value,
)
from det.runtime.receipts import record_attempt, sum_artifact_bytes
from det.runtime.registry import get_ingestion, get_source
from det.sources.base import Interval, merge_source_config
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)


@dataclass
class ExtractResult:
    pipeline: str
    raw_dir: LakeRef
    extract_run_datetime: str
    artifacts: int
    interval_start: str
    interval_end: str


@dataclass
class RunResult:
    pipeline: str
    partition_dir: Path | LakeRef
    rows: int
    data_interval_date: str
    raw_dir: LakeRef | None = None
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
        lock_ttl_sec: int | None = None,
    ) -> ExtractResult:
        extract_ts = extract_run_datetime or format_extract_run_datetime()
        config = self._load_config(pipeline, overrides)
        source = get_source(config.source.type)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)
        interval = Interval(start=start_iso, end=end_iso)
        lake = lake_root(config.destination, self.project_root)

        raw_dir = hive_partition_dir(
            raw_dataset_dir(config, self.project_root),
            interval_start_datetime=start_iso,
            interval_end_datetime=end_iso,
            extract_run_datetime=extract_ts,
        )
        with record_attempt(
            lake,
            pipeline=config.name,
            command="extract",
            interval_start=start_iso,
            interval_end=end_iso,
            extract_run_datetime=extract_ts,
            wire_version=config.wire_version,
            destination=config.destination.type,
        ) as receipt:
            if is_committed_raw_dir(raw_dir):
                raise FileExistsError(
                    f"Committed raw extract already exists at {raw_dir}; "
                    "re-extract with a new __extract_run_datetime "
                    "(do not copy data to publish)"
                )
            if raw_dir.exists():
                # Incomplete prefix (crash before manifest). Delete junk in place —
                # not a publish copy. On object storage this is list+delete of the
                # failed prefix only.
                raw_dir.rmtree()

            data_dir = raw_dir / "data"
            data_dir.mkdir(parents=True, exist_ok=True)

            with pipeline_lease(
                lake,
                pipeline=config.name,
                interval_start=start_iso,
                interval_end=end_iso,
                command="extract",
                ttl_sec=lock_ttl_sec,
            ) as lease:
                with bound_run_context(
                    command="extract",
                    pipeline=config.name,
                    interval_start=start_iso,
                    interval_end=end_iso,
                    extract_run_datetime=extract_ts,
                    destination=config.destination.type,
                    lake=sanitize_lake_uri(str(lake)),
                ):
                    logger.info(
                        "extract starting",
                        source=source.name,
                        raw_dir=str(raw_dir),
                    )
                    try:
                        artifacts = source.extract_to_raw(
                            config=effective, interval=interval, data_dir=data_dir
                        )
                        refresh_lease(lease)
                        logger.info(
                            "writing raw manifest",
                            artifacts=len(artifacts),
                            raw_dir=str(raw_dir),
                        )
                        write_manifest(
                            raw_dir,
                            {
                                "source": source.name,
                                "interval_start": start_iso,
                                "interval_end": end_iso,
                                "extract_run_datetime": extract_ts,
                                "wire_version": config.wire_version,
                                "lake_layout": LAKE_LAYOUT,
                                "artifacts": artifacts,
                            },
                        )
                    except Exception:
                        if not is_committed_raw_dir(raw_dir):
                            raw_dir.rmtree(ignore_errors=True)
                        raise
                    receipt.artifacts = len(artifacts)
                    receipt.raw_bytes = sum_artifact_bytes(artifacts)
                    logger.info(
                        "extract complete",
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
        lock_ttl_sec: int | None = None,
    ) -> RunResult:
        config = self._load_config(pipeline, overrides)
        schema = load_json_schema(resolve_path(self.project_root, config.schema_path))
        source = get_source(config.source.type)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)
        lake = lake_root(config.destination, self.project_root)

        with record_attempt(
            lake,
            pipeline=config.name,
            command="load",
            interval_start=start_iso,
            interval_end=end_iso,
            extract_run_datetime=extract_run_datetime,
            wire_version=config.wire_version,
            destination=config.destination.type,
        ) as receipt:
            with pipeline_lease(
                lake,
                pipeline=config.name,
                interval_start=start_iso,
                interval_end=end_iso,
                command="load",
                ttl_sec=lock_ttl_sec,
            ) as lease:
                with bound_run_context(
                    command="load",
                    pipeline=config.name,
                    interval_start=start_iso,
                    interval_end=end_iso,
                    extract_run_datetime=extract_run_datetime,
                    destination=config.destination.type,
                    lake=sanitize_lake_uri(str(lake)),
                ):
                    logger.info("load starting")
                    raw_dir = self._resolve_raw_dir(
                        config,
                        interval_start=start_iso,
                        interval_end=end_iso,
                        extract_run_datetime=extract_run_datetime,
                    )
                    logger.info("resolved raw partition", raw_dir=str(raw_dir))
                    manifest = read_manifest(raw_dir)
                    extract_ts = str(
                        manifest.get("extract_run_datetime") or extract_run_datetime
                    )
                    receipt.extract_run_datetime = extract_ts
                    update_run_context(extract_run_datetime=extract_ts)
                    refresh_lease(lease)

                    bronze_loaded_at = format_extract_run_datetime()
                    counted = CountingIter(
                        iter_bronze_rows(
                            source.records_from_raw(
                                config=effective, raw_dir=raw_dir, manifest=manifest
                            ),
                            schema=schema,
                            naming=config.bronze.naming,
                            extract_run_datetime=extract_ts,
                            interval_start_datetime=start_iso,
                            interval_end_datetime=end_iso,
                            bronze_loaded_at=bronze_loaded_at,
                            log_every=config.ingestion.chunk_rows,
                        )
                    )

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
                        chunk_rows=config.ingestion.chunk_rows,
                        partition=str(partition),
                    )
                    written = backend.write(
                        counted,
                        config=config,
                        project_root=self.project_root,
                        partition_dir=partition,
                        destination=config.destination,
                        chunk_rows=config.ingestion.chunk_rows,
                    )
                    schema_sha256 = sha256_file(
                        resolve_path(self.project_root, config.schema_path)
                    )
                    stamp_validation_success(
                        raw_dir,
                        schema_path=config.schema_path,
                        schema_sha256=schema_sha256,
                        row_count=counted.n,
                        wire_version=config.wire_version,
                        validated_at=format_extract_run_datetime(),
                    )
                    receipt.rows = counted.n
                    receipt.schema_sha256 = schema_sha256
                    logger.info(
                        "load complete",
                        rows=counted.n,
                        partition=str(written),
                    )
        return RunResult(
            pipeline=config.name,
            partition_dir=written,
            rows=counted.n,
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
        lock_ttl_sec: int | None = None,
    ) -> RunResult:
        extract_ts = format_extract_run_datetime()
        config = self._load_config(pipeline, overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)
        lake = lake_root(config.destination, self.project_root)
        with pipeline_lease(
            lake,
            pipeline=config.name,
            interval_start=start_iso,
            interval_end=end_iso,
            command="run",
            ttl_sec=lock_ttl_sec,
        ):
            extracted = self.extract(
                config,
                interval_start=interval_start,
                interval_end=interval_end,
                overrides=overrides,
                extract_run_datetime=extract_ts,
                lock_ttl_sec=lock_ttl_sec,
            )
            return self.load(
                config,
                interval_start=extracted.interval_start,
                interval_end=extracted.interval_end,
                overrides=overrides,
                extract_run_datetime=extracted.extract_run_datetime,
                lock_ttl_sec=lock_ttl_sec,
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
    ) -> LakeRef:
        base = (
            raw_dataset_dir(config, self.project_root)
            / f"__interval_start_datetime={to_partition_value(interval_start)}"
            / f"__interval_end_datetime={to_partition_value(interval_end)}"
        )
        if extract_run_datetime:
            target = (
                base / f"__extract_run_datetime={to_partition_value(extract_run_datetime)}"
            )
            if not is_committed_raw_dir(target):
                raise FileNotFoundError(
                    f"No committed raw extract at {target} "
                    "(incomplete extract has no meta/manifest.json)"
                )
            return target
        if not base.exists():
            raise FileNotFoundError(f"No raw partitions under {base}")
        runs = sorted(
            (
                p
                for p in base.iterdir()
                if p.is_dir()
                and p.name.startswith("__extract_run_datetime=")
                and is_committed_raw_dir(p)
            ),
            key=lambda p: p.name,
        )
        if not runs:
            raise FileNotFoundError(f"No committed extract runs under {base}")
        return runs[-1]
