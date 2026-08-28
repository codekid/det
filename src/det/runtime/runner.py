from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from det.destinations.models import (
    hive_partition_dir,
    lake_root,
    raw_dataset_dir,
)
from det.errors import DetConflictError, DetNotFoundError, reraise_as_plugin
from det.logging import bound_run_context, get_logger, sanitize_lake_uri, update_run_context
from det.plugins import load_plugins
from det.runtime.bronze_land import BronzeLandParams, land_bronze_partition
from det.runtime.config import PipelineConfig, load_pipeline, resolve_path
from det.runtime.dlt_hygiene import check_raw_hygiene
from det.runtime.lake import LakeRef
from det.runtime.layout import LAKE_LAYOUT
from det.runtime.lease import (
    LeaseFencedError,
    assert_lease_held,
    pipeline_lease,
    refresh_bronze_locks,
    refresh_lease,
    resolve_lease_options,
)
from det.runtime.lease.dataset_lock import dataset_shared_lock
from det.runtime.manifest import (
    LakePath,
    is_committed_raw_dir,
    read_manifest,
    write_manifest,
)
from det.runtime.manifest_types import ManifestPayload
from det.runtime.meta import (
    data_interval_date,
    format_extract_run_datetime,
    resolve_interval,
    to_partition_value,
)
from det.runtime.receipts import record_attempt, sum_artifact_bytes
from det.runtime.registry import get_ingestion, get_source
from det.runtime.settings import DetSettings, use_settings
from det.sources.base import Interval, merge_source_config
from det.validation.jsonschema_validator import load_json_schema

logger = get_logger(__name__)


@dataclass
class ExtractResult:
    pipeline: str
    raw_dir: LakePath
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
    raw_dir: LakePath | None = None
    extract_run_datetime: str | None = None


class PipelineRunner:
    def __init__(
        self,
        project_root: Path | None = None,
        *,
        settings: DetSettings | None = None,
    ) -> None:
        if settings is not None and project_root is not None:
            raise ValueError("pass settings= or project_root=, not both")
        if settings is None:
            settings = DetSettings.from_env(project_root=project_root)
        self.settings = settings
        self.project_root = settings.project_root
        load_plugins()

    def _lake(self, destination):
        return lake_root(destination, self.project_root, settings=self.settings)

    def _lease_kwargs(
        self, lock_ttl_sec: int | None = None, config: PipelineConfig | None = None
    ) -> dict:
        options = resolve_lease_options(
            settings=self.settings,
            pipeline=config,
            ttl_sec=self.settings.effective_lock_ttl(lock_ttl_sec),
            owner=self.settings.lock_owner,
            enabled=self.settings.locks_enabled,
        )
        return {
            "ttl_sec": self.settings.effective_lock_ttl(lock_ttl_sec),
            "owner": self.settings.lock_owner,
            "enabled": self.settings.locks_enabled,
            "options": options,
            "resolve_secret": self.settings.resolve_secret,
        }
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
        with use_settings(self.settings):
            return self._extract(
                pipeline,
                interval_start=interval_start,
                interval_end=interval_end,
                overrides=overrides,
                extract_run_datetime=extract_run_datetime,
                lock_ttl_sec=lock_ttl_sec,
            )

    def _extract(
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
        source = get_source(config.source.type, project_root=self.project_root)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)
        interval = Interval(start=start_iso, end=end_iso)
        lake = self._lake(config.destination)

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
                raise DetConflictError(
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
                **self._lease_kwargs(lock_ttl_sec, config),
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
                        try:
                            artifacts = source.extract_to_raw(
                                config=effective, interval=interval, data_dir=data_dir
                            )
                        except Exception as exc:
                            reraise_as_plugin(
                                exc, plugin=source.name, action="extract_to_raw"
                            )
                        check_raw_hygiene(raw_dir, artifacts)
                        refresh_lease(lease, store=None if lease is None else lease.store)
                        logger.info(
                            "writing raw manifest",
                            artifacts=len(artifacts),
                            raw_dir=str(raw_dir),
                        )
                        manifest_payload = cast(
                            ManifestPayload,
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
                        assert_lease_held(
                            lease, store=None if lease is None else lease.store
                        )
                        write_manifest(raw_dir, manifest_payload)
                    except LeaseFencedError:
                        # Bytes may still be useful for ops; do not scrub the
                        # prefix when publish was refused by the lease fence.
                        raise
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
        with use_settings(self.settings):
            return self._load(
                pipeline,
                interval_start=interval_start,
                interval_end=interval_end,
                overrides=overrides,
                extract_run_datetime=extract_run_datetime,
                lock_ttl_sec=lock_ttl_sec,
            )

    def _load(
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
        source = get_source(config.source.type, project_root=self.project_root)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        start_iso, end_iso = resolve_interval(interval_start, interval_end)
        lake = self._lake(config.destination)

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
                **self._lease_kwargs(lock_ttl_sec, config),
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
                    check_raw_hygiene(raw_dir, manifest.get("artifacts"))
                    extract_ts = str(
                        manifest.get("extract_run_datetime") or extract_run_datetime
                    )
                    receipt.extract_run_datetime = extract_ts
                    update_run_context(extract_run_datetime=extract_ts)
                    refresh_lease(lease, store=None if lease is None else lease.store)

                    schema_resolved = resolve_path(self.project_root, config.schema_path)
                    backend = get_ingestion(config.ingestion.library)
                    with dataset_shared_lock(
                        lake,
                        config.canonical_id,
                        command="load",
                        **self._lease_kwargs(lock_ttl_sec, config),
                    ) as dataset_lock:
                        lease_store = None if lease is None else lease.store

                        def refresh_locks() -> None:
                            refresh_bronze_locks(
                                pipeline_lease=lease,
                                dataset_lock=dataset_lock,
                                lease_store=lease_store,
                            )

                        landed = land_bronze_partition(
                            source=source,
                            backend=backend,
                            project_root=self.project_root,
                            params=BronzeLandParams(
                                raw_dir=raw_dir,
                                manifest=manifest,
                                effective_config=effective,
                                bronze_config=config,
                                schema=schema,
                                schema_path=config.schema_path,
                                schema_resolved=schema_resolved,
                                extract_ts=extract_ts,
                                start_iso=start_iso,
                                end_iso=end_iso,
                                plugin_name=source.name,
                                command="load",
                            ),
                            pipeline_lease=lease,
                            dataset_lock=dataset_lock,
                            lease_store=lease_store,
                            on_progress=refresh_locks,
                            on_chunk=refresh_locks,
                        )
                    receipt.rows = landed.rows
                    receipt.schema_sha256 = landed.schema_sha256
                    logger.info(
                        "load complete",
                        rows=landed.rows,
                        partition=str(landed.partition_dir),
                    )
        return RunResult(
            pipeline=config.name,
            partition_dir=landed.partition_dir,
            rows=landed.rows,
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
        with use_settings(self.settings):
            extract_ts = format_extract_run_datetime()
            config = self._load_config(pipeline, overrides)
            start_iso, end_iso = resolve_interval(interval_start, interval_end)
            lake = self._lake(config.destination)
            with pipeline_lease(
                lake,
                pipeline=config.name,
                interval_start=start_iso,
                interval_end=end_iso,
                command="run",
                **self._lease_kwargs(lock_ttl_sec, config),
            ):
                extracted = self._extract(
                    config,
                    interval_start=interval_start,
                    interval_end=interval_end,
                    overrides=overrides,
                    extract_run_datetime=extract_ts,
                    lock_ttl_sec=lock_ttl_sec,
                )
                return self._load(
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
        return load_pipeline(
            pipeline, project_root=self.project_root, overrides=overrides
        )

    def _resolve_raw_dir(
        self,
        config: PipelineConfig,
        *,
        interval_start: str,
        interval_end: str,
        extract_run_datetime: str | None,
    ) -> LakePath:
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
                raise DetNotFoundError(
                    f"No committed raw extract at {target} "
                    "(incomplete extract has no meta/manifest.json)"
                )
            return target
        if not base.exists():
            raise DetNotFoundError(f"No raw partitions under {base}")
        from det.runtime.manifest import committed_extract_run_dirs

        runs = committed_extract_run_dirs(base)
        if not runs:
            raise DetNotFoundError(f"No committed extract runs under {base}")
        return runs[-1]
