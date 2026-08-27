from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from det.destinations.models import (
    bronze_dataset_dir,
    hive_partition_dir,
    lake_root,
    raw_dataset_dir,
)
from det.logging import bound_run_context, get_logger, sanitize_lake_uri
from det.plugins import load_plugins
from det.runtime.coerce import CoerceError, coerce_record
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
    ValidationConfig,
    load_pipeline,
)
from det.runtime.ids import validate_canonical_id
from det.runtime.lake import LakeRef
from det.runtime.lake import relpath as lake_relpath
from det.runtime.lease import pipeline_lease
from det.runtime.load_rows import CountingIter, iter_bronze_rows
from det.runtime.manifest import (
    committed_extract_run_dirs,
    extract_run_datetime_from_raw,
    read_manifest,
    sha256_file,
    stamp_validation_success,
)
from det.runtime.meta import (
    format_extract_run_datetime,
    resolve_interval,
    to_partition_value,
)
from det.runtime.naming import apply_naming
from det.runtime.registry import get_ingestion, get_mapper, get_source
from det.runtime.settings import DetSettings, use_settings
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


@dataclass
class PartitionPlan:
    raw_path: str
    would_write_bronze_path: str | None
    interval_start: str
    interval_end: str
    extract_run_datetime: str
    rows: int
    ok: bool
    truncated: bool = False
    errors: list[str] = field(default_factory=list)
    wire_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MigratePlan:
    """Dry-run migrate preview — no bronze writes."""

    dry_run: bool
    from_raw: str
    to_bronze: str
    mapper_name: str
    schema_path: str
    extract_run_datetime: str
    partitions: list[PartitionPlan] = field(default_factory=list)
    partitions_planned: int = 0
    rows_checked: int = 0
    wire_version_filter: int | None = None
    recreate_iceberg: bool = False
    will_drop_table: bool = False
    table_location: str | None = None
    yaml_partition: str | None = None
    recreate_warning: str | None = None
    all_raw: bool = False
    all_raw_runs: bool = False

    @property
    def ok(self) -> bool:
        return all(p.ok for p in self.partitions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "ok": self.ok,
            "from_raw": self.from_raw,
            "to_bronze": self.to_bronze,
            "mapper_name": self.mapper_name,
            "schema_path": self.schema_path,
            "extract_run_datetime": self.extract_run_datetime,
            "wire_version_filter": self.wire_version_filter,
            "partitions_planned": self.partitions_planned,
            "rows_checked": self.rows_checked,
            "partitions": [p.to_dict() for p in self.partitions],
            "recreate_iceberg": self.recreate_iceberg,
            "will_drop_table": self.will_drop_table,
            "table_location": self.table_location,
            "yaml_partition": self.yaml_partition,
            "recreate_warning": self.recreate_warning,
            "all_raw": self.all_raw,
            "all_raw_runs": self.all_raw_runs,
        }


def manifest_wire_version(manifest: Mapping[str, Any]) -> int:
    """Legacy manifests without the field are treated as wire_version 1."""
    raw = manifest.get("wire_version", 1)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 1
    return value if value >= 1 else 1


def _raw_partitions_for_migrate(
    raw_dataset: Path | LakeRef,
    *,
    start: str | None,
    end: str | None,
    all_raw: bool,
    all_raw_runs: bool,
) -> list[Path | LakeRef]:
    """
    Committed raw leaves to migrate.

    Default (load parity): latest committed ``__extract_run_datetime`` per
    interval. With ``all_raw_runs``, every committed sibling. With ``all_raw``,
    every interval under the dataset (no start-key filter); otherwise only
    intervals whose start is in ``[start, end)``.
    """
    if not all_raw:
        if start is None or end is None:
            raise ValueError("interval start/end required unless all_raw=True")
        start_key, end_key = to_partition_value(start), to_partition_value(end)
    else:
        start_key = end_key = None

    parts: list[Path | LakeRef] = []
    if not raw_dataset.exists():
        return parts
    for start_dir in sorted(raw_dataset.iterdir()):
        if not start_dir.is_dir() or not start_dir.name.startswith(
            "__interval_start_datetime="
        ):
            continue
        key = start_dir.name.split("=", 1)[1]
        if start_key is not None and end_key is not None:
            if not (start_key <= key < end_key):
                continue
        for end_dir in sorted(start_dir.iterdir()):
            if not end_dir.is_dir() or not end_dir.name.startswith(
                "__interval_end_datetime="
            ):
                continue
            runs = committed_extract_run_dirs(end_dir)
            if not runs:
                continue
            if all_raw_runs:
                parts.extend(runs)
            else:
                parts.append(runs[-1])
    return parts


def _rel(path: Path | LakeRef, root: Path) -> str:
    return lake_relpath(path, root)


class BronzeMigrator:
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

    def migrate(
        self,
        *,
        pipeline: Path | str | PipelineConfig,
        to_bronze: str,
        schema_path: Path | str,
        mapper_name: str,
        interval_start: str | None = None,
        interval_end: str | None = None,
        from_raw: str | None = None,
        lake_path: str | None = None,
        bronze_prefix: str | None = None,
        raw_prefix: str | None = None,
        ingestion_library: str = "thin",
        overrides: list[str] | None = None,
        dry_run: bool = False,
        validate_limit: int | None = None,
        wire_version: int | None = None,
        lock_ttl_sec: int | None = None,
        recreate_iceberg: bool = False,
        all_raw: bool = False,
        all_raw_runs: bool = False,
    ) -> MigrateResult | MigratePlan:
        """
        Rebuild bronze from raw wire using the pipeline's source parser + naming,
        then apply a mapper and the target schema.

        When ``dry_run=True``, parse/map/validate only and return a ``MigratePlan``
        (never calls the bronze writer). ``validate_limit`` caps rows checked per
        partition (dry-run only).

        ``recreate_iceberg`` purges the target Iceberg bronze table location before
        writing (full table drop). Default raw selection matches load: latest
        committed extract per interval; bronze ``__extract_run_datetime`` comes
        from each raw manifest. ``all_raw`` (with recreate) covers every interval;
        ``all_raw_runs`` rematerializes every raw sibling as its own bronze run.
        """
        if not dry_run and validate_limit is not None:
            raise ValueError("validate_limit is only valid with dry_run=True")
        if wire_version is not None and wire_version < 1:
            raise ValueError("wire_version filter must be a positive integer (>= 1)")
        if all_raw:
            if not recreate_iceberg:
                raise ValueError("--all-raw requires --recreate-iceberg")
            if interval_start is not None or interval_end is not None:
                raise ValueError("--all-raw cannot be combined with -s/-e")
        elif interval_start is None:
            raise ValueError("-s/--interval-start is required unless --all-raw")

        job_ts = format_extract_run_datetime()
        config = load_pipeline(
            pipeline, project_root=self.project_root, overrides=overrides
        )
        if lake_path is not None:
            config.destination = DestinationConfig(
                type=config.destination.type,
                path=lake_path,
                dataset=config.destination.dataset,
                connection=config.destination.connection,
                connection_env=config.destination.connection_env,
                partition=config.destination.partition,
            )
        if bronze_prefix is not None or raw_prefix is not None:
            config.medallion = MedallionConfig(
                bronze_prefix=bronze_prefix or config.medallion.bronze_prefix,
                raw_prefix=raw_prefix or config.medallion.raw_prefix,
            )

        if recreate_iceberg and config.destination.type != "iceberg":
            raise ValueError(
                "--recreate-iceberg requires destination.type iceberg, "
                f"got {config.destination.type!r}"
            )

        schema_resolved = (
            Path(schema_path)
            if Path(schema_path).is_absolute()
            else (self.project_root / schema_path).resolve()
        )
        schema = load_json_schema(schema_resolved)
        schema_rel = _rel(schema_resolved, self.project_root)
        mapper = get_mapper(mapper_name)
        source = get_source(config.source.type)
        effective = merge_source_config(source.defaults(), config.source.overrides)
        if all_raw:
            window_start = window_end = None
        else:
            if interval_start is None:
                raise ValueError("-s/--interval-start is required unless --all-raw")
            window_start, window_end = resolve_interval(interval_start, interval_end)

        ctx_settings = (
            self.settings
            if lake_path is None
            else self.settings.with_overrides(lake_override=lake_path)
        )
        with use_settings(ctx_settings), bound_run_context(
            command="migrate",
            pipeline=config.name,
            interval_start=window_start,
            interval_end=window_end,
            extract_run_datetime=job_ts,
            destination=config.destination.type,
            lake=sanitize_lake_uri(
                str(lake_root(config.destination, self.project_root, settings=ctx_settings))
            ),
        ):
            raw_name = validate_canonical_id(from_raw or config.bronze_dataset())
            to_bronze_id = validate_canonical_id(to_bronze)
            raw_dataset = raw_dataset_dir(
                config, self.project_root, dataset=raw_name
            )
            source_parts = _raw_partitions_for_migrate(
                raw_dataset,
                start=window_start,
                end=window_end,
                all_raw=all_raw,
                all_raw_runs=all_raw_runs,
            )
            if wire_version is not None:
                filtered: list[Path | LakeRef] = []
                for part in source_parts:
                    try:
                        part_manifest = read_manifest(part)
                    except Exception:  # noqa: S112  # skip unreadable manifests
                        continue
                    if manifest_wire_version(part_manifest) == wire_version:
                        filtered.append(part)
                source_parts = filtered

            if all_raw and source_parts:
                starts: list[str] = []
                ends: list[str] = []
                for part in source_parts:
                    try:
                        man = read_manifest(part)
                        s, e = resolve_interval(
                            str(man.get("interval_start", "")),
                            str(man.get("interval_end", "")),
                        )
                        starts.append(s)
                        ends.append(e)
                    except Exception:  # noqa: S112  # skip bad manifests in all_raw window
                        continue
                if starts and ends:
                    window_start, window_end = min(starts), max(ends)

            # Keep name == source.type; _lake_id overrides the target lake/SQL identity.
            to_config = PipelineConfig(
                name=config.name,
                source=SourceConfig(type=config.source.type, overrides=config.source.overrides),
                schema=str(schema_path),
                validation=ValidationConfig(),
                ingestion=IngestionConfig(
                    library=ingestion_library,  # type: ignore[arg-type]
                    chunk_rows=config.ingestion.chunk_rows,
                ),
                destination=config.destination,
                medallion=config.medallion,
                bronze=config.bronze,
                wire_version=config.wire_version,
            )
            to_config._lake_id = to_bronze_id

            bronze_loc = bronze_dataset_dir(
                to_config, self.project_root, dataset=to_bronze_id
            )
            recreate_warning = None
            yaml_partition = None
            if recreate_iceberg:
                yaml_partition = to_config.destination.iceberg_partition
                scope = (
                    "all committed raw intervals"
                    if all_raw
                    else f"raw partitions in [{window_start}, {window_end})"
                )
                runs_note = (
                    " every raw extract-run sibling"
                    if all_raw_runs
                    else " latest raw extract per interval (load parity)"
                )
                recreate_warning = (
                    "Will DROP the entire Iceberg bronze table at "
                    f"{bronze_loc}, then rewrite {scope} ({runs_note}). "
                    "Bronze data outside the rewrite set is destroyed."
                )

            if dry_run:
                return self._migrate_dry_run(
                    config=config,
                    to_config=to_config,
                    source=source,
                    effective=effective,
                    mapper=mapper,
                    schema=schema,
                    schema_rel=schema_rel,
                    mapper_name=mapper_name,
                    raw_name=raw_name,
                    to_bronze_id=to_bronze_id,
                    source_parts=source_parts,
                    window_start=window_start or job_ts,
                    window_end=window_end or job_ts,
                    validate_limit=validate_limit,
                    wire_version_filter=wire_version,
                    recreate_iceberg=recreate_iceberg,
                    will_drop_table=recreate_iceberg,
                    table_location=_rel(bronze_loc, self.project_root),
                    yaml_partition=yaml_partition,
                    recreate_warning=recreate_warning,
                    all_raw=all_raw,
                    all_raw_runs=all_raw_runs,
                )

            if recreate_iceberg:
                from det.ingestion.iceberg_writer import purge_iceberg_table
                from det.runtime.ids import sql_names_for_config

                sql_schema, sql_table = sql_names_for_config(to_config)
                logger.warning(
                    "recreate_iceberg purging bronze table before migrate",
                    table=f"{sql_schema}.{sql_table}",
                    location=str(bronze_loc),
                    interval_start=window_start,
                    interval_end=window_end,
                    all_raw=all_raw,
                    all_raw_runs=all_raw_runs,
                )
                purge_iceberg_table(
                    lake=lake_root(
                        to_config.destination, self.project_root, settings=ctx_settings
                    ),
                    table_location=bronze_loc,
                    namespace=sql_schema,
                    table=sql_table,
                )

            backend = get_ingestion(ingestion_library)
            total_rows = 0
            written = 0

            for raw_dir in source_parts:
                manifest = read_manifest(raw_dir)
                extract_ts = extract_run_datetime_from_raw(manifest, raw_dir)
                start_iso = str(manifest.get("interval_start") or window_start)
                end_iso = str(manifest.get("interval_end") or window_end)
                start_iso, end_iso = resolve_interval(start_iso, end_iso)
                with pipeline_lease(
                    lake_root(
                        config.destination, self.project_root, settings=ctx_settings
                    ),
                    pipeline=config.name,
                    interval_start=start_iso,
                    interval_end=end_iso,
                    command="migrate",
                    ttl_sec=ctx_settings.effective_lock_ttl(lock_ttl_sec),
                    owner=ctx_settings.lock_owner,
                    enabled=ctx_settings.locks_enabled,
                ):
                    bronze_loaded_at = format_extract_run_datetime()
                    stream = iter_bronze_rows(
                        source.records_from_raw(
                            config=effective, raw_dir=raw_dir, manifest=manifest
                        ),
                        schema=schema,
                        naming=config.bronze.naming,
                        extract_run_datetime=extract_ts,
                        interval_start_datetime=start_iso,
                        interval_end_datetime=end_iso,
                        bronze_loaded_at=bronze_loaded_at,
                        mapper=mapper,
                        log_every=to_config.ingestion.chunk_rows,
                    )
                    counted = CountingIter(stream)
                    out_part = hive_partition_dir(
                        bronze_dataset_dir(
                            to_config, self.project_root, dataset=to_bronze_id
                        ),
                        interval_start_datetime=start_iso,
                        interval_end_datetime=end_iso,
                        extract_run_datetime=extract_ts,
                    )
                    backend.write(
                        counted,
                        config=to_config,
                        project_root=self.project_root,
                        partition_dir=out_part,
                        destination=to_config.destination,
                        chunk_rows=to_config.ingestion.chunk_rows,
                        run_identity=(start_iso, end_iso, extract_ts),
                    )
                    stamp_validation_success(
                        raw_dir,
                        schema_path=schema_rel,
                        schema_sha256=sha256_file(schema_resolved),
                        row_count=counted.n,
                        wire_version=to_config.wire_version,
                        validated_at=format_extract_run_datetime(),
                    )
                    total_rows += counted.n
                    written += 1
                    logger.info(
                        "migrated raw partition",
                        raw_dir=str(raw_dir),
                        rows=counted.n,
                        extract_run_datetime=extract_ts,
                    )

            return MigrateResult(
                from_raw=raw_name,
                to_bronze=to_bronze_id,
                partitions=written,
                rows=total_rows,
            )

    def _migrate_dry_run(
        self,
        *,
        config: PipelineConfig,
        to_config: PipelineConfig,
        source: Any,
        effective: dict[str, Any],
        mapper: Any,
        schema: dict[str, Any],
        schema_rel: str,
        mapper_name: str,
        raw_name: str,
        to_bronze_id: str,
        source_parts: list[Path | LakeRef],
        window_start: str,
        window_end: str,
        validate_limit: int | None,
        wire_version_filter: int | None = None,
        recreate_iceberg: bool = False,
        will_drop_table: bool = False,
        table_location: str | None = None,
        yaml_partition: str | None = None,
        recreate_warning: str | None = None,
        all_raw: bool = False,
        all_raw_runs: bool = False,
    ) -> MigratePlan:
        plans: list[PartitionPlan] = []
        rows_checked = 0

        for raw_dir in source_parts:
            errors: list[str] = []
            named_rows: list[tuple[dict[str, Any], str | None]] = []
            truncated = False
            try:
                manifest = read_manifest(raw_dir)
            except Exception as exc:
                plans.append(
                    PartitionPlan(
                        raw_path=_rel(raw_dir, self.project_root),
                        would_write_bronze_path=None,
                        interval_start=window_start,
                        interval_end=window_end,
                        extract_run_datetime="",
                        rows=0,
                        ok=False,
                        errors=[f"manifest: {exc}"],
                    )
                )
                continue

            try:
                extract_ts = extract_run_datetime_from_raw(manifest, raw_dir)
            except ValueError as exc:
                plans.append(
                    PartitionPlan(
                        raw_path=_rel(raw_dir, self.project_root),
                        would_write_bronze_path=None,
                        interval_start=window_start,
                        interval_end=window_end,
                        extract_run_datetime="",
                        rows=0,
                        ok=False,
                        errors=[str(exc)],
                    )
                )
                continue

            part_wire = manifest_wire_version(manifest)
            start_iso = str(manifest.get("interval_start") or window_start)
            end_iso = str(manifest.get("interval_end") or window_end)
            try:
                start_iso, end_iso = resolve_interval(start_iso, end_iso)
            except ValueError as exc:
                errors.append(str(exc))

            out_part = hive_partition_dir(
                bronze_dataset_dir(to_config, self.project_root, dataset=to_bronze_id),
                interval_start_datetime=start_iso,
                interval_end_datetime=end_iso,
                extract_run_datetime=extract_ts,
            )

            try:
                for source_row in source.records_from_raw(
                    config=effective, raw_dir=raw_dir, manifest=manifest
                ):
                    if validate_limit is not None and len(named_rows) >= validate_limit:
                        truncated = True
                        break
                    try:
                        named = apply_naming(source_row.data, config.bronze.naming)
                        mapped = mapper(named)
                        typed = coerce_record(mapped, schema)
                    except (CoerceError, ValueError, TypeError) as exc:
                        errors.append(str(exc))
                        if len(errors) >= 20:
                            truncated = True
                            break
                        continue
                    named_rows.append((typed, source_row.filename))
            except Exception as exc:
                errors.append(f"records_from_raw: {exc}")

            if named_rows and not errors:
                try:
                    validate_records([row for row, _ in named_rows], schema)
                except SchemaValidationError as exc:
                    errors.extend(exc.errors or [str(exc)])

            part_ok = not errors
            rows_checked += len(named_rows)
            plans.append(
                PartitionPlan(
                    raw_path=_rel(raw_dir, self.project_root),
                    would_write_bronze_path=_rel(out_part, self.project_root),
                    interval_start=start_iso,
                    interval_end=end_iso,
                    extract_run_datetime=extract_ts,
                    rows=len(named_rows),
                    ok=part_ok,
                    truncated=truncated,
                    errors=errors[:20],
                    wire_version=part_wire,
                )
            )
            logger.info(
                "migrate dry-run partition",
                raw_dir=str(raw_dir),
                rows=len(named_rows),
                ok=part_ok,
                wire_version=part_wire,
                extract_run_datetime=extract_ts,
            )

        plan_extract = plans[0].extract_run_datetime if plans else ""
        return MigratePlan(
            dry_run=True,
            from_raw=raw_name,
            to_bronze=to_bronze_id,
            mapper_name=mapper_name,
            schema_path=schema_rel,
            extract_run_datetime=plan_extract,
            partitions=plans,
            partitions_planned=len(plans),
            rows_checked=rows_checked,
            wire_version_filter=wire_version_filter,
            recreate_iceberg=recreate_iceberg,
            will_drop_table=will_drop_table,
            table_location=table_location,
            yaml_partition=yaml_partition,
            recreate_warning=recreate_warning,
            all_raw=all_raw,
            all_raw_runs=all_raw_runs,
        )
