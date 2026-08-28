from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from det.destinations.models import (
    bronze_dataset_dir,
    duckdb_connection_path,
    lake_root,
    postgres_dsn,
)
from det.ingestion.sql_replace import delete_extract_run_sql
from det.logging import bound_run_context, get_logger, sanitize_lake_uri
from det.optional_deps import require_duckdb
from det.runtime.config import PipelineConfig, load_pipeline
from det.runtime.ids import sql_names_for_config
from det.runtime.lake import LakeRef
from det.runtime.lease import assert_lease_held, pipeline_lease, resolve_lease_options
from det.runtime.lease.dataset_lock import assert_dataset_lock_held, dataset_shared_lock
from det.runtime.meta import from_partition_value, identity_iso, resolve_interval
from det.runtime.settings import DetSettings, use_settings

logger = get_logger(__name__)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class BronzeRunRef:
    """One extract run under an interval that is a prune candidate."""

    interval_start: str
    interval_end: str
    extract_run_datetime: str
    path: LakeRef | None = None  # filesystem bronze run dir, when applicable


@dataclass
class PrunePlan:
    keep: int
    to_remove: list[BronzeRunRef] = field(default_factory=list)
    to_keep: list[BronzeRunRef] = field(default_factory=list)

    @property
    def remove_count(self) -> int:
        return len(self.to_remove)


class BronzePruner:
    """Bronze-only retention. Never touches raw/."""

    def __init__(
        self,
        project_root: Path | None = None,
        *,
        settings: DetSettings | None = None,
    ) -> None:
        if settings is not None and project_root is not None:
            raise ValueError("pass settings= or project_root=, not both")
        if settings is None:
            if project_root is None:
                raise TypeError("BronzePruner requires project_root= or settings=")
            settings = DetSettings.from_env(project_root=project_root)
        self.settings = settings
        self.project_root = settings.project_root

    def _lake(self, dest):
        return lake_root(dest, self.project_root, settings=self.settings)

    def _config(self, pipeline: PipelineConfig | Path | str) -> PipelineConfig:
        return load_pipeline(pipeline, project_root=self.project_root)

    def plan(
        self,
        pipeline: PipelineConfig | Path | str,
        *,
        interval_start: str,
        interval_end: str | None,
        keep: int,
    ) -> PrunePlan:
        with use_settings(self.settings):
            return self._plan(
                self._config(pipeline),
                interval_start=interval_start,
                interval_end=interval_end,
                keep=keep,
            )

    def _plan(
        self,
        config: PipelineConfig,
        *,
        interval_start: str,
        interval_end: str | None,
        keep: int,
    ) -> PrunePlan:
        if keep < 1:
            raise ValueError("--keep must be >= 1")
        window_start, window_end = resolve_interval(interval_start, interval_end)
        dest = config.destination
        with bound_run_context(
            command="prune",
            pipeline=config.name,
            interval_start=window_start,
            interval_end=window_end,
            destination=dest.type,
            lake=sanitize_lake_uri(str(self._lake(dest))),
        ):
            if dest.type == "filesystem":
                return self._plan_filesystem(
                    config, window_start=window_start, window_end=window_end, keep=keep
                )
            if dest.type == "duckdb":
                return self._plan_duckdb(
                    config, window_start=window_start, window_end=window_end, keep=keep
                )
            if dest.type == "postgres":
                return self._plan_postgres(
                    config, window_start=window_start, window_end=window_end, keep=keep
                )
            if dest.type == "iceberg":
                return self._plan_iceberg(
                    config, window_start=window_start, window_end=window_end, keep=keep
                )
            raise ValueError(
                f"det prune does not support destination.type={dest.type!r}; "
                "use filesystem, duckdb, postgres, or iceberg"
            )

    def apply(
        self,
        pipeline: PipelineConfig | Path | str,
        plan: PrunePlan,
        *,
        interval_start: str | None = None,
        interval_end: str | None = None,
        lock_ttl_sec: int | None = None,
    ) -> int:
        with use_settings(self.settings):
            return self._apply(
                self._config(pipeline),
                plan,
                interval_start=interval_start,
                interval_end=interval_end,
                lock_ttl_sec=lock_ttl_sec,
            )

    def _apply(
        self,
        config: PipelineConfig,
        plan: PrunePlan,
        *,
        interval_start: str | None = None,
        interval_end: str | None = None,
        lock_ttl_sec: int | None = None,
    ) -> int:
        if not plan.to_remove:
            return 0
        dest = config.destination
        lake = self._lake(dest)
        lease_opts = resolve_lease_options(
            settings=self.settings,
            pipeline=config,
            ttl_sec=self.settings.effective_lock_ttl(lock_ttl_sec),
            owner=self.settings.lock_owner,
            enabled=self.settings.locks_enabled,
        )
        lease_kwargs = {
            "ttl_sec": self.settings.effective_lock_ttl(lock_ttl_sec),
            "owner": self.settings.lock_owner,
            "enabled": self.settings.locks_enabled,
            "options": lease_opts,
            "resolve_secret": self.settings.resolve_secret,
        }
        if interval_start is not None:
            start_iso, end_iso = resolve_interval(interval_start, interval_end)
            with pipeline_lease(
                lake,
                pipeline=config.name,
                interval_start=start_iso,
                interval_end=end_iso,
                command="prune",
                **lease_kwargs,
            ) as lease:
                with dataset_shared_lock(
                    lake,
                    config.canonical_id,
                    command="prune",
                    **lease_kwargs,
                ) as dataset_lock:
                    assert_lease_held(
                        lease, store=None if lease is None else lease.store
                    )
                    assert_dataset_lock_held(dataset_lock)
                    return self._apply_body(config, plan)
        with dataset_shared_lock(
            lake,
            config.canonical_id,
            command="prune",
            **lease_kwargs,
        ) as dataset_lock:
            assert_dataset_lock_held(dataset_lock)
            return self._apply_body(config, plan)

    def _apply_body(self, config: PipelineConfig, plan: PrunePlan) -> int:
        dest = config.destination
        with bound_run_context(
            command="prune",
            pipeline=config.name,
            destination=dest.type,
            lake=sanitize_lake_uri(str(self._lake(dest))),
        ):
            if dest.type == "filesystem":
                return self._apply_filesystem(config, plan)
            if dest.type == "duckdb":
                return self._apply_duckdb(config, plan)
            if dest.type == "postgres":
                return self._apply_postgres(config, plan)
            if dest.type == "iceberg":
                return self._apply_iceberg(config, plan)
            raise ValueError(f"Unsupported destination type for prune apply: {dest.type}")

    def _plan_filesystem(
        self,
        config: PipelineConfig,
        *,
        window_start: str,
        window_end: str,
        keep: int,
    ) -> PrunePlan:
        root = bronze_dataset_dir(config, self.project_root)
        if not root.exists():
            return PrunePlan(keep=keep)

        to_remove: list[BronzeRunRef] = []
        to_keep: list[BronzeRunRef] = []
        start_prefix = "__interval_start_datetime="
        end_prefix = "__interval_end_datetime="
        run_prefix = "__extract_run_datetime="

        for start_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            if not start_dir.name.startswith(start_prefix):
                continue
            start_iso = from_partition_value(start_dir.name[len(start_prefix) :])
            if not (window_start <= start_iso < window_end):
                continue
            for end_dir in sorted(p for p in start_dir.iterdir() if p.is_dir()):
                if not end_dir.name.startswith(end_prefix):
                    continue
                end_iso = from_partition_value(end_dir.name[len(end_prefix) :])
                runs = sorted(
                    (
                        p
                        for p in end_dir.iterdir()
                        if p.is_dir() and p.name.startswith(run_prefix)
                    ),
                    key=lambda p: p.name,
                )
                refs = [
                    BronzeRunRef(
                        interval_start=start_iso,
                        interval_end=end_iso,
                        extract_run_datetime=from_partition_value(
                            p.name[len(run_prefix) :]
                        ),
                        path=p,
                    )
                    for p in runs
                ]
                if len(refs) <= keep:
                    to_keep.extend(refs)
                    continue
                to_keep.extend(refs[-keep:])
                to_remove.extend(refs[:-keep])

        return PrunePlan(keep=keep, to_remove=to_remove, to_keep=to_keep)

    def _plan_duckdb(
        self,
        config: PipelineConfig,
        *,
        window_start: str,
        window_end: str,
        keep: int,
    ) -> PrunePlan:
        db_path = duckdb_connection_path(config.destination, self.project_root)
        if not db_path.exists():
            return PrunePlan(keep=keep)

        schema, table = sql_names_for_config(config)
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        duckdb = require_duckdb()
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            exists = con.execute(
                """
                select count(*) from information_schema.tables
                where table_schema = ? and table_name = ?
                """,
                [schema, table],
            ).fetchone()
            if not exists or exists[0] == 0:
                return PrunePlan(keep=keep)
            rows = con.execute(
                f"""
                select distinct
                    __interval_start_datetime,
                    __interval_end_datetime,
                    __extract_run_datetime
                from {qualified}
                where __interval_start_datetime >= ?
                  and __interval_start_datetime < ?
                order by 1, 2, 3
                """,
                [window_start, window_end],
            ).fetchall()
        finally:
            con.close()

        return _plan_from_run_rows(rows, keep=keep)

    def _plan_postgres(
        self,
        config: PipelineConfig,
        *,
        window_start: str,
        window_end: str,
        keep: int,
    ) -> PrunePlan:
        try:
            import psycopg
        except ImportError as exc:
            raise ImportError(
                'Postgres prune requires the optional extra: pip install -e ".[postgres]"'
            ) from exc

        dsn = postgres_dsn(config.destination)
        schema, table = sql_names_for_config(config)
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select count(*) from information_schema.tables
                    where table_schema = %s and table_name = %s
                    """,
                    (schema, table),
                )
                exists = cur.fetchone()
                if not exists or exists[0] == 0:
                    return PrunePlan(keep=keep)
                query = f"""
                    select distinct
                        __interval_start_datetime,
                        __interval_end_datetime,
                        __extract_run_datetime
                    from {qualified}
                    where __interval_start_datetime >= %s
                      and __interval_start_datetime < %s
                    order by 1, 2, 3
                    """
                cur.execute(query, (window_start, window_end))  # pyright: ignore[reportArgumentType]
                rows = cur.fetchall()
        return _plan_from_run_rows(rows, keep=keep)

    def _apply_filesystem(self, config: PipelineConfig, plan: PrunePlan) -> int:
        bronze_root = bronze_dataset_dir(config, self.project_root)
        removed = 0
        for ref in plan.to_remove:
            if ref.path is None or not ref.path.exists():
                continue
            if not ref.path.is_relative_to(bronze_root):
                raise RuntimeError(
                    f"refusing to delete path outside bronze dataset: {ref.path}"
                )
            ref.path.rmtree()
            removed += 1
            logger.info("pruned bronze run dir", path=str(ref.path))
        return removed

    def _apply_duckdb(self, config: PipelineConfig, plan: PrunePlan) -> int:
        db_path = duckdb_connection_path(config.destination, self.project_root)
        schema, table = sql_names_for_config(config)
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        duckdb = require_duckdb()
        con = duckdb.connect(str(db_path))
        deleted_groups = 0
        try:
            for ref in plan.to_remove:
                con.execute(
                    delete_extract_run_sql(qualified, placeholder="?"),
                    [
                        ref.interval_start,
                        ref.interval_end,
                        ref.extract_run_datetime,
                    ],
                )
                deleted_groups += 1
                logger.info(
                    "pruned duckdb bronze run",
                    table=f"{schema}.{table}",
                    interval_start=ref.interval_start,
                    extract_run_datetime=ref.extract_run_datetime,
                )
        finally:
            con.close()
        return deleted_groups

    def _apply_postgres(self, config: PipelineConfig, plan: PrunePlan) -> int:
        try:
            import psycopg
        except ImportError as exc:
            raise ImportError(
                'Postgres prune requires the optional extra: pip install -e ".[postgres]"'
            ) from exc

        dsn = postgres_dsn(config.destination)
        schema, table = sql_names_for_config(config)
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        deleted_groups = 0
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                for ref in plan.to_remove:
                    delete_sql = delete_extract_run_sql(qualified, placeholder="%s")
                    cur.execute(
                        delete_sql,  # pyright: ignore[reportArgumentType]
                        (
                            ref.interval_start,
                            ref.interval_end,
                            ref.extract_run_datetime,
                        ),
                    )
                    deleted_groups += 1
                    logger.info(
                        "pruned postgres bronze run",
                        table=f"{schema}.{table}",
                        interval_start=ref.interval_start,
                        extract_run_datetime=ref.extract_run_datetime,
                    )
            conn.commit()
        return deleted_groups

    def _plan_iceberg(
        self,
        config: PipelineConfig,
        *,
        window_start: str,
        window_end: str,
        keep: int,
    ) -> PrunePlan:
        from det.ingestion.iceberg_writer import list_iceberg_extract_runs, load_iceberg_table

        schema, table = sql_names_for_config(config)
        ice = load_iceberg_table(
            lake=self._lake(config.destination),
            namespace=schema,
            table=table,
            table_location=bronze_dataset_dir(config, self.project_root),
        )
        if ice is None:
            return PrunePlan(keep=keep)
        rows = list_iceberg_extract_runs(
            ice, window_start=window_start, window_end=window_end
        )
        return _plan_from_run_rows(rows, keep=keep)

    def _apply_iceberg(self, config: PipelineConfig, plan: PrunePlan) -> int:
        from det.ingestion.iceberg_writer import delete_iceberg_extract_run, load_iceberg_table

        schema, table = sql_names_for_config(config)
        ice = load_iceberg_table(
            lake=self._lake(config.destination),
            namespace=schema,
            table=table,
            table_location=bronze_dataset_dir(config, self.project_root),
        )
        if ice is None:
            return 0
        deleted_groups = 0
        for ref in plan.to_remove:
            delete_iceberg_extract_run(
                ice,
                (ref.interval_start, ref.interval_end, ref.extract_run_datetime),
            )
            deleted_groups += 1
            logger.info(
                "pruned iceberg bronze run",
                table=f"{schema}.{table}",
                interval_start=ref.interval_start,
                extract_run_datetime=ref.extract_run_datetime,
            )
        return deleted_groups


def _plan_from_run_rows(rows: list, *, keep: int) -> PrunePlan:
    by_interval: dict[tuple[str, str], list[BronzeRunRef]] = {}
    for start, end, run in rows:
        start_s = identity_iso(start)
        end_s = identity_iso(end)
        run_s = identity_iso(run)
        by_interval.setdefault((start_s, end_s), []).append(
            BronzeRunRef(
                interval_start=start_s,
                interval_end=end_s,
                extract_run_datetime=run_s,
            )
        )

    to_remove: list[BronzeRunRef] = []
    to_keep: list[BronzeRunRef] = []
    for refs in by_interval.values():
        refs_sorted = sorted(refs, key=lambda r: r.extract_run_datetime)
        if len(refs_sorted) <= keep:
            to_keep.extend(refs_sorted)
            continue
        to_keep.extend(refs_sorted[-keep:])
        to_remove.extend(refs_sorted[:-keep])
    return PrunePlan(keep=keep, to_remove=to_remove, to_keep=to_keep)
