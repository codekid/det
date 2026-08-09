from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from det.destinations.models import bronze_dataset_dir, duckdb_connection_path
from det.logging import get_logger
from det.runtime.config import PipelineConfig
from det.runtime.ids import sql_names_for_config
from det.runtime.meta import from_partition_value, resolve_interval, to_interval_datetime

logger = get_logger(__name__)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class BronzeRunRef:
    """One extract run under an interval that is a prune candidate."""

    interval_start: str
    interval_end: str
    extract_run_datetime: str
    path: Path | None = None  # filesystem bronze run dir, when applicable


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

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root.resolve()

    def plan(
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
        raise ValueError(
            f"det prune does not support destination.type={dest.type!r}; "
            "use filesystem, duckdb, or postgres"
        )

    def apply(self, config: PipelineConfig, plan: PrunePlan) -> int:
        if not plan.to_remove:
            return 0
        dest = config.destination
        if dest.type == "filesystem":
            return self._apply_filesystem(config, plan)
        if dest.type == "duckdb":
            return self._apply_duckdb(config, plan)
        if dest.type == "postgres":
            return self._apply_postgres(config, plan)
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
                where cast(__interval_start_datetime as varchar) >= ?
                  and cast(__interval_start_datetime as varchar) < ?
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

        dsn = config.destination.connection
        if not dsn:
            return PrunePlan(keep=keep)
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
                cur.execute(
                    f"""
                    select distinct
                        __interval_start_datetime,
                        __interval_end_datetime,
                        __extract_run_datetime
                    from {qualified}
                    where cast(__interval_start_datetime as text) >= %s
                      and cast(__interval_start_datetime as text) < %s
                    order by 1, 2, 3
                    """,
                    (window_start, window_end),
                )
                rows = cur.fetchall()
        return _plan_from_run_rows(rows, keep=keep)

    def _apply_filesystem(self, config: PipelineConfig, plan: PrunePlan) -> int:
        bronze_root = bronze_dataset_dir(config, self.project_root).resolve()
        removed = 0
        for ref in plan.to_remove:
            if ref.path is None or not ref.path.exists():
                continue
            path = ref.path.resolve()
            if not path.is_relative_to(bronze_root):
                raise RuntimeError(f"refusing to delete path outside bronze dataset: {path}")
            shutil.rmtree(path)
            removed += 1
            logger.info("pruned bronze run dir", path=str(path))
        return removed

    def _apply_duckdb(self, config: PipelineConfig, plan: PrunePlan) -> int:
        db_path = duckdb_connection_path(config.destination, self.project_root)
        schema, table = sql_names_for_config(config)
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        con = duckdb.connect(str(db_path))
        deleted_groups = 0
        try:
            for ref in plan.to_remove:
                con.execute(
                    f"""
                    delete from {qualified}
                    where cast(__interval_start_datetime as varchar) = ?
                      and cast(__interval_end_datetime as varchar) = ?
                      and cast(__extract_run_datetime as varchar) = ?
                    """,
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

        dsn = config.destination.connection
        if not dsn:
            raise ValueError("destination.connection is required for postgres prune")
        schema, table = sql_names_for_config(config)
        qualified = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        deleted_groups = 0
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                for ref in plan.to_remove:
                    cur.execute(
                        f"""
                        delete from {qualified}
                        where cast(__interval_start_datetime as text) = %s
                          and cast(__interval_end_datetime as text) = %s
                          and cast(__extract_run_datetime as text) = %s
                        """,
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


def _plan_from_run_rows(rows: list, *, keep: int) -> PrunePlan:
    by_interval: dict[tuple[str, str], list[BronzeRunRef]] = {}
    for start, end, run in rows:
        start_s = _as_iso(start)
        end_s = _as_iso(end)
        run_s = _as_iso(run)
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


def _as_iso(value: object) -> str:
    if hasattr(value, "isoformat"):
        return to_interval_datetime(value.isoformat())  # type: ignore[union-attr]
    text = str(value)
    if len(text) >= 15 and text[8:9] == "T" and "-" not in text[:8]:
        return from_partition_value(text)
    return to_interval_datetime(text)
