from __future__ import annotations

from pathlib import Path

import duckdb
import structlog
from typer.testing import CliRunner

from det.cli import app
from det.ingestion.duckdb_writer import write_duckdb_table
from det.logging import configure_logging
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.meta import to_partition_value
from det.runtime.prune import BronzePruner


def _invoke_prune(args: list[str]):
    runner = CliRunner()
    try:
        return runner.invoke(app, args)
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")


def _fs_config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
        ingestion=IngestionConfig(library="dlt"),
        destination=DestinationConfig(type="filesystem", path=str(tmp_path / "lake")),
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
    )


def _duck_config(tmp_path: Path, db_path: Path) -> PipelineConfig:
    return PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
        ingestion=IngestionConfig(library="dlt"),
        destination=DestinationConfig(
            type="duckdb",
            path=str(tmp_path / "lake"),
            connection=str(db_path),
            dataset="bronze",
        ),
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
    )


def _mk_run(
    base: Path,
    *,
    interval_start: str,
    interval_end: str,
    extract_run: str,
) -> Path:
    run = (
        base
        / f"__interval_start_datetime={to_partition_value(interval_start)}"
        / f"__interval_end_datetime={to_partition_value(interval_end)}"
        / f"__extract_run_datetime={to_partition_value(extract_run)}"
    )
    run.mkdir(parents=True, exist_ok=True)
    (run / "data.jsonl").write_text("{}\n", encoding="utf-8")
    return run


def test_filesystem_prune_dry_run_and_apply_keeps_newest(tmp_path: Path):
    config = _fs_config(tmp_path)
    bronze = tmp_path / "lake" / "bronze" / "noaa" / "storm_events"
    raw = tmp_path / "lake" / "raw" / "noaa" / "storm_events"
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    runs = [
        "2026-08-06T10:00:00+00:00",
        "2026-08-06T11:00:00+00:00",
        "2026-08-06T12:00:00+00:00",
    ]
    bronze_dirs = [
        _mk_run(bronze, interval_start=start, interval_end=end, extract_run=r) for r in runs
    ]
    raw_dirs = [
        _mk_run(raw, interval_start=start, interval_end=end, extract_run=r) for r in runs
    ]

    pruner = BronzePruner(tmp_path)
    plan = pruner.plan(
        config,
        interval_start="2026-08-01",
        interval_end="2026-09-01",
        keep=1,
    )
    assert plan.remove_count == 2
    assert {r.extract_run_datetime for r in plan.to_remove} == {
        "2026-08-06T10:00:00+00:00",
        "2026-08-06T11:00:00+00:00",
    }
    # Dry-run semantics: plan only; dirs untouched until apply.
    assert all(p.exists() for p in bronze_dirs)
    assert all(p.exists() for p in raw_dirs)

    removed = pruner.apply(config, plan)
    assert removed == 2
    assert bronze_dirs[0].exists() is False
    assert bronze_dirs[1].exists() is False
    assert bronze_dirs[2].exists() is True
    assert all(p.exists() for p in raw_dirs)


def test_duckdb_prune_deletes_old_runs(tmp_path: Path):
    db_path = tmp_path / "analytics.duckdb"
    config = _duck_config(tmp_path, db_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    for i, run in enumerate(
        (
            "2026-08-06T10:00:00+00:00",
            "2026-08-06T11:00:00+00:00",
            "2026-08-06T12:00:00+00:00",
        )
    ):
        write_duckdb_table(
            [
                {
                    "event_id": i,
                    "__row_hash": f"h{i}",
                    "__extract_run_datetime": run,
                    "__interval_start_datetime": start,
                    "__interval_end_datetime": end,
                }
            ],
            connection_path=db_path,
            schema="bronze_noaa",
            table="storm_events",
        )

    pruner = BronzePruner(tmp_path)
    plan = pruner.plan(
        config,
        interval_start="2026-08-01",
        interval_end="2026-09-01",
        keep=1,
    )
    assert plan.remove_count == 2
    removed = pruner.apply(config, plan)
    assert removed == 2

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "select __extract_run_datetime, event_id from bronze_noaa.storm_events"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "2026-08-06T12:00:00+00:00"
        assert rows[0][1] == 2
    finally:
        con.close()


def test_cli_prune_requires_mode(tmp_path: Path):
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        f"""
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/noaa/storm_events/storm_events.schema.yaml
destination:
  type: filesystem
  path: {tmp_path / "lake"}
""",
        encoding="utf-8",
    )
    result = _invoke_prune(
        [
            "prune",
            "--pipeline",
            str(pipeline),
            "--interval-start",
            "2026-08-01",
            "--interval-end",
            "2026-09-01",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
    assert "--dry-run" in result.output or "exactly one" in result.output


def test_cli_prune_rejects_both_flags(tmp_path: Path):
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        f"""
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/noaa/storm_events/storm_events.schema.yaml
destination:
  type: filesystem
  path: {tmp_path / "lake"}
""",
        encoding="utf-8",
    )
    result = _invoke_prune(
        [
            "prune",
            "--pipeline",
            str(pipeline),
            "--interval-start",
            "2026-08-01",
            "--dry-run",
            "--apply",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
