"""Unit tests for det export-bq bronze loaders (no live BQ)."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

from det.cli.app import app
from det.runtime.bronze_export import load_bronze_export_rows
from det.runtime.config import PipelineConfig
from det.runtime.meta import to_partition_value
from det.runtime.settings import DetSettings


def _fs_config(project_root: Path) -> PipelineConfig:
    return PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
            "schema": "schemas/noaa/storm_events/storm_events.schema.yaml",
            "destination": {"type": "filesystem"},
            "wire_version": 1,
            "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
        }
    )


def _write_run(
    bronze_root: Path,
    *,
    interval_start: str,
    interval_end: str,
    extract_run: str,
    row_extract_run: str | None = None,
) -> None:
    run_meta = row_extract_run if row_extract_run is not None else extract_run
    run_dir = (
        bronze_root
        / f"__interval_start_datetime={to_partition_value(interval_start)}"
        / f"__interval_end_datetime={to_partition_value(interval_end)}"
        / f"__extract_run_datetime={to_partition_value(extract_run)}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta").mkdir(exist_ok=True)
    (run_dir / "meta" / "manifest.json").write_text("{}", encoding="utf-8")
    row = {
        "event_id": 1,
        "__interval_start_datetime": interval_start,
        "__interval_end_datetime": interval_end,
        "__extract_run_datetime": run_meta,
    }
    (run_dir / "data.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")


def test_filesystem_fractional_extract_run_selects_second_precision_hive(
    tmp_path: Path, project_root: Path
) -> None:
    lake = tmp_path / "lake"
    bronze = lake / "bronze" / "noaa" / "storm_events_v1"
    hive_run = "2026-08-06T12:00:00+00:00"
    row_run = "2026-08-06T12:00:00.123456+00:00"
    _write_run(
        bronze,
        interval_start="2026-08-06T00:00:00+00:00",
        interval_end="2026-08-07T00:00:00+00:00",
        extract_run=hive_run,
        row_extract_run=row_run,
    )
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path=str(lake)
    )
    cfg = _fs_config(project_root)
    batches = load_bronze_export_rows(
        cfg,
        project_root=tmp_path,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        extract_run_datetime=row_run,
        settings=settings,
    )
    assert len(batches) == 1
    identity, rows = batches[0]
    assert identity[2] == row_run
    assert len(rows) == 1


def test_filesystem_fractional_mismatch_raises(
    tmp_path: Path, project_root: Path
) -> None:
    lake = tmp_path / "lake"
    bronze = lake / "bronze" / "noaa" / "storm_events_v1"
    hive_run = "2026-08-06T12:00:00+00:00"
    row_run = "2026-08-06T12:00:00.123456+00:00"
    _write_run(
        bronze,
        interval_start="2026-08-06T00:00:00+00:00",
        interval_end="2026-08-07T00:00:00+00:00",
        extract_run=hive_run,
        row_extract_run=row_run,
    )
    settings = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_path=str(lake)
    )
    cfg = _fs_config(project_root)
    with pytest.raises(ValueError, match="does not match bronze row identity"):
        load_bronze_export_rows(
            cfg,
            project_root=tmp_path,
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            extract_run_datetime="2026-08-06T12:00:00.999999+00:00",
            settings=settings,
        )


def test_iceberg_groups_multiple_runs_without_pregroup_reject() -> None:
    rows = [
        {
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
            "__extract_run_datetime": "2026-08-06T12:00:00+00:00",
            "event_id": 1,
        },
        {
            "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
            "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
            "__extract_run_datetime": "2026-08-06T13:00:00+00:00",
            "event_id": 2,
        },
    ]
    cfg = PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
            "schema": "schemas/noaa/storm_events/storm_events.schema.yaml",
            "destination": {"type": "iceberg"},
            "wire_version": 1,
        }
    )

    class _FakeIce:
        pass

    with (
        patch(
            "det.runtime.bronze_export.iceberg_writer.load_iceberg_table",
            return_value=_FakeIce(),
        ),
        patch(
            "det.runtime.bronze_export.iceberg_writer.scan_iceberg_rows",
            return_value=rows,
        ),
        patch(
            "det.runtime.bronze_export.bronze_dataset_dir",
            return_value=Path("/tmp/bronze"),
        ),
        patch(
            "det.runtime.bronze_export.lake_root",
            return_value=Path("/tmp/lake"),
        ),
    ):
        batches = load_bronze_export_rows(
            cfg,
            project_root=Path("/tmp"),
            interval_start="2026-08-06",
            interval_end="2026-08-07",
        )
    assert len(batches) == 2
    runs = {identity[2] for identity, _ in batches}
    assert runs == {
        "2026-08-06T12:00:00+00:00",
        "2026-08-06T13:00:00+00:00",
    }


def test_filesystem_export_raises_when_list_cap_exceeded(tmp_path: Path) -> None:
    cfg = _fs_config(tmp_path)
    over = [
        {
            "interval_start": "2026-08-06T00:00:00+00:00",
            "interval_end": "2026-08-07T00:00:00+00:00",
            "extract_run_datetime": f"2026-08-06T12:00:{i:02d}+00:00",
            "path": f"bronze/run_{i}",
        }
        for i in range(201)
    ]
    with (
        patch(
            "det.runtime.bronze_export.walk_hive_runs",
            return_value=over,
        ),
        patch(
            "det.runtime.bronze_export.bronze_dataset_dir",
            return_value=tmp_path / "bronze",
        ),
        patch("det.runtime.bronze_export.DEFAULT_LIST_LIMIT", 200),
    ):
        with pytest.raises(ValueError, match="more than 200 committed runs"):
            load_bronze_export_rows(
                cfg,
                project_root=tmp_path,
                interval_start="2026-08-06",
                interval_end="2026-08-07",
            )


def test_export_bq_cli_leases_each_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """export-bq must hold a pipeline lease across DELETE+load for each identity."""

    monkeypatch.setenv("DET_LOCK", "0")
    monkeypatch.setenv("DET_GCP_PROJECT", "det-test")
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)

    schema = tmp_path / "schema.yaml"
    schema.write_text(
        yaml.safe_dump(
            {
                "type": "object",
                "properties": {"event_id": {"type": "integer"}},
            }
        ),
        encoding="utf-8",
    )
    pipe = tmp_path / "pipe.yaml"
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "noaa.storm_events",
                "source": {"type": "noaa.storm_events"},
                "schema": str(schema),
                "destination": {"type": "filesystem"},
                "wire_version": 1,
            }
        ),
        encoding="utf-8",
    )

    identity = (
        "2026-08-06T00:00:00+00:00",
        "2026-08-07T00:00:00+00:00",
        "2026-08-06T12:00:00+00:00",
    )
    leases: list[dict[str, Any]] = []
    refreshes: list[str] = []
    writes: list[dict[str, Any]] = []

    @contextmanager
    def fake_lease(*_a: Any, **kwargs: Any):
        leases.append(kwargs)
        yield SimpleNamespace(store=None)

    def fake_refresh(**kwargs: Any) -> None:
        refreshes.append("refresh")

    def fake_write(*_a: Any, **kwargs: Any) -> Path:
        writes.append(kwargs)
        on_chunk = kwargs.get("on_chunk")
        if on_chunk is not None:
            on_chunk()
        return Path("bq/det-test/bronze_noaa/storm_events_v1")

    monkeypatch.setattr("det.cli.export_cmd.pipeline_lease", fake_lease)
    monkeypatch.setattr("det.cli.export_cmd.refresh_bronze_locks", fake_refresh)
    monkeypatch.setattr(
        "det.cli.export_cmd.load_bronze_export_rows",
        lambda *_a, **_k: [
            (
                identity,
                [
                    {
                        "event_id": 1,
                        "__interval_start_datetime": identity[0],
                        "__interval_end_datetime": identity[1],
                        "__extract_run_datetime": identity[2],
                    }
                ],
            )
        ],
    )
    monkeypatch.setattr("det.cli.export_cmd.write_to_bigquery", fake_write)

    result = CliRunner().invoke(
        app,
        [
            "export-bq",
            "-p",
            str(pipe),
            "-s",
            "2026-08-06",
            "-e",
            "2026-08-07",
            "--project-root",
            str(tmp_path),
            "--lake-path",
            str(tmp_path / "lake"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(leases) == 1
    assert leases[0]["pipeline"] == "noaa.storm_events"
    assert leases[0]["interval_start"] == identity[0]
    assert leases[0]["interval_end"] == identity[1]
    assert leases[0]["command"] == "export-bq"
    assert len(writes) == 1
    assert writes[0]["run_identity"] == identity
    assert writes[0]["on_chunk"] is not None
    # on_chunk during write + explicit refresh after write
    assert len(refreshes) >= 2


def test_iceberg_over_cap_raises() -> None:
    cfg = PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
            "schema": "schemas/noaa/storm_events/storm_events.schema.yaml",
            "destination": {"type": "iceberg"},
            "wire_version": 1,
        }
    )

    def _fake_scan(*_a: Any, **kwargs: Any) -> list[dict[str, Any]]:
        limit = int(kwargs["limit"])
        return [
            {
                "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
                "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
                "__extract_run_datetime": "2026-08-06T12:00:00+00:00",
                "i": i,
            }
            for i in range(limit)
        ]

    with (
        patch(
            "det.runtime.bronze_export.iceberg_writer.load_iceberg_table",
            return_value=object(),
        ),
        patch(
            "det.runtime.bronze_export.iceberg_writer.scan_iceberg_rows",
            side_effect=_fake_scan,
        ),
        patch(
            "det.runtime.bronze_export._EXPORT_ICEBERG_LIMIT",
            3,
        ),
        patch(
            "det.runtime.bronze_export.bronze_dataset_dir",
            return_value=Path("/tmp/bronze"),
        ),
        patch(
            "det.runtime.bronze_export.lake_root",
            return_value=Path("/tmp/lake"),
        ),
    ):
        with pytest.raises(ValueError, match="more than 3 rows"):
            load_bronze_export_rows(
                cfg,
                project_root=Path("/tmp"),
                interval_start="2026-08-06",
                interval_end="2026-08-07",
            )
