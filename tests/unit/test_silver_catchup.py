"""Unit tests for bronze↔silver catch-up diff and manifest helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.destinations.models import to_partition_value
from det.errors import DetConflictError
from det.runtime.settings import DetSettings, use_settings
from det.runtime.config import load_pipeline_config
from det.runtime.silver_catchup import (
    catchup_select_from_manifest,
    catchup_vars_from_manifest,
    diff_bronze_silver,
    ensure_bq_catchup_external_table,
    list_silver_extract_runs,
    manifest_payload_from_catchup,
    plan_catchup_manifest,
    read_catchup_manifest,
    write_catchup_manifest,
)


def _write_pipeline(root: Path) -> None:
    provider, source = "example_api", "events"
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True, exist_ok=True)
    schema_rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
    schema_path = root / schema_rel
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "integer"}},
                "additionalProperties": False,
            }
        ),
        encoding="utf-8",
    )
    (pipe_dir / f"{source}.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "example_api.events",
                "source": {"type": "example_api.events"},
                "schema": schema_rel,
                "destination": {"type": "filesystem", "path": "./data/lake"},
                "wire_version": 1,
                "dbt": {
                    "silver": {
                        "materialized": "incremental",
                        "unique_key": ["__row_hash"],
                        "order_by": ["__extract_run_datetime desc"],
                        "watermark": "__extract_run_datetime",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _write_bronze_run(
    lake: Path,
    *,
    interval_start: str,
    interval_end: str,
    extract_run: str,
) -> Path:
    from det.runtime.manifest import write_manifest

    run_dir = (
        lake
        / "bronze"
        / "example_api"
        / "events_v1"
        / f"__interval_start_datetime={to_partition_value(interval_start)}"
        / f"__interval_end_datetime={to_partition_value(interval_end)}"
        / f"__extract_run_datetime={to_partition_value(extract_run)}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "data.jsonl").write_text(
        json.dumps({"id": 1, "__extract_run_datetime": extract_run}) + "\n",
        encoding="utf-8",
    )
    write_manifest(
        run_dir,
        {
            "interval_start": interval_start,
            "interval_end": interval_end,
            "extract_run_datetime": extract_run,
        },
    )
    return run_dir


def _silver_db(
    tmp_path: Path,
    runs: list[str] | list[tuple[str, str, str]],
) -> Path:
    """Build a silver DuckDB table.

    ``runs`` entries are either extract-run timestamps (interval defaults to a
    single day window) or ``(interval_start, interval_end, extract_run)`` tuples.
    """
    duckdb = pytest.importorskip("duckdb")
    db = tmp_path / "analytics.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create schema if not exists silver_example_api")
    con.execute(
        """
        create table silver_example_api.silver_example_api__events (
            id integer,
            __extract_run_datetime timestamptz,
            __interval_start_datetime timestamptz,
            __interval_end_datetime timestamptz,
            __row_hash varchar
        )
        """
    )
    for i, entry in enumerate(runs):
        if isinstance(entry, tuple):
            start, end, ts = entry
        else:
            ts = entry
            start = "2026-09-02T00:00:00+00:00"
            end = "2026-09-03T00:00:00+00:00"
        con.execute(
            """
            insert into silver_example_api.silver_example_api__events
            values (?, ?, ?, ?, ?)
            """,
            [i, ts, start, end, f"h{i}"],
        )
    con.close()
    return db


@pytest.fixture
def catchup_root(tmp_path: Path) -> Path:
    _write_pipeline(tmp_path)
    lake = tmp_path / "data" / "lake"
    lake.mkdir(parents=True)
    return tmp_path


def test_diff_hole_behind_max_watermark(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    early = "2026-09-02T12:08:00+00:00"
    late = "2026-09-02T12:10:00+00:00"
    early_start, early_end = "2026-09-01T00:00:00+00:00", "2026-09-02T00:00:00+00:00"
    late_start, late_end = "2026-09-02T00:00:00+00:00", "2026-09-03T00:00:00+00:00"
    _write_bronze_run(
        lake,
        interval_start=early_start,
        interval_end=early_end,
        extract_run=early,
    )
    _write_bronze_run(
        lake,
        interval_start=late_start,
        interval_end=late_end,
        extract_run=late,
    )
    db = _silver_db(catchup_root, [(late_start, late_end, late)])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        out = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
        )
    assert out["catchup_count"] == 1
    assert out["catchup_runs"][0]["extract_run_datetime"].startswith("2026-09-02T12:08")
    assert out["ok_count"] == 1
    assert out["stale_siblings_count"] == 0


def test_diff_same_extract_run_timestamp_distinct_intervals(
    catchup_root: Path, monkeypatch
):
    """Parallel intervals sharing a run clock must not false-negative catch-up."""
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    shared_ts = "2026-09-02T12:00:00+00:00"
    a_start, a_end = "2026-09-02T09:00:00+00:00", "2026-09-02T10:00:00+00:00"
    b_start, b_end = "2026-09-02T10:00:00+00:00", "2026-09-02T11:00:00+00:00"
    _write_bronze_run(
        lake, interval_start=a_start, interval_end=a_end, extract_run=shared_ts
    )
    _write_bronze_run(
        lake, interval_start=b_start, interval_end=b_end, extract_run=shared_ts
    )
    # Silver only covered interval A; interval B must still be a catch-up hole.
    db = _silver_db(catchup_root, [(a_start, a_end, shared_ts)])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        out = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
        )
    assert out["ok_count"] == 1
    assert out["catchup_count"] == 1
    hole = out["catchup_runs"][0]
    assert hole["interval_start"].startswith("2026-09-02T10:00")
    assert hole["extract_run_datetime"].startswith("2026-09-02T12:00")


def test_diff_latest_present_empty_catchup(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    run_a = "2026-09-02T10:00:00+00:00"
    run_b = "2026-09-02T12:00:00+00:00"
    start, end = "2026-09-02T00:00:00+00:00", "2026-09-03T00:00:00+00:00"
    _write_bronze_run(lake, interval_start=start, interval_end=end, extract_run=run_a)
    _write_bronze_run(lake, interval_start=start, interval_end=end, extract_run=run_b)
    db = _silver_db(catchup_root, [run_b])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        out = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
        )
    assert out["catchup_count"] == 0
    assert out["ok_count"] == 1
    assert out["stale_siblings_count"] == 1
    assert out["stale_siblings_ignored"][0]["extract_run_datetime"].startswith(
        "2026-09-02T10:00"
    )


def test_manifest_roundtrip_and_vars(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    payload = manifest_payload_from_catchup(
        [
            {
                "pipeline": "example_api.events",
                "extract_run_datetime": "2026-09-02T12:08:00+00:00",
                "interval_start": "2026-09-01T00:00:00+00:00",
                "interval_end": "2026-09-02T00:00:00+00:00",
            }
        ]
    )
    mid = payload["manifest_id"]
    with use_settings(settings):
        path = write_catchup_manifest(
            payload, project_root=catchup_root, settings=settings
        )
        loaded = read_catchup_manifest(
            manifest_id=mid, project_root=catchup_root, settings=settings
        )
    assert loaded is not None
    assert loaded["runs"][0]["pipeline"] == "example_api.events"
    assert loaded["manifest_id"] == mid
    assert path.name == f"{mid}.json"
    assert path.exists()
    runs_path = path.parent / f"{mid}.runs.jsonl"
    assert runs_path.exists()
    runs_lines = [
        __import__("json").loads(line)
        for line in runs_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert runs_lines == [
        {
            "extract_run_datetime": "2026-09-02T12:08:00+00:00",
            "interval_end": "2026-09-02T00:00:00+00:00",
            "interval_start": "2026-09-01T00:00:00+00:00",
            "pipeline": "example_api.events",
        }
    ]
    vars_map = catchup_vars_from_manifest(payload)
    assert vars_map["det_catchup"] is True
    assert vars_map["det_catchup_manifest_id"] == mid
    assert "det_catchup_by_pipeline" not in vars_map
    selects = catchup_select_from_manifest(payload, project_root=catchup_root)
    assert selects == ["silver_example_api__events"]

    with use_settings(settings):
        with pytest.raises(DetConflictError, match="already exists"):
            write_catchup_manifest(
                payload, project_root=catchup_root, settings=settings
            )
        still = read_catchup_manifest(
            manifest_id=mid, project_root=catchup_root, settings=settings
        )
    assert still == loaded


def test_write_catchup_manifest_sidecar_failure_leaves_no_commit(
    catchup_root: Path, monkeypatch
):
    """Failed .runs.jsonl write must not leave a committed scm JSON."""
    from unittest.mock import patch

    from det.runtime.lake import LakeRef

    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    payload = manifest_payload_from_catchup(
        [
            {
                "pipeline": "example_api.events",
                "extract_run_datetime": "2026-09-02T12:08:00+00:00",
                "interval_start": "2026-09-01T00:00:00+00:00",
                "interval_end": "2026-09-02T00:00:00+00:00",
            }
        ]
    )
    mid = payload["manifest_id"]
    scm = lake / "ops" / "silver_catchup" / f"{mid}.json"
    runs = lake / "ops" / "silver_catchup" / f"{mid}.runs.jsonl"
    real_exclusive = LakeRef.create_exclusive

    def _fail_runs_once(self: LakeRef, data: bytes) -> str:
        if self.name.endswith(".runs.jsonl"):
            raise OSError("simulated runs write failure")
        return real_exclusive(self, data)

    with use_settings(settings):
        with (
            patch.object(LakeRef, "create_exclusive", _fail_runs_once),
            pytest.raises(OSError, match="simulated runs write failure"),
        ):
            write_catchup_manifest(
                payload, project_root=catchup_root, settings=settings
            )
        assert not scm.exists()
        assert not runs.exists()
        # Retry without the failure injects a full publish.
        path = write_catchup_manifest(
            payload, project_root=catchup_root, settings=settings
        )
    assert path.exists()
    assert runs.exists()
    assert read_catchup_manifest(
        manifest_id=mid, project_root=catchup_root, settings=settings
    ) is not None


def test_write_catchup_manifest_recovers_identical_orphan_sidecar(
    catchup_root: Path, monkeypatch
):
    """Identical .runs.jsonl without scm JSON is recoverable; different is not."""
    from det.runtime.silver_catchup import catchup_content_digest, _runs_jsonl_bytes

    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    payload = manifest_payload_from_catchup(
        [
            {
                "pipeline": "example_api.events",
                "extract_run_datetime": "2026-09-02T12:08:00+00:00",
                "interval_start": "2026-09-01T00:00:00+00:00",
                "interval_end": "2026-09-02T00:00:00+00:00",
            }
        ]
    )
    mid = payload["manifest_id"]
    catchup_dir = lake / "ops" / "silver_catchup"
    catchup_dir.mkdir(parents=True)
    runs = catchup_dir / f"{mid}.runs.jsonl"
    runs.write_bytes(_runs_jsonl_bytes(payload["runs"]))
    assert not (catchup_dir / f"{mid}.json").exists()

    with use_settings(settings):
        path = write_catchup_manifest(
            payload, project_root=catchup_root, settings=settings
        )
        assert path.exists()
        loaded = read_catchup_manifest(
            manifest_id=mid, project_root=catchup_root, settings=settings
        )
    assert loaded is not None
    assert loaded["manifest_id"] == mid

    other = manifest_payload_from_catchup(
        [
            {
                "pipeline": "example_api.events",
                "extract_run_datetime": "2026-09-03T12:08:00+00:00",
                "interval_start": "2026-09-02T00:00:00+00:00",
                "interval_end": "2026-09-03T00:00:00+00:00",
            }
        ]
    )
    # Force same id so sidecar path collides with different bytes.
    other["manifest_id"] = mid
    other["content_digest"] = catchup_content_digest(other["runs"])
    # Remove commit marker so write reaches sidecar identity check.
    (catchup_dir / f"{mid}.json").unlink()
    with use_settings(settings):
        with pytest.raises(DetConflictError, match="runs NDJSON already exists"):
            write_catchup_manifest(
                other, project_root=catchup_root, settings=settings
            )


def test_plan_catchup_manifest_single(catchup_root: Path, monkeypatch):
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    run = "2026-09-02T12:08:00+00:00"
    _write_bronze_run(
        lake,
        interval_start="2026-09-01T00:00:00+00:00",
        interval_end="2026-09-02T00:00:00+00:00",
        extract_run=run,
    )
    db = _silver_db(catchup_root, [])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        planned = plan_catchup_manifest(
            project_root=catchup_root,
            pipeline="example_api.events",
            analytics_db=db,
        )
    assert planned["dry_run"] is True
    assert planned["manifest"]["runs"][0]["extract_run_datetime"].startswith(
        "2026-09-02T12:08"
    )
    assert planned["manifest_id"].startswith("scm_")
    assert planned["content_digest"].startswith("sha256:")
    assert planned["manifest_relpath"].endswith(f"{planned['manifest_id']}.json")


def test_plan_includes_all_catchup_rows_beyond_display_limit(
    catchup_root: Path, monkeypatch
):
    """Apply manifest must not drop holes that exceed the MCP display limit."""
    lake = catchup_root / "data" / "lake"
    monkeypatch.setenv("DET_LAKE_PATH", str(lake))
    n = 5
    for i in range(n):
        day = f"2026-09-{i + 1:02d}"
        _write_bronze_run(
            lake,
            interval_start=f"{day}T00:00:00+00:00",
            interval_end=f"2026-09-{i + 2:02d}T00:00:00+00:00",
            extract_run=f"2026-09-{i + 1:02d}T12:00:00+00:00",
        )
    db = _silver_db(catchup_root, [])
    settings = DetSettings.from_env(project_root=catchup_root).with_overrides(
        lake_override=str(lake)
    )
    with use_settings(settings):
        preview = diff_bronze_silver(
            "example_api.events",
            project_root=catchup_root,
            analytics_db=db,
            limit=2,
        )
        planned = plan_catchup_manifest(
            project_root=catchup_root,
            pipeline="example_api.events",
            analytics_db=db,
            limit=2,
        )
    assert preview["truncated"] is True
    assert len(preview["catchup_runs"]) == 2
    assert preview["catchup_count"] == 2  # bronze listing itself was clamped
    assert planned["diff"]["complete"] is True
    assert planned["diff"]["truncated"] is False
    assert len(planned["manifest"]["runs"]) == n
    assert planned["diff"]["catchup_count"] == n


def test_list_silver_extract_runs_bigquery_note_without_project(
    catchup_root: Path, monkeypatch
):
    monkeypatch.setenv("DET_DBT_TARGET", "bigquery")
    monkeypatch.delenv("DET_GCP_PROJECT", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    config = load_pipeline_config(
        catchup_root / "configs" / "pipelines" / "example_api" / "events.yaml"
    )
    keys, note = list_silver_extract_runs(config, project_root=catchup_root)
    assert keys == set()
    assert note is not None
    assert "BigQuery" in note
    assert "DET_GCP_PROJECT" in note


def test_list_silver_extract_runs_bigquery_mocked_client(
    catchup_root: Path, monkeypatch
):
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv("DET_DBT_TARGET", "bigquery")
    monkeypatch.setenv("DET_GCP_PROJECT", "proj-test")
    config = load_pipeline_config(
        catchup_root / "configs" / "pipelines" / "example_api" / "events.yaml"
    )

    class _Row(tuple):
        pass

    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        _Row(
            (
                "2026-09-01T00:00:00+00:00",
                "2026-09-02T00:00:00+00:00",
                "2026-09-02T12:08:00+00:00",
            )
        )
    ]
    with patch("google.cloud.bigquery.Client", return_value=mock_client):
        keys, note = list_silver_extract_runs(config, project_root=catchup_root)
    assert note is None
    assert keys == {
        (
            "2026-09-01T00:00:00+00:00",
            "2026-09-02T00:00:00+00:00",
            "2026-09-02T12:08:00+00:00",
        )
    }


def test_ensure_bq_catchup_external_table_requires_gs():
    with pytest.raises(ValueError, match="gs://"):
        ensure_bq_catchup_external_table(
            runs_uri="/tmp/local.runs.jsonl",
            manifest_id="scm_" + ("ab" * 8),
        )


def test_catchup_bq_relation_is_manifest_scoped():
    from det.runtime.silver_catchup import catchup_bq_relation

    mid = "scm_" + ("cd" * 8)
    assert catchup_bq_relation(
        project="proj", dataset="analytics", manifest_id=mid
    ) == f"`proj.analytics._det_catchup_runs_{mid}`"
