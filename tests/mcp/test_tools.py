from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.mcp.context import PathSandboxError
from det.mcp.tools import (
    biglake_register_dry_run,
    check,
    describe_pipeline,
    init_pipeline_dry_run,
    list_approvals,
    list_bronze_partitions,
    list_pipelines,
    list_raw_partitions,
    list_runs,
    list_sources_tool,
    prune_dry_run,
    read_manifest,
    scaffold_dbt_dry_run,
    summarize_runs,
)
from det.runtime.biglake_register import BigLakeRegisterPlan, BigLakeTablePlan
from det.runtime.meta import to_partition_value


def _write_pipeline(root: Path, canonical: str = "example_api.events") -> Path:
    provider, source = canonical.split(".", 1)
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True)
    schema_rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
    schema_path = root / schema_rel
    schema_path.parent.mkdir(parents=True)
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
    path = pipe_dir / f"{source}.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": canonical,
                "source": {"type": canonical},
                "schema": schema_rel,
                "ingestion": {"library": "det"},
                "destination": {"type": "filesystem", "path": "./data/lake"},
                "medallion": {"bronze_prefix": "bronze", "raw_prefix": "raw"},
            }
        ),
        encoding="utf-8",
    )
    return path


def _mk_run(base: Path, *, start: str, end: str, run: str) -> Path:
    run_dir = (
        base
        / f"__interval_start_datetime={to_partition_value(start)}"
        / f"__interval_end_datetime={to_partition_value(end)}"
        / f"__extract_run_datetime={to_partition_value(run)}"
    )
    run_dir.mkdir(parents=True)
    (run_dir / "data").mkdir()
    (run_dir / "meta").mkdir()
    (run_dir / "meta" / "manifest.json").write_text(
        json.dumps({"ok": True, "run": run}),
        encoding="utf-8",
    )
    (run_dir / "data.jsonl").write_text("{}\n", encoding="utf-8")
    return run_dir


def test_list_and_describe_pipeline(tmp_path: Path):
    _write_pipeline(tmp_path)
    listed = list_pipelines(root=tmp_path)
    assert listed["pipelines"] == ["example_api.events"]
    summary = describe_pipeline("example_api.events", root=tmp_path)
    assert summary["name"] == "example_api.events"
    assert summary["source"]["type"] == "example_api.events"
    assert summary["fs_dataset"] == "example_api/events_v1"
    assert summary["destination"]["sql_schema"] == "bronze_example_api"
    assert summary["destination"]["sql_table"] == "events_v1"
    assert summary["destination"]["type"] == "filesystem"
    assert "dbt" in summary


def test_list_sources_after_plugins(tmp_path: Path):
    sources = list_sources_tool(root=tmp_path)
    assert "example_api.events" in sources["sources"]
    assert "noaa.storm_events" in sources["sources"]
    assert sources.get("errors") == []


def test_raw_bronze_partitions_and_manifest(tmp_path: Path):
    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    raw = tmp_path / "data" / "lake" / "raw" / "example_api" / "events_v1"
    bronze = tmp_path / "data" / "lake" / "bronze" / "example_api" / "events_v1"
    r1 = _mk_run(raw, start=start, end=end, run="2026-08-06T10:00:00+00:00")
    _mk_run(raw, start=start, end=end, run="2026-08-06T11:00:00+00:00")
    _mk_run(bronze, start=start, end=end, run="2026-08-06T10:00:00+00:00")
    _mk_run(bronze, start=start, end=end, run="2026-08-06T11:00:00+00:00")
    _mk_run(bronze, start=start, end=end, run="2026-08-06T12:00:00+00:00")

    raw_listing = list_raw_partitions("example_api.events", root=tmp_path)
    assert len(raw_listing["runs"]) == 2
    bronze_listing = list_bronze_partitions("example_api.events", root=tmp_path)
    assert len(bronze_listing["runs"]) == 3

    manifest = read_manifest(str(r1.relative_to(tmp_path)), root=tmp_path)
    assert manifest["manifest"]["ok"] is True


def test_prune_dry_run_smoke(tmp_path: Path):
    _write_pipeline(tmp_path)
    start, end = "2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00"
    bronze = tmp_path / "data" / "lake" / "bronze" / "example_api" / "events_v1"
    runs = [
        "2026-08-06T10:00:00+00:00",
        "2026-08-06T11:00:00+00:00",
        "2026-08-06T12:00:00+00:00",
    ]
    dirs = [_mk_run(bronze, start=start, end=end, run=r) for r in runs]
    plan = prune_dry_run(
        "example_api.events",
        interval_start="2026-08-01",
        interval_end="2026-09-01",
        keep=1,
        root=tmp_path,
    )
    assert plan["remove_count"] == 2
    assert all(d.exists() for d in dirs)
    ap = plan["approval_plan"]
    assert ap["command"] == "prune"
    assert ap["argv"][:3] == ["prune", "-p", "example_api.events"]
    assert "--apply" in ap["argv"]
    assert len(ap["plan_digest"]) == 64
    assert "det approve" in ap["note"]


def test_scaffold_and_init_dry_run(tmp_path: Path):
    _write_pipeline(tmp_path)
    (tmp_path / "dbt" / "models" / "silver").mkdir(parents=True)
    (tmp_path / "dbt" / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")

    scaffold = scaffold_dbt_dry_run("example_api.events", root=tmp_path)
    assert scaffold["dry_run"] is True
    assert scaffold["dataset"] == "example_api.events_v1"
    assert scaffold["actions"]
    assert any(a["path"].endswith("ops_slo_expected.csv") for a in scaffold["actions"])
    assert not (tmp_path / "dbt" / "models" / "silver" / "stg_example_api__events.sql").exists()
    assert scaffold["approval_plan"]["command"] == "scaffold-dbt"

    init = init_pipeline_dry_run(
        "example_api.events",
        "example_api.events",
        root=tmp_path,
        skip_dbt=True,
    )
    assert init["dry_run"] is True
    assert init["name"] == "example_api.events"
    assert init["approval_plan"]["command"] == "init-pipeline"
    assert "--skip-dbt" in init["approval_plan"]["argv"]


def test_read_manifest_rejects_escape(tmp_path: Path):
    with pytest.raises(PathSandboxError):
        read_manifest(str(tmp_path.parent / "nope"), root=tmp_path)


def _write_postgres_pipeline(tmp_path: Path, **destination) -> None:
    _write_pipeline(tmp_path)
    pipe = tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    doc = yaml.safe_load(pipe.read_text(encoding="utf-8"))
    doc["destination"] = {"type": "postgres", "dataset": "bronze", **destination}
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")


def test_describe_pipeline_reports_the_secret_name(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DET_POSTGRES_DSN", "postgresql://det:hunter2pw@db/det")
    _write_postgres_pipeline(tmp_path, connection_env="DET_POSTGRES_DSN")
    described = describe_pipeline("example_api.events", root=tmp_path)
    assert described["destination"]["connection_env"] == "DET_POSTGRES_DSN"
    assert described["destination"]["connection"] == "env:DET_POSTGRES_DSN"
    assert "hunter2pw" not in json.dumps(described)


def test_describe_pipeline_never_echoes_a_literal_dsn(tmp_path: Path):
    _write_postgres_pipeline(tmp_path, connection="postgresql://det:hunter2pw@db/det")
    described = describe_pipeline("example_api.events", root=tmp_path)
    assert "hunter2pw" not in json.dumps(described)
    assert "connection_env" in described["destination"]["connection"]


def test_bronze_postgres_hint_never_echoes_a_dsn(tmp_path: Path):
    _write_postgres_pipeline(tmp_path, connection="postgresql://det:hunter2pw@db/det")
    listing = list_bronze_partitions("example_api.events", root=tmp_path)
    assert listing["destination_type"] == "postgres"
    assert "hunter2pw" not in json.dumps(listing)


def test_bronze_duckdb_hint(tmp_path: Path):
    _write_pipeline(tmp_path)
    pipe = tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    doc = yaml.safe_load(pipe.read_text(encoding="utf-8"))
    doc["destination"] = {
        "type": "duckdb",
        "path": "./data/lake",
        "connection": "./data/analytics.duckdb",
        "dataset": "bronze",
    }
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    listing = list_bronze_partitions("example_api.events", root=tmp_path)
    assert listing["destination_type"] == "duckdb"
    assert listing["runs"] == []
    assert listing["schema"] == "bronze_example_api"
    assert listing["table"] == "events_v1"
    assert "note" in listing


def test_list_runs_and_summarize_never_return_connection(tmp_path: Path):
    from datetime import UTC, datetime

    _write_pipeline(tmp_path)
    dt = datetime.now(UTC).date().isoformat()
    receipt_dir = tmp_path / "data" / "lake" / "runs" / f"dt={dt}" / "example_api.events"
    receipt_dir.mkdir(parents=True)
    (receipt_dir / "extract__x__1.json").write_text(
        json.dumps(
            {
                "pipeline": "example_api.events",
                "command": "extract",
                "status": "ok",
                "started_at": f"{dt}T12:00:00+00:00",
                "duration_ms": 12,
                "destination": "postgres",
                "connection": "postgresql://det:hunter2pw@db/det",
                "rows": 3,
            }
        ),
        encoding="utf-8",
    )
    listed = list_runs("example_api.events", root=tmp_path)
    blob = json.dumps(listed)
    assert "hunter2pw" not in blob
    assert "connection" not in listed["runs"][0]
    assert listed["runs"][0]["destination"] == "postgres"
    assert "authority" in listed["note"]
    summary = summarize_runs("example_api.events", root=tmp_path)
    assert summary["groups"][0]["ok"] == 1
    assert "hunter2pw" not in json.dumps(summary)


def test_list_runs_rejects_escape(tmp_path: Path):
    outside = tmp_path.parent / f"det-runs-escape-{tmp_path.name}.yaml"
    outside.write_text("name: nope\n", encoding="utf-8")
    try:
        with pytest.raises(PathSandboxError):
            list_runs(str(outside), root=tmp_path)
    finally:
        outside.unlink(missing_ok=True)


def test_check_ok_on_valid_pipeline(tmp_path: Path):
    _write_pipeline(tmp_path)
    payload = check("example_api.events", root=tmp_path)
    assert payload["ok"] is True
    assert payload["error_count"] == 0
    assert all(f["severity"] != "error" for f in payload["findings"])


def test_check_missing_schema(tmp_path: Path):
    _write_pipeline(tmp_path)
    schema = tmp_path / "schemas" / "example_api" / "events" / "events.schema.yaml"
    schema.unlink()
    payload = check("example_api.events", root=tmp_path)
    assert payload["ok"] is False
    assert any(f["code"] == "missing_schema" for f in payload["findings"])


def test_list_approvals_empty_on_tmp_root(tmp_path: Path):
    listed = list_approvals(root=tmp_path)
    assert listed["approvals"] == []
    assert listed["project_root"] == str(tmp_path.resolve())


def test_biglake_register_dry_run_returns_iam_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The success path must build an IAM hint and an approval plan.

    A gs:// lake cannot be reached from a unit test, so the plan builder is
    stubbed; ``build_iam_hint`` stays real so a missing import in the tool
    module fails here instead of at the agent.
    """
    plan = BigLakeRegisterPlan(
        project="proj",
        location="US",
        connection="det-lake-conn",
        lake_uri="gs://b/lake",
        tables=(
            BigLakeTablePlan(
                bq_dataset="bronze_example_api",
                bq_table="events_v1",
                table_location="gs://b/lake/bronze/example_api/events_v1",
                metadata_uri=(
                    "gs://b/lake/bronze/example_api/events_v1"
                    "/metadata/00001-abc.metadata.json"
                ),
                kind="bronze",
            ),
        ),
    )
    monkeypatch.setattr(
        "det.runtime.biglake_register.build_biglake_register_plan",
        lambda **kwargs: plan,
    )
    monkeypatch.setattr(
        "det.runtime.biglake_register._lookup_connection_sa",
        lambda *a, **k: None,
    )

    payload = biglake_register_dry_run(lake_path="gs://b/lake", root=tmp_path)

    assert payload["project"] == "proj"
    assert payload["lake_uri"] == "gs://b/lake"
    assert payload["tables"][0]["bq_table"] == "events_v1"
    assert payload["iam_hint"]["bucket"] == "b"
    assert payload["approval_plan"]["command"] == "biglake-register"
    assert "--apply" in payload["approval_plan"]["argv"]
    assert "no BigLake tables created" in payload["note"]
