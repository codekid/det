"""Unit tests for BigLake registration plan builder."""

from __future__ import annotations

from pathlib import Path

import pytest

from det.ingestion.iceberg_catalog import hint_version_from_metadata_location
from det.runtime.biglake_register import (
    BigLakeRegisterPlan,
    _lake_bucket,
    build_biglake_register_plan,
    build_iam_hint,
    external_table_ddl,
    format_dry_run,
    format_iam_hint,
)
from det.runtime.lake import open_lake


def _write_iceberg_table(lake_root: Path, rel: str, metadata_name: str) -> None:
    table = lake_root / rel
    meta = table / "metadata"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / metadata_name).write_text("{}", encoding="utf-8")
    stem = hint_version_from_metadata_location(metadata_name)
    (meta / "version-hint.text").write_text(stem, encoding="utf-8")


def test_build_plan_requires_gs_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_GCP_PROJECT", "test-project")
    with pytest.raises(ValueError, match="gs://"):
        build_biglake_register_plan(
            project_root=tmp_path,
            lake_path=str(tmp_path / "lake"),
            project="test-project",
        )


def test_external_table_ddl():
    plan = BigLakeRegisterPlan(
        project="proj",
        location="US",
        connection="det-lake-conn",
        lake_uri="gs://b/lake",
        tables=(),
    )
    from det.runtime.biglake_register import BigLakeTablePlan

    table = BigLakeTablePlan(
        bq_dataset="bronze_example_api",
        bq_table="events_v1",
        table_location="gs://b/lake/bronze/example_api/events_v1",
        metadata_uri="gs://b/lake/bronze/example_api/events_v1/metadata/00001-abc.metadata.json",
        kind="bronze",
    )
    ddl = external_table_ddl(plan, table)
    assert "CREATE OR REPLACE EXTERNAL TABLE" in ddl
    assert "bronze_example_api.events_v1" in ddl
    assert "format = 'ICEBERG'" in ddl
    assert "00001-abc.metadata.json" in ddl


def test_bronze_table_plans_from_local_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    lake = open_lake(str(tmp_path / "lake"), tmp_path, lake_mode="local")
    table_dir = lake / "bronze" / "example_api" / "events_v1"
    meta = table_dir / "metadata"
    meta.mkdir(parents=True)
    (meta / "00001-abc.metadata.json").write_text("{}", encoding="utf-8")

    from det.runtime.biglake_register import _bronze_table_plans

    plans = _bronze_table_plans(lake, None)
    assert len(plans) == 1
    assert plans[0].bq_dataset == "bronze_example_api"
    assert plans[0].bq_table == "events_v1"


def test_metadata_uri_from_local_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    lake = open_lake(str(tmp_path / "lake"), tmp_path, lake_mode="local")
    table_dir = lake / "bronze" / "example_api" / "events_v1"
    meta = table_dir / "metadata"
    meta.mkdir(parents=True)
    (meta / "00001-abc.metadata.json").write_text("{}", encoding="utf-8")
    (meta / "version-hint.text").write_text("00001-abc", encoding="utf-8")

    from det.runtime.biglake_register import _metadata_uri_for_table

    uri = _metadata_uri_for_table(table_dir)
    assert uri.endswith("00001-abc.metadata.json")


def test_lake_bucket_parses_gs_uri():
    assert _lake_bucket("gs://b/lake") == "b"
    assert _lake_bucket("gs://my-bucket/det-lake") == "my-bucket"


def test_build_iam_hint_with_connection_sa(monkeypatch: pytest.MonkeyPatch):
    plan = BigLakeRegisterPlan(
        project="proj",
        location="US",
        connection="det-lake-conn",
        lake_uri="gs://b/lake",
        tables=(),
    )

    class FakeConn:
        service_account_id = "bqcx-123@test.iam.gserviceaccount.com"

    class FakeClient:
        def get_connection(self, name: str) -> FakeConn:
            assert name == "projects/proj/locations/US/connections/det-lake-conn"
            return FakeConn()

    monkeypatch.setattr(
        "google.cloud.bigquery.Client",
        lambda project: FakeClient(),
    )
    hint = build_iam_hint(plan)
    assert hint["bucket"] == "b"
    assert hint["connection_sa"] == "bqcx-123@test.iam.gserviceaccount.com"
    assert "gcloud storage buckets add-iam-policy-binding" in hint["gcloud_command"]
    assert "gs://b" in hint["gcloud_command"]
    assert "bqcx-123@test.iam.gserviceaccount.com" in hint["gcloud_command"]


def test_build_iam_hint_without_connection_sa(monkeypatch: pytest.MonkeyPatch):
    plan = BigLakeRegisterPlan(
        project="proj",
        location="US",
        connection="det-lake-conn",
        lake_uri="gs://b/lake",
        tables=(),
    )
    monkeypatch.setattr(
        "det.runtime.biglake_register._lookup_connection_sa",
        lambda *a, **k: None,
    )
    hint = build_iam_hint(plan)
    assert hint["bucket"] == "b"
    assert "connection_sa" not in hint
    assert "note" in hint
    assert "prerequisites" in hint["note"]


def test_format_dry_run_includes_iam_hint(monkeypatch: pytest.MonkeyPatch):
    plan = BigLakeRegisterPlan(
        project="proj",
        location="US",
        connection="det-lake-conn",
        lake_uri="gs://b/lake",
        tables=(),
    )
    monkeypatch.setattr(
        "det.runtime.biglake_register._lookup_connection_sa",
        lambda *a, **k: None,
    )
    out = format_dry_run(plan, ["biglake-register", "--apply"])
    assert "IAM hint:" in out
    assert "approval_plan:" in out
    assert format_iam_hint(plan) in out
