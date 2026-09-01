from __future__ import annotations

from pathlib import Path

import yaml

from det.runtime.check import check_pipeline_config, check_project, has_errors, has_warnings
from det.runtime.discovery import PluginLoadError


def _write_pipeline(
    root: Path,
    *,
    canonical: str = "example_api.events",
    schema_rel: str | None = None,
    source_type: str | None = None,
    write_schema: bool = True,
    with_dbt: bool = False,
) -> Path:
    provider, source = canonical.split(".", 1)
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True, exist_ok=True)
    rel = schema_rel or f"schemas/{provider}/{source}/{source}.schema.yaml"
    if write_schema:
        schema_path = root / rel
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
    path = pipe_dir / f"{source}.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": canonical,
                "source": {"type": source_type or canonical},
                "schema": rel,
                "ingestion": {"library": "thin"},
                "destination": {"type": "filesystem", "path": "./data/lake"},
            }
        ),
        encoding="utf-8",
    )
    if with_dbt:
        (root / "dbt" / "models" / "silver").mkdir(parents=True, exist_ok=True)
        (root / "dbt" / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")
    return path


def test_valid_pipeline_no_errors(tmp_path: Path):
    _write_pipeline(tmp_path)
    findings = check_project(tmp_path)
    assert not has_errors(findings)


def test_missing_schema_error(tmp_path: Path):
    _write_pipeline(tmp_path, write_schema=False)
    findings = check_project(tmp_path)
    assert has_errors(findings)
    assert any(f.code == "missing_schema" for f in findings)


def test_unknown_source_error(tmp_path: Path):
    pipe = tmp_path / "configs" / "pipelines" / "fake" / "source.yaml"
    pipe.parent.mkdir(parents=True)
    schema_rel = "schemas/fake/source/source.schema.yaml"
    schema_path = tmp_path / schema_rel
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(
        yaml.safe_dump(
            {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
        ),
        encoding="utf-8",
    )
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "fake.source",
                "source": {"type": "fake.source"},
                "schema": schema_rel,
                "destination": {"type": "filesystem", "path": "./data/lake"},
            }
        ),
        encoding="utf-8",
    )
    findings = check_project(tmp_path, pipeline="fake.source")
    assert has_errors(findings)
    assert any(f.code == "unknown_source" for f in findings)


def test_plugin_load_error(tmp_path: Path, monkeypatch):
    _write_pipeline(tmp_path)

    def boom(_name: str, *, project_root=None):  # noqa: ANN001
        raise PluginLoadError(
            "failed to import source 'example_api.events' from "
            "det.sources.example_api.events: boom",
            module="det.sources.example_api.events",
        )

    monkeypatch.setattr("det.runtime.check.get_source", boom)
    findings = check_project(tmp_path)
    assert has_errors(findings)
    assert any(f.code == "plugin_load_error" for f in findings)


def test_missing_dbt_models_warning(tmp_path: Path):
    path = _write_pipeline(tmp_path, with_dbt=True)
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not has_errors(findings)
    assert has_warnings(findings)
    assert any(f.code == "missing_dbt_models" for f in findings)


def test_scaffold_sql_stale_when_lookback_changes(tmp_path: Path):
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    path = _write_pipeline(tmp_path, with_dbt=True)
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["dbt"] = {
        "silver": {
            "materialized": "incremental",
            "unique_key": ["id"],
            "order_by": ["__extract_run_datetime desc"],
            "watermark": "__extract_run_datetime",
            "lookback": "3 days",
        }
    }
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    config = load_pipeline_config(path)
    scaffold_dbt(config, project_root=tmp_path, force=True)
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not any(f.code == "scaffold_sql_stale" for f in findings)

    doc["dbt"]["silver"]["lookback"] = "7 days"
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert any(f.code == "scaffold_sql_stale" for f in findings)
    assert has_warnings(findings)
    assert not has_errors(findings)

    config = load_pipeline_config(path)
    scaffold_dbt(config, project_root=tmp_path, force=True)
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not any(f.code == "scaffold_sql_stale" for f in findings)


def _write_pipeline_with(root: Path, **doc_updates) -> Path:
    path = _write_pipeline(root)
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc.update(doc_updates)
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return path


def test_passwordful_dsn_in_config_is_an_error(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        destination={
            "type": "postgres",
            "connection": "postgresql://det:hunter2pw@db/det",
            "dataset": "bronze",
        },
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert has_errors(findings)
    secret = [f for f in findings if f.code == "secret_in_config"]
    assert secret and "connection_env" in secret[0].detail
    assert "hunter2pw" not in secret[0].detail


def test_passwordless_dsn_in_config_is_a_warning(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        destination={
            "type": "postgres",
            "connection": "postgresql://db/det",
            "dataset": "bronze",
        },
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not has_errors(findings)
    assert any(
        f.code == "secret_in_config" and f.severity == "warning" for f in findings
    )


def test_connection_env_is_clean(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        destination={
            "type": "postgres",
            "connection_env": "DET_POSTGRES_DSN",
            "dataset": "bronze",
        },
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not any(f.code == "secret_in_config" for f in findings)


def test_duckdb_file_path_is_clean(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        destination={
            "type": "duckdb",
            "connection": "./data/analytics.duckdb",
            "dataset": "bronze",
        },
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not any(f.code == "secret_in_config" for f in findings)


def test_lake_userinfo_is_an_error(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        destination={"type": "filesystem", "path": "s3://AKIA:hunter2pw@bucket/lake"},
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert has_errors(findings)
    secret = [f for f in findings if f.code == "secret_in_config"]
    assert secret and "hunter2pw" not in secret[0].detail


def test_credential_literal_in_overrides_is_an_error(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        source={
            "type": "example_api.events",
            "overrides": {"headers": {"api_key": "tok-abc123"}},
        },
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert has_errors(findings)
    secret = [f for f in findings if f.code == "secret_in_config"]
    assert secret and "headers.api_key" in secret[0].detail
    assert "tok-abc123" not in secret[0].detail


def test_auth_env_name_in_overrides_is_clean(tmp_path: Path):
    path = _write_pipeline_with(
        tmp_path,
        source={
            "type": "example_api.events",
            "overrides": {"auth_env": "DET_EXAMPLE_API"},
        },
    )
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not any(f.code == "secret_in_config" for f in findings)


def test_repo_root_smoke():
    """Real project: no errors; example_api may warn about missing dbt models."""
    root = Path(__file__).resolve().parents[2]
    findings = check_project(root)
    assert not has_errors(findings)
    # example_api.events has no stg/silver in-repo today
    codes = {(f.pipeline, f.code) for f in findings if f.severity == "warning"}
    assert ("example_api.events", "missing_dbt_models") in codes or not any(
        f.pipeline == "example_api.events" for f in findings
    )


def test_lake_mode_mismatch_is_error(tmp_path: Path, monkeypatch):
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.setenv("DET_LAKE_PATH", "s3://bucket/det-lake")
    findings = check_project(tmp_path)
    assert has_errors(findings)
    assert any(f.code == "lake_mode_mismatch" for f in findings)


def test_lake_cloud_experimental_warning(tmp_path: Path, monkeypatch):
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.setenv("DET_LAKE_PATH", "s3://bucket/det-lake")
    monkeypatch.delenv("DET_ICEBERG_CATALOG", raising=False)
    monkeypatch.delenv("DET_ICEBERG_REST_URI", raising=False)
    monkeypatch.setenv("DET_REQUIRE_APPROVAL", "1")
    findings = check_project(tmp_path)
    assert not has_errors(findings)
    assert has_warnings(findings)
    cloud = [f for f in findings if f.code == "lake_cloud_experimental"]
    assert len(cloud) == 1
    assert "hadoop" in cloud[0].detail
    assert not any(f.code == "approval_recommended_cloud_lake" for f in findings)


def test_approval_recommended_cloud_lake_when_gate_off(
    tmp_path: Path, monkeypatch
) -> None:
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.setenv("DET_LAKE_PATH", "s3://bucket/det-lake")
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)
    monkeypatch.delenv("DET_ICEBERG_CATALOG", raising=False)
    monkeypatch.delenv("DET_ICEBERG_REST_URI", raising=False)
    findings = check_project(tmp_path)
    assert not has_errors(findings)
    assert any(f.code == "approval_recommended_cloud_lake" for f in findings)


def test_approval_recommended_cloud_lake_skipped_for_local(
    tmp_path: Path, monkeypatch
) -> None:
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "data" / "lake"))
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)
    findings = check_project(tmp_path)
    assert not any(f.code == "approval_recommended_cloud_lake" for f in findings)


def test_iceberg_rest_uri_missing_is_error(tmp_path: Path, monkeypatch):
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_ICEBERG_CATALOG", "rest")
    monkeypatch.delenv("DET_ICEBERG_REST_URI", raising=False)
    monkeypatch.delenv("DET_LAKE_MODE", raising=False)
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    findings = check_project(tmp_path)
    assert has_errors(findings)
    assert any(f.code == "iceberg_rest_uri_missing" for f in findings)


def test_iceberg_glue_requires_s3_is_error(tmp_path: Path, monkeypatch):
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_ICEBERG_CATALOG", "glue")
    monkeypatch.setenv("DET_LAKE_PATH", str(tmp_path / "data" / "lake"))
    monkeypatch.delenv("DET_LAKE_MODE", raising=False)
    findings = check_project(tmp_path)
    assert has_errors(findings)
    assert any(f.code == "iceberg_glue_requires_s3" for f in findings)


def test_iceberg_glue_requires_s3_uses_destination_path(
    tmp_path: Path, monkeypatch
):
    """Env lake may be s3:// while destination.path is local — register uses dest."""
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_ICEBERG_CATALOG", "glue")
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.setenv("DET_LAKE_PATH", "s3://bucket/det-lake")
    findings = check_project(tmp_path)
    assert has_errors(findings)
    glue = [f for f in findings if f.code == "iceberg_glue_requires_s3"]
    assert glue
    assert any(f.pipeline == "example_api.events" for f in glue)


def test_iceberg_rest_ok_with_uri(tmp_path: Path, monkeypatch):
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.setenv("DET_LAKE_PATH", "gs://bucket/det-lake")
    monkeypatch.setenv("DET_ICEBERG_CATALOG", "rest")
    monkeypatch.setenv(
        "DET_ICEBERG_REST_URI",
        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    )
    monkeypatch.setenv("DET_REQUIRE_APPROVAL", "1")
    findings = check_project(tmp_path)
    assert not has_errors(findings)
    cloud = [f for f in findings if f.code == "lake_cloud_experimental"]
    assert len(cloud) == 1
    assert "rest" in cloud[0].detail
    assert not any(f.code == "approval_recommended_cloud_lake" for f in findings)


def test_full_validate_gated_when_env_unset(tmp_path: Path, monkeypatch) -> None:
    _write_pipeline(tmp_path)
    monkeypatch.delenv("DET_ALLOW_FULL_VALIDATE", raising=False)
    findings = check_project(tmp_path)
    assert any(f.code == "full_validate_gated" for f in findings)


def test_full_validate_gated_skipped_when_env_set(tmp_path: Path, monkeypatch) -> None:
    _write_pipeline(tmp_path)
    monkeypatch.setenv("DET_ALLOW_FULL_VALIDATE", "1")
    findings = check_project(tmp_path)
    assert not any(f.code == "full_validate_gated" for f in findings)


def test_ingestion_library_dlt_deprecated_warning(tmp_path: Path):
    path = _write_pipeline(tmp_path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data["ingestion"] = {"library": "dlt"}
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not has_errors(findings)
    deprecated = [f for f in findings if f.code == "ingestion_library_dlt_deprecated"]
    assert len(deprecated) == 1
    assert deprecated[0].detail == (
        "ingestion.library: dlt is deprecated; use ingestion.library: det instead "
        "(the dlt alias will be removed in a future release)"
    )
