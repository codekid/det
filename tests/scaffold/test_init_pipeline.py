from __future__ import annotations

from pathlib import Path

import pytest

from det.scaffold.init_pipeline import init_pipeline


def test_init_pipeline_dry_run(tmp_path: Path):
    result = init_pipeline(
        name="example_api.events",
        source_type="example_api.events",
        project_root=tmp_path,
        dry_run=True,
    )
    assert result.name == "example_api.events"
    assert any(a.action == "would_write" for a in result.actions)
    assert not (
        tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    ).exists()


def test_init_pipeline_writes_and_scaffolds(tmp_path: Path):
    (tmp_path / "dbt" / "models" / "silver").mkdir(parents=True)
    result = init_pipeline(
        name="example_api.events",
        source_type="example_api.events",
        project_root=tmp_path,
    )
    pipe = tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    schema = tmp_path / "schemas" / "example_api" / "events" / "events.schema.yaml"
    assert pipe.exists()
    assert schema.exists()
    assert (
        tmp_path / "dbt" / "models" / "silver" / "stg_example_api__events.sql"
    ).exists()
    assert (
        tmp_path / "dbt" / "models" / "silver" / "silver_example_api__events.sql"
    ).exists()
    assert result.scaffold is not None
    text = (tmp_path / "dbt" / "models" / "silver" / "sources.yml").read_text(
        encoding="utf-8"
    )
    assert "iceberg_scan(" in text
    assert "bronze_example_api" in text
    pipe_text = pipe.read_text(encoding="utf-8")
    assert "wire_version: 1" in pipe_text
    dest_block = pipe_text.split("destination:", 1)[1].split("medallion:", 1)[0]
    assert "type: iceberg" in dest_block
    assert "path:" not in dest_block
    stg = (
        tmp_path / "dbt" / "models" / "silver" / "stg_example_api__events.sql"
    ).read_text(encoding="utf-8")
    assert 'det_bronze_from("events_v1", "bronze_example_api")' in stg


def test_init_pipeline_postgres_writes_connection_env(tmp_path: Path):
    result = init_pipeline(
        name="example_api.events",
        source_type="example_api.events",
        project_root=tmp_path,
        skip_dbt=True,
        destination_type="postgres",
        connection="DET_POSTGRES_DSN",
    )
    dest_block = (
        result.pipeline_path.read_text(encoding="utf-8")
        .split("destination:", 1)[1]
        .split("medallion:", 1)[0]
    )
    assert "connection_env: DET_POSTGRES_DSN" in dest_block
    assert "connection:" not in dest_block


def test_init_pipeline_refuses_a_passwordful_dsn(tmp_path: Path):
    with pytest.raises(ValueError, match="DET_POSTGRES_DSN"):
        init_pipeline(
            name="example_api.events",
            source_type="example_api.events",
            project_root=tmp_path,
            skip_dbt=True,
            destination_type="postgres",
            connection="postgresql://det:hunter2pw@db/det",
        )
    assert not (
        tmp_path / "configs" / "pipelines" / "example_api" / "events.yaml"
    ).exists()


def test_init_pipeline_filesystem_scaffolds_read_json(tmp_path: Path):
    (tmp_path / "dbt" / "models" / "silver").mkdir(parents=True)
    result = init_pipeline(
        name="example_api.events",
        source_type="example_api.events",
        project_root=tmp_path,
        destination_type="filesystem",
    )
    pipe_text = result.pipeline_path.read_text(encoding="utf-8")
    dest_block = pipe_text.split("destination:", 1)[1].split("medallion:", 1)[0]
    assert "type: filesystem" in dest_block
    text = (tmp_path / "dbt" / "models" / "silver" / "sources.yml").read_text(
        encoding="utf-8"
    )
    assert "read_json(" in text
    assert "**/data.jsonl" in text


def test_init_pipeline_iceberg_omits_path_and_scaffolds_scan(tmp_path: Path):
    (tmp_path / "dbt" / "models" / "silver").mkdir(parents=True)
    result = init_pipeline(
        name="example_api.events",
        source_type="example_api.events",
        project_root=tmp_path,
        destination_type="iceberg",
    )
    pipe_text = result.pipeline_path.read_text(encoding="utf-8")
    dest_block = pipe_text.split("destination:", 1)[1].split("medallion:", 1)[0]
    assert "type: iceberg" in dest_block
    assert "path:" not in dest_block
    text = (tmp_path / "dbt" / "models" / "silver" / "sources.yml").read_text(
        encoding="utf-8"
    )
    assert "iceberg_scan(" in text
    assert "**/data.jsonl" not in text
