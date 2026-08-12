from __future__ import annotations

from pathlib import Path

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
    assert "read_json(" in text
    assert "bronze_example_api" in text
    pipe_text = pipe.read_text(encoding="utf-8")
    assert "wire_version: 1" in pipe_text
    stg = (
        tmp_path / "dbt" / "models" / "silver" / "stg_example_api__events.sql"
    ).read_text(encoding="utf-8")
    assert 'det_bronze_from("events_v1", "bronze_example_api")' in stg
