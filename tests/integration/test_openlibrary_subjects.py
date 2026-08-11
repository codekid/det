"""Open Library subjects pipeline: love works extract → bronze + dbt scaffold."""

from __future__ import annotations

import json
from pathlib import Path

from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config
from det.runtime.dbt_runner import default_select_for_pipeline
from det.runtime.runner import PipelineRunner
from det.scaffold.dbt import scaffold_dbt


def test_openlibrary_subjects_run_and_scaffold(tmp_path: Path, project_root: Path):
    load_plugins()
    lake = tmp_path / "lake"
    models = tmp_path / "dbt" / "models" / "silver"
    fixtures = json.loads(
        (project_root / "tests/fixtures/openlibrary/subjects_love.json").read_text(
            encoding="utf-8"
        )
    )

    schema_src = project_root / "schemas/openlibrary/subjects/subjects.schema.yaml"
    schema_dst = tmp_path / "schemas/openlibrary/subjects/subjects.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")

    pipe_dst = tmp_path / "configs/pipelines/openlibrary/subjects.yaml"
    pipe_dst.parent.mkdir(parents=True)
    pipe_dst.write_text(
        f"""
name: openlibrary.subjects
source:
  type: openlibrary.subjects
  overrides:
    subject: love
    fixture_records: {json.dumps(fixtures)}
schema: schemas/openlibrary/subjects/subjects.schema.yaml
destination:
  type: filesystem
  path: {lake.as_posix()}
dbt:
  silver:
    unique_key: [key, subject_key]
    order_by: ["__extract_run_datetime desc"]
    not_null: [key, subject_key]
  stg:
    exclude: [subject, ia_collection]
""",
        encoding="utf-8",
    )

    config = load_pipeline_config(pipe_dst)
    assert default_select_for_pipeline(config) == ["stg_openlibrary__subjects+"]

    result = scaffold_dbt(
        config, project_root=tmp_path, dbt_models_dir=models, warn=False, force=True
    )
    assert result.dataset == "openlibrary.subjects"
    stg = (models / "stg_openlibrary__subjects.sql").read_text(encoding="utf-8")
    assert "subject_key" in stg
    assert "availability__status" in stg
    assert " as subject," not in stg and " as subject\n" not in stg
    assert " as ia_collection" not in stg
    assert "authors as authors" in stg

    runner = PipelineRunner(tmp_path)
    out = runner.run(
        config,
        interval_start="2026-01-01T00:00:00Z",
        interval_end="2026-01-02T00:00:00Z",
    )
    assert out.rows == 2
    assert out.partition_dir is not None
    jsonl = Path(out.partition_dir) / "data.jsonl"
    rows = [
        json.loads(line)
        for line in jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 2
    assert rows[0]["key"] == "/works/OL21177W"
    assert rows[0]["subject_key"] == "/subjects/love"
    assert rows[0]["authors"][0]["name"] == "Emily Brontë"
    assert rows[0]["availability"]["status"] == "open"
