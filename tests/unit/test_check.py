from __future__ import annotations

from pathlib import Path

import yaml

from det.runtime.check import check_pipeline_config, check_project, has_errors, has_warnings


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


def test_missing_dbt_models_warning(tmp_path: Path):
    path = _write_pipeline(tmp_path, with_dbt=True)
    findings = check_pipeline_config(path, project_root=tmp_path)
    assert not has_errors(findings)
    assert has_warnings(findings)
    assert any(f.code == "missing_dbt_models" for f in findings)


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
