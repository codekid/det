"""Public lake lifecycle APIs accept canonical pipeline ids."""

from __future__ import annotations

from pathlib import Path

import yaml

from det import (
    BronzeMigrator,
    BronzePruner,
    PipelineRunner,
    check_project,
    has_errors,
    inspect_lease,
    load_pipeline,
    release_lock,
)
from det.runtime.lease import force_release_lock, read_lock
from det.runtime.registry import clear_registries
from det.scaffold.init_source import init_source


def _project_with_source(tmp_path: Path) -> Path:
    clear_registries()
    init_source(
        name="acme.widgets",
        project_root=tmp_path,
        skip_dbt=True,
        destination_type="filesystem",
    )
    # Point destination at an absolute lake under tmp.
    pipe = tmp_path / "configs/pipelines/acme/widgets.yaml"
    doc = yaml.safe_load(pipe.read_text(encoding="utf-8"))
    doc["destination"]["path"] = str(tmp_path / "lake")
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return tmp_path


def test_load_pipeline_canonical_id(tmp_path: Path) -> None:
    root = _project_with_source(tmp_path)
    cfg = load_pipeline("acme.widgets", project_root=root)
    assert cfg.name == "acme.widgets"


def test_runner_accepts_canonical_id(tmp_path: Path) -> None:
    root = _project_with_source(tmp_path)
    result = PipelineRunner(root).run(
        "acme.widgets",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == 1
    assert result.raw_dir is not None


def test_check_and_pruner_canonical_id(tmp_path: Path) -> None:
    root = _project_with_source(tmp_path)
    findings = check_project(root, pipeline="acme.widgets")
    assert not has_errors(findings)

    runner = PipelineRunner(root)
    runner.run("acme.widgets", interval_start="2026-08-06", interval_end="2026-08-07")
    # Second extract run (new stamp) so prune has something to keep/remove.
    runner.extract(
        "acme.widgets",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        extract_run_datetime="2026-08-06T13:00:00+00:00",
    )
    runner.load(
        "acme.widgets",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        extract_run_datetime="2026-08-06T13:00:00+00:00",
    )

    pruner = BronzePruner(root)
    plan = pruner.plan(
        "acme.widgets",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        keep=1,
    )
    assert plan.keep == 1
    removed = pruner.apply("acme.widgets", plan)
    assert removed >= 0


def test_migrator_dry_run_canonical_id(tmp_path: Path) -> None:
    root = _project_with_source(tmp_path)
    PipelineRunner(root).run(
        "acme.widgets",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    schema = root / "schemas/acme/widgets/widgets.schema.yaml"
    plan = BronzeMigrator(root).migrate(
        pipeline="acme.widgets",
        to_bronze="acme.widgets_v2",
        schema_path=schema,
        mapper_name="identity",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        dry_run=True,
    )
    assert plan is not None


def test_lock_aliases_are_public() -> None:
    assert inspect_lease is read_lock
    assert release_lock is force_release_lock
