"""det.testing helpers for plugin authors."""

from __future__ import annotations

from pathlib import Path

import pytest

from det.runtime.registry import clear_registries, get_source, list_sources
from det.scaffold.init_source import init_source
from det.testing import (
    TestProject,
    assert_no_dlt_artifacts,
    assert_raw_contract,
    extract_fixture,
    isolated_registries,
    records_from_fixture,
    register_source_for_tests,
    run_extract_load,
    secrets_map,
)


def test_secrets_map_lookup() -> None:
    lookup = secrets_map({"ACME_TOKEN": "secret", "EMPTY": None})
    assert lookup("ACME_TOKEN") == "secret"
    assert lookup("EMPTY") is None
    assert lookup("MISSING") is None


def test_register_source_for_tests_isolates() -> None:
    class StubSource:
        name = "testkit.stub"

        def defaults(self):
            return {"fixture_records": [{"id": 1}], "record_path": "data.records", "auth_env": None}

        def extract_to_raw(self, *, config, interval, data_dir):
            from det.sources.http_json import nest_under_path, write_json_page

            pages = data_dir / "pages"
            pages.mkdir(parents=True, exist_ok=True)
            return [
                write_json_page(
                    pages_dir=pages,
                    data_dir=data_dir,
                    page_num=1,
                    body=nest_under_path(
                        list(config["fixture_records"]),
                        record_path=config["record_path"],
                    ),
                    origin="fixture_records",
                )
            ]

        def records_from_raw(self, *, config, raw_dir, manifest):
            import json

            from det.sources.base import SourceRow
            from det.sources.http_json import dig

            for art in manifest.get("artifacts") or []:
                payload = json.loads((raw_dir / art["path"]).read_text(encoding="utf-8"))
                for row in dig(payload, config["record_path"]) or []:
                    yield SourceRow(data=row)

    with isolated_registries():
        register_source_for_tests("testkit.stub", StubSource)
        assert get_source("testkit.stub").name == "testkit.stub"
    # After exit, factory is gone (unless discovered elsewhere).
    clear_registries()
    with pytest.raises(Exception):
        get_source("testkit.stub")


def test_extract_fixture_roundtrip() -> None:
    clear_registries()
    # Use in-tree example with fixture overrides via extract_fixture rows.
    from det.sources.example_api.events import ExampleApiSource

    source = ExampleApiSource()
    fx = extract_fixture(
        source,
        rows=[
            {
                "id": "e1",
                "occurred_at": "2026-08-06T12:00:00Z",
                "severity": "low",
                "state": "TX",
                "status": "1",
            }
        ],
    )
    try:
        assert fx.artifacts
        rows = records_from_fixture(source, fixture=fx)
        assert len(rows) == 1
        assert rows[0].data["id"] == "e1"
    finally:
        fx.cleanup()


def test_write_minimal_pipeline_and_run_extract_load(tmp_path: Path) -> None:
    clear_registries()
    init_source(
        name="acme.widgets",
        project_root=tmp_path,
        skip_dbt=True,
        destination_type="filesystem",
    )
    # Re-point lake via TestProject helpers on the same root.
    proj = TestProject(tmp_path)
    # Overwrite pipeline lake path to TestProject convention if needed — init_source
    # already wrote a pipeline; rewrite with fixture rows for a known count.
    proj.write_minimal_pipeline(
        "acme.widgets",
        fixture_rows=[{"id": 1, "payload": "a"}, {"id": 2, "payload": "b"}],
    )
    assert "acme.widgets" in list_sources(project_root=tmp_path)

    result = run_extract_load(
        proj,
        "acme.widgets",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert result.rows == 2
    assert result.raw_dir is not None
    manifest = assert_raw_contract(result.raw_dir)
    assert manifest["source"] == "acme.widgets"
    assert_no_dlt_artifacts(result.raw_dir)
    assert_no_dlt_artifacts(result.partition_dir)


def test_assert_no_dlt_artifacts_rejects_key(tmp_path: Path) -> None:
    bad = tmp_path / "data" / "pages"
    bad.mkdir(parents=True)
    (bad / "0001.json").write_text('{"_dlt_id": "x", "id": 1}', encoding="utf-8")
    with pytest.raises(AssertionError, match="dlt-shaped key"):
        assert_no_dlt_artifacts(tmp_path)
