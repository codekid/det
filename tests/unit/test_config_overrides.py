from __future__ import annotations

import pytest

from det.runtime.config import apply_overrides, load_pipeline_config
from det.sources.base import merge_source_config
from det.sources.noaa.fatalities import NoaaFatalitiesSource
from det.sources.noaa.storm_events import NoaaStormEventsSource


def test_merge_overrides_win(tmp_path):
    defaults = NoaaStormEventsSource().defaults()
    merged = merge_source_config(defaults, {"filename_substr": "locations-ftp"})
    assert merged["filename_substr"] == "locations-ftp"
    assert merged["url"] == defaults["url"]


def test_fatalities_defaults_use_fatalities_substr():
    defaults = NoaaFatalitiesSource().defaults()
    assert defaults["filename_substr"] == "fatalities-ftp"
    assert defaults["url"] == NoaaStormEventsSource().defaults()["url"]


def test_locations_defaults_use_locations_substr():
    from det.sources.noaa.locations import NoaaLocationsSource

    defaults = NoaaLocationsSource().defaults()
    assert defaults["filename_substr"] == "locations-ftp"
    assert defaults["url"] == NoaaStormEventsSource().defaults()["url"]


def test_load_pipeline_config(project_root):
    cfg = load_pipeline_config(project_root / "configs/pipelines/noaa/storm_events.yaml")
    assert cfg.name == "noaa.storm_events"
    assert cfg.source.type == "noaa.storm_events"
    assert cfg.ingestion.library == "det"
    assert cfg.ingestion.chunk_rows == 10_000
    assert cfg.destination.type == "iceberg"
    assert cfg.destination.partition == "extract_run"
    assert cfg.destination.iceberg_partition == "extract_run"
    assert cfg.dbt.silver.materialized == "incremental"
    assert cfg.slo is not None
    assert cfg.slo.cadence == "daily"


def test_iceberg_partition_defaults_to_extract_run():
    from det.runtime.config import DestinationConfig

    dest = DestinationConfig(type="iceberg")
    assert dest.partition == "extract_run"
    assert dest.iceberg_partition == "extract_run"


def test_iceberg_partition_none_allowed():
    from det.runtime.config import DestinationConfig

    dest = DestinationConfig(type="iceberg", partition="none")
    assert dest.partition == "none"


def test_partition_rejected_on_non_iceberg():
    from pydantic import ValidationError

    from det.runtime.config import DestinationConfig

    with pytest.raises(ValidationError, match="partition"):
        DestinationConfig(type="filesystem", partition="none")
    with pytest.raises(ValidationError, match="partition"):
        DestinationConfig(
            type="duckdb", connection="./x.duckdb", partition="extract_run"
        )


def test_small_pipeline_yamls_use_partition_none(project_root):
    for rel in (
        "configs/pipelines/noaa/locations.yaml",
        "configs/pipelines/noaa/fatalities.yaml",
        "configs/pipelines/example_api/events.yaml",
        "configs/pipelines/example_api/orders.yaml",
    ):
        cfg = load_pipeline_config(project_root / rel)
        assert cfg.destination.type == "iceberg"
        assert cfg.destination.partition == "none"


def test_ingestion_chunk_rows_rejects_zero():
    from pydantic import ValidationError

    from det.runtime.config import IngestionConfig

    with pytest.raises(ValidationError):
        IngestionConfig(chunk_rows=0)


def test_cli_set_ingestion_chunk_rows(project_root):
    cfg = load_pipeline_config(
        project_root / "configs/pipelines/noaa/storm_events.yaml",
        overrides=["ingestion.chunk_rows=2"],
    )
    assert cfg.ingestion.chunk_rows == 2


def test_cli_set_overrides_apply_to_loaded_config(project_root):
    cfg = load_pipeline_config(
        project_root / "configs/pipelines/noaa/storm_events.yaml",
        overrides=[
            "ingestion.library=thin",
            "source.overrides.local_csv_dir=fixtures/storm_events",
        ],
    )
    assert cfg.ingestion.library == "thin"
    assert cfg.source.overrides["local_csv_dir"] == "fixtures/storm_events"
    assert cfg.source.type == "noaa.storm_events"


def test_override_values_are_parsed_as_yaml():
    raw = apply_overrides({}, ["a.b=3", "a.c=true", "a.d=null"])
    assert raw["a"] == {"b": 3, "c": True, "d": None}


def test_override_requires_assignment_form():
    with pytest.raises(ValueError):
        apply_overrides({}, ["ingestion.library"])


def test_in_tree_storm_events_silver_is_incremental(project_root):
    silver = (
        project_root / "dbt/models/silver/silver_noaa__storm_events.sql"
    ).read_text(encoding="utf-8")
    assert 'materialized="incremental"' in silver
    assert "is_incremental()" in silver
    assert "__extract_run_datetime >" in silver
    assert 'unique_key=["__row_hash"]' in silver
    assert 'incremental_strategy="delete+insert"' in silver
