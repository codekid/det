from __future__ import annotations

import pytest

from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.ids import (
    dbt_model_slug,
    default_schema_path,
    fs_dataset_parts,
    fs_dataset_relpath,
    lake_dataset_id,
    parse_canonical_id,
    qualified_sql_table,
    sql_names_for_config,
    sql_schema_name,
    validate_canonical_id,
)


def test_parse_and_fs_parts():
    assert parse_canonical_id("noaa.storm_events") == ("noaa", "storm_events")
    assert fs_dataset_parts("noaa.storm_events") == ("noaa", "storm_events")
    assert fs_dataset_relpath("noaa.storm_events") == "noaa/storm_events"
    assert default_schema_path("noaa.storm_events") == (
        "schemas/noaa/storm_events/storm_events.schema.yaml"
    )
    assert dbt_model_slug("noaa.storm_events") == "noaa__storm_events"
    assert dbt_model_slug("example_api.events") == "example_api__events"


def test_lake_dataset_id_always_suffixes_wire_version():
    assert lake_dataset_id("noaa.locations", 1) == "noaa.locations_v1"
    assert lake_dataset_id("noaa.locations", 2) == "noaa.locations_v2"
    assert parse_canonical_id("noaa.locations_v1") == ("noaa", "locations_v1")
    with pytest.raises(ValueError, match="wire_version"):
        lake_dataset_id("noaa.locations", 0)


def test_sql_schema_is_medallion_provider():
    assert sql_schema_name("bronze", "noaa") == "bronze_noaa"
    assert qualified_sql_table("bronze", "noaa", "storm_events") == (
        "bronze_noaa.storm_events"
    )


def test_sql_names_for_config():
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
        destination=DestinationConfig(type="duckdb", connection="./x.duckdb", dataset="bronze"),
        wire_version=1,
    )
    assert config.bronze_dataset() == "noaa.storm_events_v1"
    assert sql_names_for_config(config) == ("bronze_noaa", "storm_events_v1")


def test_rejects_flat_names():
    with pytest.raises(ValueError, match="provider.source"):
        validate_canonical_id("storm_events")


def test_pipeline_name_must_match_source_type():
    with pytest.raises(ValueError, match="must equal source.type"):
        PipelineConfig(
            name="noaa.storm_events",
            source=SourceConfig(type="example_api.events"),
            schema_path="schemas/x.yaml",
        )


def test_schema_defaults_when_omitted():
    config = PipelineConfig.model_validate(
        {
            "name": "noaa.storm_events",
            "source": {"type": "noaa.storm_events"},
        }
    )
    assert config.schema_path == "schemas/noaa/storm_events/storm_events.schema.yaml"


def test_pipeline_rejects_top_level_dataset():
    with pytest.raises(ValueError, match="dataset:"):
        PipelineConfig(
            name="noaa.storm_events",
            source=SourceConfig(type="noaa.storm_events"),
            schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
            dataset="noaa.storm_events_v2",
            destination=DestinationConfig(type="filesystem", path="./data/lake"),
            medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
            ingestion=IngestionConfig(library="det"),
        )
