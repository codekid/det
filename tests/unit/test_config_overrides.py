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
    assert "delete+insert" in silver
    assert "incremental_strategy=" in silver


def test_bigquery_silver_config_defaults_granularity_day():
    from det.runtime.config import BigQueryPartitionConfig, BigQuerySilverConfig

    part = BigQueryPartitionConfig(field="__extract_run_datetime")
    assert part.data_type == "timestamp"
    assert part.granularity == "day"
    assert part.to_dbt_dict() == {
        "field": "__extract_run_datetime",
        "data_type": "timestamp",
        "granularity": "day",
    }
    bq = BigQuerySilverConfig(
        partition_by=part,
        cluster_by=["id"],
    )
    assert bq.has_layout()
    assert BigQuerySilverConfig().has_layout() is False


def test_bigquery_silver_config_int64_rejects_granularity():
    from pydantic import ValidationError

    from det.runtime.config import BigQueryPartitionConfig

    with pytest.raises(ValidationError, match="granularity"):
        BigQueryPartitionConfig(
            field="partition_id",
            data_type="int64",
            granularity="day",
        )
    part = BigQueryPartitionConfig(field="partition_id", data_type="int64")
    assert part.granularity is None
    assert part.to_dbt_dict() == {"field": "partition_id", "data_type": "int64"}


def test_bigquery_silver_config_rejects_cluster_by_over_four():
    from pydantic import ValidationError

    from det.runtime.config import BigQuerySilverConfig

    with pytest.raises(ValidationError, match="at most 4"):
        BigQuerySilverConfig(cluster_by=["a", "b", "c", "d", "e"])


def test_bigquery_silver_config_require_filter_needs_partition():
    from pydantic import ValidationError

    from det.runtime.config import BigQuerySilverConfig

    with pytest.raises(ValidationError, match="require_partition_filter"):
        BigQuerySilverConfig(require_partition_filter=True)


def test_bigquery_layout_rejected_on_view_materialized():
    from pydantic import ValidationError

    from det.runtime.config import DbtSilverConfig, RelationConfig

    with pytest.raises(ValidationError, match="not view"):
        DbtSilverConfig(
            materialized="view",
            unique_key=["id"],
            bigquery={"partition_by": {"field": "__extract_run_datetime"}},
        )
    with pytest.raises(ValidationError, match="not view"):
        RelationConfig(
            path="abilities",
            materialized="view",
            bigquery={"cluster_by": ["id"]},
        )


def test_silver_knobs_reject_unsafe_identifiers():
    from pydantic import ValidationError

    from det.runtime.config import DbtSilverConfig

    with pytest.raises(ValidationError, match="unique_key"):
        DbtSilverConfig(unique_key=["id; drop table"])
    with pytest.raises(ValidationError, match="order_by"):
        DbtSilverConfig(order_by=["id; drop"])
    with pytest.raises(ValidationError, match="watermark"):
        DbtSilverConfig(watermark="1 = 1")
    with pytest.raises(ValidationError, match="lookback"):
        DbtSilverConfig(lookback="7 days; select 1")
    with pytest.raises(ValidationError, match="lookback"):
        DbtSilverConfig(lookback="yesterday")
    ok = DbtSilverConfig(
        unique_key=["__row_hash", "event_id"],
        order_by=["event_id asc", "__extract_run_datetime desc"],
        watermark="__bronze_loaded_at",
        lookback="7 days",
    )
    assert ok.lookback == "7 days"
    assert ok.watermark == "__bronze_loaded_at"
