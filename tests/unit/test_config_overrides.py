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


def test_load_pipeline_config(project_root):
    cfg = load_pipeline_config(project_root / "configs/pipelines/noaa/storm_events.yaml")
    assert cfg.name == "noaa.storm_events"
    assert cfg.source.type == "noaa.storm_events"
    assert cfg.ingestion.library == "det"
    assert cfg.destination.type == "filesystem"


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
