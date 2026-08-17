from __future__ import annotations

import pytest

from det.runtime.config import DbtDocsConfig, DbtStgConfig, PipelineConfig
from det.scaffold.dbt import (
    post_stg_description_map,
    schema_column_descriptions,
    schema_root_description,
)


def test_schema_root_and_column_descriptions():
    schema = {
        "description": "  Root blurb.  ",
        "properties": {
            "id": {"type": "string", "description": "Primary key"},
            "skip": {"type": "string"},
            "bad": {"type": "string", "description": 123},
        },
    }
    assert schema_root_description(schema) == "Root blurb."
    assert schema_column_descriptions(schema) == {"id": "Primary key"}


def test_post_stg_description_map_coalesce_rename_exclude():
    schema_descs = {
        "severity": "Legacy severity",
        "level": "Current severity",
        "debug_info": "Debug only",
        "state": "State code",
    }
    stg = DbtStgConfig(
        coalesce={"severity": ["severity", "severity_level", "level"]},
        rename={"severity": "event_severity"},
        exclude=["debug_info", "level", "severity_level"],
    )
    out = post_stg_description_map(schema_descs, stg)
    assert out["event_severity"] == "Legacy severity"
    assert "severity" not in out
    assert "level" not in out
    assert "debug_info" not in out
    assert out["state"] == "State code"


def test_post_stg_coalesce_inherits_first_source_desc():
    stg = DbtStgConfig(
        coalesce={"severity": ["severity", "level"]},
        rename={"severity": "event_severity"},
    )
    out = post_stg_description_map({"level": "From level"}, stg)
    assert out["event_severity"] == "From level"


def test_docs_config_validates_keys_and_values():
    DbtDocsConfig(columns={"event_severity": "ok"})
    with pytest.raises(ValueError, match="dbt.docs.columns"):
        DbtDocsConfig(columns={"BadName": "x"})
    with pytest.raises(ValueError, match="non-empty"):
        DbtDocsConfig(columns={"event_severity": "  "})


def test_pipeline_accepts_dbt_docs():
    cfg = PipelineConfig.model_validate(
        {
            "name": "example_api.events",
            "source": {"type": "example_api.events"},
            "schema": "schemas/example_api/events/events.schema.yaml",
            "dbt": {"docs": {"columns": {"event_severity": "Normalized severity"}}},
        }
    )
    assert cfg.dbt.docs.columns["event_severity"] == "Normalized severity"
