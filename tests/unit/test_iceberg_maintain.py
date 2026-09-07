"""Tests for destination.iceberg config and maintain plan API."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from det.runtime.config import (
    DestinationConfig,
    IcebergDestinationConfig,
    IcebergMaintainConfig,
)
from det.runtime.iceberg_maintain import (
    fleet_maintain_defaults,
    iter_iceberg_maintain_plans,
)


def test_iceberg_block_rejected_on_non_iceberg():
    with pytest.raises(ValidationError, match="destination.iceberg"):
        DestinationConfig(
            type="filesystem",
            iceberg=IcebergDestinationConfig(
                table_properties={"write.target-file-size-bytes": "1"}
            ),
        )


def test_iceberg_unknown_maintain_key_forbidden():
    with pytest.raises(ValidationError):
        IcebergMaintainConfig(compact=True)  # type: ignore[call-arg]


def test_iceberg_duration_validation():
    with pytest.raises(ValidationError, match="7d"):
        IcebergMaintainConfig(expire_older_than="week")
    ok = IcebergMaintainConfig(expire_older_than="14d", remove_orphans_older_than=None)
    assert ok.expire_older_than == "14d"
    assert ok.remove_orphans_older_than is None


def test_table_properties_stringify():
    block = IcebergDestinationConfig(table_properties={"n": 512})  # type: ignore[arg-type]
    assert block.table_properties == {"n": "512"}


def test_destination_iceberg_table_properties_helper():
    dest = DestinationConfig(
        type="iceberg",
        iceberg=IcebergDestinationConfig(
            table_properties={"write.target-file-size-bytes": "536870912"}
        ),
    )
    assert dest.iceberg_table_properties == {
        "write.target-file-size-bytes": "536870912"
    }
    assert DestinationConfig(type="iceberg").iceberg_table_properties == {}


def test_ensure_iceberg_table_passes_properties() -> None:
    pytest.importorskip("pyiceberg")
    from pyiceberg.exceptions import NoSuchTableError

    from det.ingestion.iceberg_writer import ensure_iceberg_table

    created: dict = {}

    class Restish:
        def load_table(self, identifier):  # noqa: ANN001
            raise NoSuchTableError("missing")

        def create_namespace(self, ns: str) -> None:
            created["ns"] = ns

        def create_table(self, identifier, **kwargs):  # noqa: ANN001
            created["ident"] = identifier
            created["properties"] = kwargs.get("properties")
            return MagicMock(name="table")

    ensure_iceberg_table(
        catalog=Restish(),
        identifier=("bronze_acme", "feed_v1"),
        location="file:///tmp/bronze/acme/feed_v1",
        columns=[("__row_hash", "STRING"), ("id", "INTEGER")],
        partition="none",
        table_properties={"write.target-file-size-bytes": "1048576"},
    )
    assert created["properties"] == {"write.target-file-size-bytes": "1048576"}


def _write_iceberg_pipeline(root: Path, *, name: str = "acme.feed", **iceberg: object) -> None:
    provider, source = name.split(".", 1)
    pipe_dir = root / "configs" / "pipelines" / provider
    pipe_dir.mkdir(parents=True, exist_ok=True)
    schema_rel = f"schemas/{provider}/{source}/{source}.schema.yaml"
    schema_path = root / schema_rel
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(
        yaml.safe_dump(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            }
        ),
        encoding="utf-8",
    )
    dest: dict = {"type": "iceberg", "path": "./data/lake"}
    if iceberg:
        dest["iceberg"] = iceberg
    (pipe_dir / f"{source}.yaml").write_text(
        yaml.safe_dump(
            {
                "name": name,
                "source": {"type": name},
                "schema": schema_rel,
                "destination": dest,
                "wire_version": 1,
            }
        ),
        encoding="utf-8",
    )


def test_fleet_maintain_defaults_from_env():
    fleet = fleet_maintain_defaults(
        {
            "DET_ICEBERG_MAINTAIN_EXPIRE_OLDER_THAN": "30d",
            "DET_ICEBERG_MAINTAIN_EXPIRE": "0",
            "DET_ICEBERG_MAINTAIN_REWRITE_DATA": "true",
        }
    )
    assert fleet.expire_older_than == "30d"
    assert fleet.expire is False
    assert fleet.rewrite_data is True


def test_iter_plans_skips_hadoop(tmp_path: Path):
    _write_iceberg_pipeline(tmp_path)
    plans = list(
        iter_iceberg_maintain_plans(
            tmp_path, environ={"DET_ICEBERG_CATALOG": "hadoop"}
        )
    )
    assert len(plans) == 1
    assert plans[0].actionable is False
    assert plans[0].skip_reason is not None
    assert "hadoop" in plans[0].skip_reason


def test_iter_plans_rest_actionable_and_pipeline_override(tmp_path: Path):
    _write_iceberg_pipeline(
        tmp_path,
        table_properties={"write.target-file-size-bytes": "2"},
        maintain={"expire": False, "expire_older_than": "1d"},
    )
    plans = list(
        iter_iceberg_maintain_plans(
            tmp_path,
            environ={
                "DET_ICEBERG_CATALOG": "rest",
                "DET_ICEBERG_MAINTAIN_EXPIRE_OLDER_THAN": "30d",
            },
        )
    )
    assert len(plans) == 1
    plan = plans[0]
    assert plan.actionable is True
    assert plan.skip_reason is None
    assert plan.table_properties == {"write.target-file-size-bytes": "2"}
    assert plan.maintain.expire is False
    assert plan.maintain.expire_older_than == "1d"
    assert plan.sql_relation.startswith("bronze_")
    dumped = plan.to_dict()
    assert dumped["maintain"]["expire"] is False


def test_iter_plans_inherits_fleet_when_maintain_omitted(tmp_path: Path):
    _write_iceberg_pipeline(
        tmp_path, table_properties={"write.format.default": "parquet"}
    )
    plans = list(
        iter_iceberg_maintain_plans(
            tmp_path,
            environ={
                "DET_ICEBERG_CATALOG": "glue",
                "DET_ICEBERG_MAINTAIN_EXPIRE_OLDER_THAN": "21d",
            },
        )
    )
    assert plans[0].maintain.expire_older_than == "21d"
    assert plans[0].catalog_kind == "glue"


def test_partial_maintain_override_preserves_fleet_fields(tmp_path: Path):
    _write_iceberg_pipeline(tmp_path, maintain={"expire": False})
    plans = list(
        iter_iceberg_maintain_plans(
            tmp_path,
            environ={
                "DET_ICEBERG_CATALOG": "rest",
                "DET_ICEBERG_MAINTAIN_EXPIRE_OLDER_THAN": "30d",
                "DET_ICEBERG_MAINTAIN_REWRITE_DATA": "true",
                "DET_ICEBERG_MAINTAIN_REMOVE_ORPHANS_OLDER_THAN": "5d",
            },
        )
    )
    maintain = plans[0].maintain
    assert maintain.expire is False
    assert maintain.expire_older_than == "30d"
    assert maintain.rewrite_data is True
    assert maintain.remove_orphans_older_than == "5d"
    assert maintain.rewrite_manifests is False
    assert maintain.z_order == []


def test_plan_mutation_isolated_from_fleet_and_siblings(tmp_path: Path):
    _write_iceberg_pipeline(tmp_path, name="acme.one")
    _write_iceberg_pipeline(tmp_path, name="acme.two")
    fleet = fleet_maintain_defaults(
        {"DET_ICEBERG_MAINTAIN_EXPIRE_OLDER_THAN": "9d"}
    )
    plans = list(
        iter_iceberg_maintain_plans(
            tmp_path,
            environ={"DET_ICEBERG_CATALOG": "rest"},
            fleet_defaults=fleet,
        )
    )
    assert len(plans) == 2
    first, second = plans[0], plans[1]
    assert first.maintain.expire_older_than == "9d"
    assert second.maintain.expire_older_than == "9d"

    first.maintain.z_order.append("col_a")
    with pytest.raises(TypeError):
        first.table_properties["x"] = "y"  # type: ignore[index]

    assert fleet.z_order == []
    assert second.maintain.z_order == []
    assert first.maintain.z_order == ["col_a"]
    assert first.maintain is not second.maintain
    assert first.maintain is not fleet
