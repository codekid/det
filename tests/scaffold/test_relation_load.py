"""Tests for dbt.stg.relations.*.load materialization mapping."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from det.runtime.config import RelationConfig, load_pipeline_config
from det.scaffold.dbt import scaffold_dbt
from det.scaffold.dbt_sql import SpineEntry, relation_chain_for, spine_for_relation
from det.scaffold.relation_load import (
    relation_dedupe_key,
    relation_delete_key,
    resolve_relation_materialization,
)


def test_resolve_legacy_materialized_only() -> None:
    rel = RelationConfig(materialized="table")
    mat = resolve_relation_materialization(rel)
    assert mat.stg_materialized == "table"
    assert mat.silver_materialized == "table"
    assert mat.incremental_strategy is None
    assert not mat.is_parent_replace


def test_resolve_full_refresh() -> None:
    rel = RelationConfig(load="full_refresh")
    mat = resolve_relation_materialization(rel)
    assert mat.stg_materialized == "view"
    assert mat.silver_materialized == "table"
    assert mat.incremental_strategy is None


def test_resolve_parent_replace_uses_parent_strategy() -> None:
    rel = RelationConfig(load="parent_replace")
    mat = resolve_relation_materialization(rel, incremental_strategy="merge")
    assert mat.stg_materialized == "view"
    assert mat.silver_materialized == "incremental"
    assert mat.incremental_strategy == "merge"
    assert mat.is_parent_replace


def test_load_parent_replace_rejects_materialized_table() -> None:
    with pytest.raises(ValueError, match="parent_replace conflicts"):
        RelationConfig(load="parent_replace", materialized="table")


def test_load_full_refresh_allows_materialized_table() -> None:
    rel = RelationConfig(load="full_refresh", materialized="table")
    assert rel.load == "full_refresh"
    mat = resolve_relation_materialization(rel)
    assert mat.silver_materialized == "table"


def test_delete_key_excludes_self_grain() -> None:
    spine = [
        SpineEntry(name="line_items__sku", level_idx=0, kind="grain", field="sku"),
        SpineEntry(
            name="line_items__tax_lines__title",
            level_idx=1,
            kind="grain",
            field="title",
        ),
        SpineEntry(
            name="line_items__tax_lines__rate",
            level_idx=1,
            kind="grain",
            field="rate",
        ),
    ]
    assert relation_dedupe_key("id", spine) == [
        "id",
        "line_items__sku",
        "line_items__tax_lines__title",
        "line_items__tax_lines__rate",
    ]
    assert relation_delete_key("id", spine) == ["id", "line_items__sku"]


def test_delete_key_top_level_is_parent_only() -> None:
    spine = [
        SpineEntry(name="line_items__sku", level_idx=0, kind="grain", field="sku"),
    ]
    assert relation_delete_key("id", spine) == ["id"]
    assert relation_dedupe_key("id", spine) == ["id", "line_items__sku"]


def test_scaffold_parent_replace_and_full_refresh(
    tmp_path: Path, project_root: Path
) -> None:
    schema_src = project_root / "schemas/example_api/orders/orders.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/orders/orders.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")

    pipe = tmp_path / "configs/pipelines/example_api/orders.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text(
        yaml.safe_dump(
            {
                "name": "example_api.orders",
                "source": {"type": "example_api.orders"},
                "schema": "schemas/example_api/orders/orders.schema.yaml",
                "destination": {"type": "filesystem", "path": str(tmp_path / "lake")},
                "dbt": {
                    "silver": {
                        "unique_key": ["id"],
                        "order_by": ["__extract_run_datetime desc"],
                        "incremental_strategy": "delete+insert",
                    },
                    "stg": {
                        "relations": {
                            "discount_codes": {
                                "load": "full_refresh",
                                "parent_key": "id",
                            },
                            "line_items": {
                                "load": "parent_replace",
                                "parent_key": "id",
                                "grain": ["sku"],
                                "relations": {
                                    "tax_lines": {
                                        "load": "parent_replace",
                                        "parent_key": "id",
                                        "grain": ["title", "rate"],
                                    }
                                },
                            },
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    config = load_pipeline_config(pipe)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models, warn=False, force=True)

    stg_disc = (models / "stg_example_api__orders__discount_codes.sql").read_text(
        encoding="utf-8"
    )
    sil_disc = (models / "silver_example_api__orders__discount_codes.sql").read_text(
        encoding="utf-8"
    )
    assert 'materialized="view"' in stg_disc
    assert 'materialized="table"' in sil_disc
    assert "det_catchup" not in sil_disc

    stg_li = (models / "stg_example_api__orders__line_items.sql").read_text(
        encoding="utf-8"
    )
    sil_li = (models / "silver_example_api__orders__line_items.sql").read_text(
        encoding="utf-8"
    )
    assert 'materialized="view"' in stg_li
    assert 'materialized="incremental"' in sil_li
    assert "det_catchup" in sil_li
    assert "det_silver_incremental_filter" in sil_li
    assert 'unique_key=["id"]' in sil_li
    # parent_replace: BQ must delete+insert (not merge) so vanished children drop
    assert (
        "incremental_strategy='delete+insert' if target.name == 'bigquery' "
        "else 'delete+insert'"
    ) in sil_li
    assert "'merge' if target.name == 'bigquery'" not in sil_li
    assert "partition_by=" in sil_li
    assert "line_items__sku" in sil_li

    sil_tax = (
        models / "silver_example_api__orders__line_items__tax_lines.sql"
    ).read_text(encoding="utf-8")
    assert 'materialized="incremental"' in sil_tax
    # delete key = parent + ancestor spine (line_items__sku), not self grain
    assert 'unique_key=["id", "line_items__sku"]' in sil_tax
    assert "line_items__tax_lines__title" in sil_tax  # in dedupe partition_by

    # YAML tests use dedupe grain, not delete_key (would wrongly unique-test parent id)
    yml = yaml.safe_load(
        (models / "_silver__models.yml").read_text(encoding="utf-8")
    )
    li_model = next(
        m
        for m in yml["models"]
        if m["name"] == "silver_example_api__orders__line_items"
    )
    li_cols = {c["name"]: c for c in li_model["columns"]}
    assert "id" in li_cols and "line_items__sku" in li_cols
    assert "unique" not in li_cols["id"].get("tests", [])
    assert "not_null" in li_cols["line_items__sku"]["tests"]

    tax_model = next(
        m
        for m in yml["models"]
        if m["name"] == "silver_example_api__orders__line_items__tax_lines"
    )
    tax_cols = {c["name"]: c for c in tax_model["columns"]}
    assert "line_items__tax_lines__title" in tax_cols
    assert "not_null" in tax_cols["line_items__tax_lines__title"]["tests"]


def test_spine_helpers_align_with_chain() -> None:
    rels = {
        "line_items": RelationConfig(
            load="parent_replace",
            grain=["sku"],
            relations={
                "tax_lines": RelationConfig(
                    load="parent_replace", grain=["title", "rate"]
                )
            },
        )
    }
    name_parts = ["line_items", "tax_lines"]
    chain = relation_chain_for(rels, name_parts)
    spine = spine_for_relation(name_parts, chain)
    assert relation_delete_key("id", spine) == ["id", "line_items__sku"]
