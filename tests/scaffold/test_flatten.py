from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from det.runtime.config import DbtStgConfig, FlattenConfig, load_pipeline_config
from det.scaffold.dbt import (
    duckdb_type_for_prop,
    read_json_columns_from_schema,
    relation_stg_columns,
    scaffold_dbt,
    stg_columns_from_schema,
)
from det.scaffold.flatten import (
    detect_flatten_collisions,
    path_parts_to_column,
    plan_flatten,
)
from det.scaffold.view_warn import collect_view_size_warnings
from det.validation.jsonschema_validator import load_json_schema

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "example_api"
    / "orders_nested.schema.yaml"
)


def _nested_schema() -> dict:
    return load_json_schema(FIXTURE)


def test_path_parts_to_column():
    assert path_parts_to_column(("shipping_address", "city")) == "shipping_address__city"
    assert (
        path_parts_to_column(("shipping_address", "geo", "lat"))
        == "shipping_address__geo__lat"
    )


def test_plan_flatten_unlimited_and_depth_one():
    schema = _nested_schema()
    unlimited = plan_flatten(schema, FlattenConfig())
    names = {leaf.column_name for leaf in unlimited}
    assert "shipping_address__city" in names
    assert "shipping_address__geo__coords__lat" in names
    assert "customer__loyalty__tier__name" in names
    assert "customer__default_address__geo__coords__lon" in names
    assert "email" not in names
    assert "sku" not in names

    depth1 = plan_flatten(schema, FlattenConfig(depth=1))
    d1 = {leaf.column_name for leaf in depth1}
    assert "shipping_address__city" in d1
    assert "shipping_address__geo__coords__lat" not in d1
    assert "customer__id" in d1
    assert "customer__loyalty__tier__name" not in d1


def test_plan_flatten_skips_relation_paths_and_respects_include_exclude():
    schema = _nested_schema()
    leaves = plan_flatten(
        schema,
        FlattenConfig(include=["shipping_address"]),
        relation_paths={"line_items"},
    )
    assert all(leaf.root == "shipping_address" for leaf in leaves)

    excluded = plan_flatten(
        schema, FlattenConfig(exclude=["shipping_address", "customer"])
    )
    assert excluded == []


def test_collision_raises():
    schema = _nested_schema()
    leaves = plan_flatten(schema, FlattenConfig())
    with pytest.raises(ValueError, match="collides"):
        detect_flatten_collisions(leaves, reserved={"shipping_address__city"})


def test_object_array_read_json_types_are_json():
    schema = _nested_schema()
    cols = read_json_columns_from_schema(schema)
    assert cols["shipping_address"] == "JSON"
    assert cols["line_items"] == "JSON"
    assert cols["id"] == "VARCHAR"
    assert duckdb_type_for_prop(schema["properties"]["shipping_address"]) == "JSON"


def test_stg_columns_flatten_and_omit_relation_array():
    schema = _nested_schema()
    stg = DbtStgConfig.model_validate(
        {
            "relations": {"line_items": {"path": "line_items", "materialized": "view"}},
        }
    )
    cols = stg_columns_from_schema(schema, stg)
    by_name = {c["name"]: c["expr"] for c in cols}
    assert "id" in by_name
    assert "email" in by_name
    assert "line_items" not in by_name
    assert "shipping_address" not in by_name
    assert "shipping_address__city" in by_name
    assert "det_json_path_string('shipping_address', '$.city')" in by_name[
        "shipping_address__city"
    ]
    assert "shipping_address__geo__coords__lat" in by_name
    assert "customer__loyalty__tier__name" in by_name


def test_relation_stg_columns_unnest_paths():
    schema = _nested_schema()
    stg = DbtStgConfig.model_validate(
        {
            "relations": {
                "line_items": {
                    "path": "line_items",
                    "relations": {"tax_lines": {"path": "tax_lines"}},
                }
            }
        }
    )
    rel = stg.relations["line_items"]
    cols = relation_stg_columns(
        schema,
        path_chain=["line_items"],
        relation=rel,
        parent_key="id",
        stg=stg,
    )
    by_name = {c["name"]: c["expr"] for c in cols}
    assert "id" in by_name
    assert "__rel_index" in by_name
    assert "sku" in by_name
    assert "det_json_path_string('_rel', '$.sku')" in by_name["sku"]
    assert "variant__id" in by_name
    assert "variant__product__category__name" in by_name
    assert (
        "det_json_path_string('_rel', '$.variant.product.category.name')"
        in by_name["variant__product__category__name"]
    )
    assert "price_set__shop_money__amount" in by_name

    tax = stg.relations["line_items"].relations["tax_lines"]
    tax_cols = relation_stg_columns(
        schema,
        path_chain=["line_items", "tax_lines"],
        relation=tax,
        parent_key="id",
        stg=stg,
    )
    tax_by = {c["name"]: c["expr"] for c in tax_cols}
    assert "__rel_index_0" in tax_by
    assert "__rel_index_1" in tax_by
    assert "title" in tax_by
    assert "price_set__shop_money__amount" in tax_by


def test_stg_columns_scoped_fields_and_leaf_coalesce():
    schema = _nested_schema()
    stg = DbtStgConfig.model_validate(
        {
            "fields": {
                "shipping_address": {
                    "null_sentinels": {"city": ["", "NA"]},
                    "rename": {"city": "ship_city"},
                    "geo": {
                        "coords": {
                            "rename": {"lat": "ship_lat", "lon": "ship_lon"},
                            "coalesce": {
                                "lat": ["lat", "lon"],
                            },
                        }
                    },
                },
                "customer": {
                    "loyalty": {
                        "tier": {
                            "rename": {"name": "loyalty_tier"},
                        }
                    }
                },
            },
            "relations": {
                "line_items": {
                    "path": "line_items",
                    "rename": {"sku": "line_sku"},
                    "not_null": ["sku"],
                    "variant": {
                        "product": {
                            "category": {
                                "rename": {"name": "category"},
                                "not_null": ["name"],
                            }
                        }
                    },
                    "relations": {
                        "tax_lines": {
                            "path": "tax_lines",
                            "not_null": ["title", "rate"],
                            "price_set": {
                                "shop_money": {
                                    "rename": {"amount": "tax_amount"},
                                }
                            },
                        }
                    },
                }
            },
        }
    )
    cols = stg_columns_from_schema(schema, stg)
    by_name = {c["name"]: c["expr"] for c in cols}
    assert "shipping_address__city" not in by_name
    assert "shipping_address__ship_city" in by_name
    assert "nullif(" in by_name["shipping_address__ship_city"]
    assert "shipping_address__geo__coords__lat" not in by_name
    assert "shipping_address__geo__coords__ship_lat" in by_name
    lat_expr = by_name["shipping_address__geo__coords__ship_lat"]
    assert "coalesce(" in lat_expr
    assert "shipping_address__geo__coords__ship_lon" in by_name

    assert "customer__loyalty__tier__name" not in by_name
    assert "customer__loyalty__tier__loyalty_tier" in by_name

    rel = stg.relations["line_items"]
    rel_cols = relation_stg_columns(
        schema,
        path_chain=["line_items"],
        relation=rel,
        parent_key="id",
        stg=stg,
    )
    rel_by = {c["name"]: c["expr"] for c in rel_cols}
    assert "sku" not in rel_by
    assert "line_sku" in rel_by
    assert "variant__product__category__name" not in rel_by
    assert "variant__product__category__category" in rel_by

    tax = rel.relations["tax_lines"]
    tax_cols = relation_stg_columns(
        schema,
        path_chain=["line_items", "tax_lines"],
        relation=tax,
        parent_key="id",
        stg=stg,
    )
    tax_by = {c["name"]: c["expr"] for c in tax_cols}
    assert "price_set__shop_money__amount" not in tax_by
    assert "price_set__shop_money__tax_amount" in tax_by


def test_scaffold_writes_relation_models(tmp_path: Path):
    schema_path = tmp_path / "schemas" / "example_api" / "orders" / "orders.schema.yaml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    pipeline = tmp_path / "configs" / "pipelines" / "example_api" / "orders.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: example_api.orders
source:
  type: example_api.orders
schema: schemas/example_api/orders/orders.schema.yaml
dbt:
  silver:
    unique_key: [id]
    order_by: ["__extract_run_datetime desc"]
  stg:
    relations:
      discount_codes:
        materialized: view
      line_items:
        materialized: view
        relations:
          tax_lines:
            materialized: view
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models, warn=False)

    parent = (models / "stg_example_api__orders.sql").read_text(encoding="utf-8")
    assert "shipping_address__geo__coords__lat" in parent
    assert "customer__loyalty__tier__name" in parent
    assert " as line_items" not in parent
    assert " as discount_codes" not in parent

    child = (models / "stg_example_api__orders__line_items.sql").read_text(
        encoding="utf-8"
    )
    assert 'materialized="view"' in child
    assert "unnest(cast(_parent.line_items as JSON[]))" in child
    assert "with ordinality" in child
    assert "variant__product__category__name" in child
    assert "price_set__shop_money__amount" in child

    tax = (models / "stg_example_api__orders__line_items__tax_lines.sql").read_text(
        encoding="utf-8"
    )
    assert "json_extract(t0._rel, '$.tax_lines')" in tax
    assert "__rel_index_0" in tax
    assert "__rel_index_1" in tax
    assert "price_set__shop_money__amount" in tax

    discounts = (models / "stg_example_api__orders__discount_codes.sql").read_text(
        encoding="utf-8"
    )
    assert "unnest(cast(_parent.discount_codes as JSON[]))" in discounts

    silver_child = (models / "silver_example_api__orders__line_items.sql").read_text(
        encoding="utf-8"
    )
    assert 'materialized="view"' in silver_child
    assert 'ref("stg_example_api__orders__line_items")' in silver_child
    assert 'partition_by=["id", "__rel_index"]' in silver_child


def test_scaffold_scoped_adapts_and_relation_silver_tests(tmp_path: Path):
    schema_path = tmp_path / "schemas" / "example_api" / "orders" / "orders.schema.yaml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    pipeline = tmp_path / "configs" / "pipelines" / "example_api" / "orders.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: example_api.orders
source:
  type: example_api.orders
schema: schemas/example_api/orders/orders.schema.yaml
dbt:
  silver:
    unique_key: [id]
    order_by: ["__extract_run_datetime desc"]
  stg:
    fields:
      shipping_address:
        rename:
          city: ship_city
        null_sentinels:
          city: ["", "NA"]
        geo:
          coords:
            rename:
              lat: ship_lat
    relations:
      line_items:
        materialized: view
        rename:
          sku: line_sku
        not_null: [sku, quantity]
        accepted_values:
          sku: [SKU-A, SKU-B]
        variant:
          product:
            category:
              rename:
                name: category
              not_null: [name]
        relations:
          tax_lines:
            materialized: view
            not_null: [title, rate]
            price_set:
              shop_money:
                rename:
                  amount: tax_amount
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    models = tmp_path / "dbt" / "models" / "silver"
    scaffold_dbt(config, project_root=tmp_path, dbt_models_dir=models, warn=False)

    parent = (models / "stg_example_api__orders.sql").read_text(encoding="utf-8")
    assert "shipping_address__ship_city" in parent
    assert "nullif(" in parent
    assert "shipping_address__geo__coords__ship_lat" in parent
    assert " as shipping_address__city" not in parent

    child = (models / "stg_example_api__orders__line_items.sql").read_text(
        encoding="utf-8"
    )
    assert " as line_sku" in child
    assert "variant__product__category__category" in child

    tax = (models / "stg_example_api__orders__line_items__tax_lines.sql").read_text(
        encoding="utf-8"
    )
    assert "price_set__shop_money__tax_amount" in tax

    silver_yml = yaml.safe_load(
        (models / "_silver__models.yml").read_text(encoding="utf-8")
    )
    by_model = {m["name"]: m for m in silver_yml["models"]}
    li = by_model["silver_example_api__orders__line_items"]
    li_tests = {c["name"]: c["tests"] for c in li["columns"]}
    assert "not_null" in li_tests["line_sku"]
    assert "not_null" in li_tests["quantity"]
    assert "not_null" in li_tests["variant__product__category__category"]
    assert any(
        isinstance(t, dict) and "accepted_values" in t for t in li_tests["line_sku"]
    )
    tax_model = by_model["silver_example_api__orders__line_items__tax_lines"]
    tax_tests = {c["name"]: c["tests"] for c in tax_model["columns"]}
    assert "not_null" in tax_tests["title"]
    assert "not_null" in tax_tests["rate"]


def test_view_warn_triggers_for_large_view_relation(tmp_path: Path):
    schema_path = tmp_path / "schemas" / "example_api" / "orders" / "orders.schema.yaml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    pipeline = tmp_path / "configs" / "pipelines" / "example_api" / "orders.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: example_api.orders
source:
  type: example_api.orders
schema: schemas/example_api/orders/orders.schema.yaml
dbt:
  silver:
    unique_key: [id]
  stg:
    view_warn:
      sample_rows: 10
      parent_rows: 1000000
      child_rows: 20
    relations:
      line_items:
        materialized: view
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    bronze = (
        tmp_path
        / "data"
        / "lake"
        / "bronze"
        / "example_api"
        / "orders_v1"
        / "dt=2020-01-01"
    )
    bronze.mkdir(parents=True)
    rows = [
        {"id": str(i), "line_items": [{"sku": "a"}, {"sku": "b"}, {"sku": "c"}]}
        for i in range(10)
    ]
    (bronze / "data.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n",
        encoding="utf-8",
    )

    warnings = collect_view_size_warnings(config, project_root=tmp_path)
    assert any("line_items" in w.message for w in warnings)

    # table materialization → no child warn
    pipeline.write_text(
        pipeline.read_text(encoding="utf-8").replace(
            "materialized: view", "materialized: table"
        ),
        encoding="utf-8",
    )
    config_table = load_pipeline_config(pipeline)
    assert collect_view_size_warnings(config_table, project_root=tmp_path) == []


def test_view_warn_skips_empty_lake(tmp_path: Path):
    schema_path = tmp_path / "schemas" / "example_api" / "orders" / "orders.schema.yaml"
    schema_path.parent.mkdir(parents=True)
    schema_path.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    pipeline = tmp_path / "configs" / "pipelines" / "example_api" / "orders.yaml"
    pipeline.parent.mkdir(parents=True)
    pipeline.write_text(
        """
name: example_api.orders
source:
  type: example_api.orders
schema: schemas/example_api/orders/orders.schema.yaml
dbt:
  silver:
    unique_key: [id]
  stg:
    relations:
      line_items:
        materialized: view
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    config = load_pipeline_config(pipeline)
    assert collect_view_size_warnings(config, project_root=tmp_path) == []
