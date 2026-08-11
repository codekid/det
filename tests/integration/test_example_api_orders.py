"""Showcase pipeline: nested flatten + line_items relation."""

from __future__ import annotations

import json
from pathlib import Path

from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config
from det.runtime.dbt_runner import default_select_for_pipeline
from det.runtime.runner import PipelineRunner
from det.scaffold.dbt import scaffold_dbt


def test_example_api_orders_run_and_scaffold(tmp_path: Path, project_root: Path):
    load_plugins()
    lake = tmp_path / "lake"
    models = tmp_path / "dbt" / "models" / "silver"

    # Copy pipeline/schema into tmp project shaped like the repo.
    schema_src = project_root / "schemas/example_api/orders/orders.schema.yaml"
    schema_dst = tmp_path / "schemas/example_api/orders/orders.schema.yaml"
    schema_dst.parent.mkdir(parents=True)
    schema_dst.write_text(schema_src.read_text(encoding="utf-8"), encoding="utf-8")

    pipe_dst = tmp_path / "configs/pipelines/example_api/orders.yaml"
    pipe_dst.parent.mkdir(parents=True)
    pipe_dst.write_text(
        f"""
name: example_api.orders
source:
  type: example_api.orders
schema: schemas/example_api/orders/orders.schema.yaml
destination:
  type: filesystem
  path: {lake.as_posix()}
dbt:
  silver:
    unique_key: [id]
    order_by: ["__extract_run_datetime desc"]
    not_null: [id]
    unique: [id]
  stg:
    relations:
      discount_codes:
        materialized: view
        parent_key: id
      line_items:
        materialized: view
        parent_key: id
        relations:
          tax_lines:
            materialized: view
            parent_key: id
    null_sentinels:
      email: ["", "NA"]
    map:
      status:
        "1": open
        "2": closed
""",
        encoding="utf-8",
    )

    config = load_pipeline_config(pipe_dst)
    assert default_select_for_pipeline(config) == [
        "stg_example_api__orders+",
        "stg_example_api__orders__discount_codes+",
        "stg_example_api__orders__line_items+",
        "stg_example_api__orders__line_items__tax_lines+",
    ]

    result = scaffold_dbt(
        config, project_root=tmp_path, dbt_models_dir=models, warn=False, force=True
    )
    assert result.dataset == "example_api.orders"
    stg = (models / "stg_example_api__orders.sql").read_text(encoding="utf-8")
    assert "shipping_address__geo__coords__lat" in stg
    assert "customer__default_address__geo__coords__lon" in stg
    assert "customer__loyalty__tier__name" in stg
    assert " as line_items" not in stg
    child = (models / "stg_example_api__orders__line_items.sql").read_text(
        encoding="utf-8"
    )
    assert "unnest(cast(_parent.line_items as JSON[]))" in child
    assert "variant__product__category__name" in child
    assert "price_set__shop_money__amount" in child
    tax = (models / "stg_example_api__orders__line_items__tax_lines.sql").read_text(
        encoding="utf-8"
    )
    assert "json_extract(t0._rel, '$.tax_lines')" in tax
    assert (models / "stg_example_api__orders__discount_codes.sql").exists()

    runner = PipelineRunner(tmp_path)
    out = runner.run(
        config,
        interval_start="2026-01-01T00:00:00Z",
        interval_end="2026-01-02T00:00:00Z",
    )
    assert out.rows == 2
    assert out.partition_dir is not None
    jsonl = Path(out.partition_dir) / "data.jsonl"
    rows = [
        json.loads(line)
        for line in jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 2
    assert isinstance(rows[0]["shipping_address"], dict)
    assert isinstance(rows[0]["line_items"], list)
    assert rows[0]["line_items"][0]["sku"] == "SKU-A"
