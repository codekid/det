from __future__ import annotations

from pathlib import Path

from det.mcp.catalog import describe_dbt_model, list_dbt_models


def _write_model(root: Path, rel: str, sql: str, yaml_text: str | None = None) -> None:
    path = root / "dbt" / "models" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sql, encoding="utf-8")
    if yaml_text is not None:
        yml = path.with_name("_models.yml")
        yml.write_text(yaml_text, encoding="utf-8")


def test_list_and_describe_tmp_models(tmp_path: Path):
    _write_model(
        tmp_path,
        "silver/stg_demo__events.sql",
        '{{ config(materialized="view", schema="silver_demo") }}\nselect 1 as id\n',
    )
    _write_model(
        tmp_path,
        "gold/gold_demo.sql",
        "select 1 as event_year\n",
        yaml_text="""
version: 2
models:
  - name: gold_demo
    description: Demo gold mart
    config:
      meta:
        grain: [event_year]
    columns:
      - name: event_year
        description: Year grain
""",
    )
    _write_model(
        tmp_path,
        "ops/det__ops_run_daily.sql",
        "{{ config(tags=['ops']) }}\nselect 1 as attempts\n",
        yaml_text="""
version: 2
models:
  - name: det__ops_run_daily
    description: Daily rollup
    config:
      meta:
        grain: [attempt_date, pipeline, command]
""",
    )

    listed = list_dbt_models(root=tmp_path)
    by_name = {m["name"]: m for m in listed["models"]}
    assert by_name["stg_demo__events"]["layer"] == "stg"
    assert by_name["stg_demo__events"]["warehouse"] == "analytics"
    assert by_name["stg_demo__events"]["schema"] == "silver_demo"
    assert by_name["gold_demo"]["layer"] == "gold"
    assert by_name["gold_demo"]["schema"] == "gold"
    assert by_name["gold_demo"]["grain"] == ["event_year"]
    assert by_name["det__ops_run_daily"]["warehouse"] == "ops"
    assert by_name["det__ops_run_daily"]["layer"] == "ops"
    assert by_name["det__ops_run_daily"]["schema"] == "ops"

    described = describe_dbt_model("gold_demo", root=tmp_path)
    assert described["ok"] is True
    assert described["columns"][0]["name"] == "event_year"
    missing = describe_dbt_model("nope", root=tmp_path)
    assert missing["ok"] is False
    assert missing["error"] == "model_not_found"


def test_repo_gold_and_ops_catalog():
    listed = list_dbt_models()
    by_name = {m["name"]: m for m in listed["models"]}
    gold = by_name["gold_yearly_damage"]
    assert gold["layer"] == "gold"
    assert gold["warehouse"] == "analytics"
    assert gold["grain"] == ["event_year", "state"]
    daily = by_name["det__ops_run_daily"]
    assert daily["warehouse"] == "ops"
    assert daily["grain"] == ["attempt_date", "pipeline", "command"]
    described = describe_dbt_model("gold_yearly_damage")
    cols = {c["name"]: c for c in described["columns"]}
    assert "total_property_damage" in cols
