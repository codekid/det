---
name: det-dbt
description: >-
  DET silver/gold dbt workflows: scaffold dry-run, dbt_dry_run select/env,
  dbt.silver knobs, and bronze source env. Gold models are hand-written.
---

# DET dbt (silver / gold)

Prefer MCP dry-runs, then `det dbt` / `det scaffold-dbt` after confirmation.
Install: `uv pip install -e ".[mcp,dbt]"`.

## Mental model

- DET owns raw + bronze.
- dbt owns silver + gold (`stg_*` → `silver_*` → hand-written gold).
- Gold is **never** scaffolded.

## Workflow

1. `describe_pipeline` — read `dbt.silver` (`materialized`, `unique_key`, `order_by`,
   `incremental_strategy`, `watermark`, `lookback`).
2. Preview scaffold: `scaffold_dbt_dry_run` → review actions → CLI
   `det scaffold-dbt -p …` (add `--force` only when overwriting is intended).
3. Preview CLI argv/env: `dbt_dry_run` with pipeline (default select is
   `stg_{provider}__{source}+`, same as `det dbt`).
4. Run locally: `det dbt -p <pipeline>` or `make dbt` (sets `DET_LAKE_PATH`).

## Bronze → stg env

| Var | Role |
| --- | --- |
| `DET_LAKE_PATH` | Filesystem lake root for `read_json_auto` |
| `DET_BRONZE_SOURCE` | `filesystem` (default) or `duckdb` |
| `DET_BRONZE_SCHEMA` | SQL schema when bronze is DuckDB/Postgres |
| `DET_ANALYTICS_DUCKDB` | Absolute analytics DB path (prefer in Airflow/Compose) |

Macro `det_bronze_from` switches stg to a native DuckDB table when
`DET_BRONZE_SOURCE=duckdb`.

## Hard rules

- MCP `dbt_dry_run` / `scaffold_dbt_dry_run` never write or run mutating dbt builds.
- Do not invent gold models via scaffold.
- Do not suggest `dlt.pipeline` for landing.
