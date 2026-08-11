---
name: det-dbt
description: >-
  DET silver/gold dbt workflows: scaffold dry-run, dbt_dry_run select/env,
  dbt.silver / dbt.stg knobs, and bronze source env. Gold models are hand-written.
---

# DET dbt (silver / gold)

Prefer MCP dry-runs, then `det dbt` / `det scaffold-dbt` after confirmation.
Install: `uv pip install -e ".[mcp,dbt]"`.

## Mental model

- DET owns raw + bronze (wire-faithful typed landing).
- dbt owns silver + gold (`stg_*` → `silver_*` → hand-written gold).
- Gold is **never** scaffolded.
- Frequent wire **renames / sentinels / value maps** → `dbt.stg` + scaffold (not bronze mappers).
- True wire parse/layout breaks → `wire_version` + new lake `dataset:` (see det-migrate).
- Nested flatten is **not** supported via `dbt.stg` yet.

## Workflow

1. `describe_pipeline` — read `dbt.silver` and `dbt.stg`.
2. Preview scaffold: `scaffold_dbt_dry_run` → review actions → CLI
   `det scaffold-dbt -p …` (add `--force` only when overwriting is intended).
3. Preview CLI argv/env: `dbt_dry_run` with pipeline (default select is
   `stg_{provider}__{source}+`, same as `det dbt`).
4. Run locally: `det dbt -p <pipeline>` or `make dbt` (sets `DET_LAKE_PATH`).

## `dbt.stg` / `dbt.silver` (scaffold knobs)

```yaml
dbt:
  silver:
    unique_key: [__row_hash]
    # Tests on silver (materialized/deduped) — avoid scanning stg views on large lakes.
    # Column names are post-stg (after rename/coalesce).
    not_null: [id]
    unique: [id]
    accepted_values:
      event_severity: [low, medium, high]
  stg:
    coalesce:
      severity: [severity, severity_level, level]
    null_sentinels:
      state: ["", "NA"]
    rename:
      severity: event_severity
    exclude: [debug_info]
    map:
      status: {"1": "open", "2": "closed"}
```

Scaffold stg applies: coalesce → null_sentinels → map → rename; `exclude` drops from
stg select only. **`read_json` columns** are widened to schema ∪ coalesce sources
so historical keys in older JSONL remain visible (`union_by_name=true`).

Bronze recreate after renames: identity migrate (no growing Python normalize mapper).

## Bronze → stg env

| Var | Role |
| --- | --- |
| `DET_LAKE_PATH` | Filesystem lake root for schema-aware `read_json` |
| `DET_BRONZE_SOURCE` | `filesystem` (default) or `duckdb` |
| `DET_BRONZE_SCHEMA` | SQL schema when bronze is DuckDB/Postgres |
| `DET_ANALYTICS_DUCKDB` | Absolute analytics DB path (prefer in Airflow/Compose) |

Macro `det_bronze_from("table", "bronze_{provider}")` switches stg to a native
DuckDB table when `DET_BRONZE_SOURCE=duckdb`. Pass the source name explicitly so
multi-provider projects parse when `det dbt -p` sets `DET_BRONZE_SCHEMA`.

## Hard rules

- MCP `dbt_dry_run` / `scaffold_dbt_dry_run` never write or run mutating dbt builds.
- Do not invent gold models via scaffold.
- Do not suggest `dlt.pipeline` for landing.
