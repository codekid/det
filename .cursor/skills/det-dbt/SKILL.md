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
- SQL schemas mirror bronze: `bronze_{provider}` → `silver_{provider}`; gold stays `gold`.
  Silver schema comes from model `config(schema="silver_{provider}")` (scaffold), not
  `dbt_project.yml +schema`. `generate_schema_name` prevents `target.schema` prefixing
  (`main_silver_*`). Note: `stg_*` models also land in `silver_{provider}` (not a
  separate `stg_*` SQL schema).
- Gold is **never** scaffolded.
- Frequent wire **renames / sentinels / value maps** → `dbt.stg` + scaffold (not bronze mappers).
- True wire parse/layout breaks → `wire_version` + new lake `dataset:` (see det-migrate).
- Nested **structs** flatten in `dbt.stg` (`a.b` → `a__b`); **arrays** only via explicit `relations:`.

## Workflow

1. `describe_pipeline` — read `dbt.silver` and `dbt.stg`.
2. Preview scaffold: `scaffold_dbt_dry_run` → review actions → CLI
   `det scaffold-dbt -p …` (add `--force` only when overwriting is intended).
3. Preview CLI argv/env: `dbt_dry_run` with pipeline (default select is
   `stg_{provider}__{source}+` plus each `dbt.stg.relations` child `stg_…__{relation}+`).
4. Run locally: `det dbt -p <pipeline>` or `make dbt` (sets `DET_LAKE_PATH`).
   Nested flatten/relations: configure `dbt.stg` then `det scaffold-dbt -p …`.

## `dbt.stg` / `dbt.silver` (scaffold knobs)

```yaml
dbt:
  silver:
    unique_key: [__row_hash]
    # Tests on silver (materialized/deduped) — avoid scanning stg views on large lakes.
    # Column names are post-stg (after rename/coalesce/flatten).
    not_null: [id]
    unique: [id]
    accepted_values:
      event_severity: [low, medium, high]
  stg:
    # flatten:                 # omit = unlimited object depth
    #   depth: 1               # optional cap on object levels
    #   include: [shipping_address]
    #   exclude: [client_details]
    # Top-level scalars only (full-replace rename)
    coalesce:
      severity: [severity, severity_level, level]
    null_sentinels:
      state: ["", "NA"]
    rename:
      severity: event_severity
    exclude: [debug_info]
    map:
      status: {"1": "open", "2": "closed"}
    # Nested struct adapts — keys under each scope are relative (never restate prefix)
    fields:
      shipping_address:
        rename: { city: ship_city }   # → shipping_address__ship_city
        geo:
          coords:
            rename: { lat: ship_lat } # → shipping_address__geo__coords__ship_lat
    relations:
      discount_codes:
        path: discount_codes   # top-level array
        materialized: view
      line_items:
        path: line_items
        materialized: view     # view|table → stg + silver for the child
        # parent_key: id       # default: first non-meta silver.unique_key
        rename: { sku: line_sku }
        not_null: [sku, quantity]   # relative; silver uses post-rename names
        relations:             # nested array under each line item
          tax_lines:
            path: tax_lines
            materialized: view
            not_null: [title, rate]
    view_warn:                 # advisory sample; never fails the build
      enabled: true
      sample_rows: 5000
      parent_rows: 500000
      child_rows: 2000000
```

### Nested flatten + relations

| Rule | Behavior |
| --- | --- |
| Structs | Auto-flatten under `flatten` (`shipping_address.city` → `shipping_address__city`) |
| Struct adapts | `dbt.stg.fields` scopes; relative keys only; scoped rename keeps path prefix |
| Arrays | Explicit `relations:` only → `stg_{provider}__{source}__{relation}` |
| Nested arrays | `relations.*.relations` (path relative to parent item) → `…__line_items__tax_lines` |
| Relation tests | `not_null` / `unique` / `accepted_values` on the relation (not inherited from parent silver) |
| Depth | Default unlimited; set `flatten.depth: N` only to cap |
| Adapt order | flatten → coalesce → null_sentinels → map → rename → exclude |
| Collisions | Scaffold fails if flattened name clashes |
| Relation mat. | `relations.*.materialized` applies to **both** child stg and silver (default `view`) |
| Engine | DuckDB macros (`det_json_path_*`, unnest); bronze stays wire-faithful |

Scaffold stg applies that adapt order; `exclude` drops from stg select only.
**`read_json` columns** are widened to schema ∪ coalesce sources so historical keys
in older JSONL remain visible (`union_by_name=true`). Object/array roots use DuckDB
`JSON` typing for path extract.

`det scaffold-dbt` / `det dbt -p` sample the lake and **warn** when a view relation
looks large (`view_warn`); they do not fail.

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
