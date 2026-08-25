---
name: det-dbt
description: >-
  DET silver/gold dbt workflows: scaffold dry-run, dbt_dry_run select/env,
  dbt.silver / dbt.stg knobs, bronze source env, and ops SLOs (pipeline slo:
  → seed → tag:ops tests). Gold models are hand-written.
---

# DET dbt (silver / gold)

Prefer MCP dry-runs, then `det dbt` / `det scaffold-dbt` after confirmation.
Install: `uv pip install -e ".[mcp,dbt]"`.

## Mental model

- DET owns raw + bronze (wire-faithful typed landing).
    dbt owns silver + gold (`stg_*` → `silver_*` → hand-written gold).
- SQL schemas mirror bronze: `bronze_{provider}` → `silver_{provider}`; gold stays `gold`.
  Silver schema comes from model `config(schema="silver_{provider}")` (scaffold), not
  `dbt_project.yml +schema`. `generate_schema_name` prevents `target.schema` prefixing
  (`main_silver_*`). Note: `stg_*` models also land in `silver_{provider}` (not a
  separate `stg_*` SQL schema). Model names stay stable from pipeline ``name``
  (`stg_noaa__storm_events`); bronze ``_vN`` is wired in ``sources.yml`` /
  ``det_bronze_from``.
- Gold is **never** scaffolded.
- Frequent wire **renames / sentinels / value maps** → `dbt.stg` + scaffold (not bronze mappers).
- True wire parse/layout breaks → bump `wire_version` (lake id becomes
  `{name}_vN`); see det-migrate. Re-scaffold with `--force` so sources point at
  the new bronze era (model names stay the same).
- Nested **structs** flatten in `dbt.stg` (`a.b` → `a__b`); **arrays** only via explicit `relations:`.
- **Docs:** JSON Schema `description` → bronze `sources.yml`; `dbt.docs.columns`
  (post-stg names) → silver `_silver__models.yml`. `dbt.stg` is transforms-only.

## Workflow

1. `describe_pipeline` — read `dbt.silver` and `dbt.stg`.
2. Preview scaffold: `scaffold_dbt_dry_run` → review actions → CLI
   `det scaffold-dbt -p …` (add `--force` only when overwriting is intended).
3. Preview CLI argv/env: `dbt_dry_run` with pipeline (default select is
   `stg_{provider}__{source}+` plus each `dbt.stg.relations` child `stg_…__{relation}+`).
4. Run locally: `det dbt -p <pipeline>` or `make dbt` (sets `DET_LAKE_PATH`).
   Nested flatten/relations: configure `dbt.stg` then `det scaffold-dbt -p …`.

## `dbt.stg` / `dbt.silver` / `dbt.docs` (scaffold knobs)

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
    # Opt-in BigQuery table layout (ignored on DuckDB). Not Iceberg destination.partition.
    # bigquery:
    #   partition_by:
    #     field: __extract_run_datetime
    #     data_type: timestamp
    #     granularity: day
    #   cluster_by: [id]
    #   require_partition_filter: false
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
        # omit grain → discount_codes__index from unnest ordinality
      line_items:
        path: line_items
        materialized: view     # view|table → stg + silver for the child
        # parent_key: id       # default: first non-meta silver.unique_key
        grain: [sku]           # → line_items__sku on this table and descendants
        rename: { sku: line_sku }
        not_null: [sku, quantity]   # relative; silver uses post-rename names
        relations:             # nested array under each line item
          tax_lines:
            path: tax_lines
            materialized: view
            grain: [title, rate]  # → line_items__tax_lines__title, …__rate
            not_null: [title, rate]
    view_warn:                 # advisory sample; never fails the build
      enabled: true
      sample_rows: 5000
      parent_rows: 500000
      child_rows: 2000000
  # Post-stg column docs for silver YAML (wire field docs live on the JSON Schema).
  docs:
    columns:
      event_severity: Normalized severity for reporting (coalesced historical names).
      state: State abbreviation; sentinels cleared in stg.
```

### Nested flatten + relations

| Rule | Behavior |
| --- | --- |
| Structs | Auto-flatten under `flatten` (`shipping_address.city` → `shipping_address__city`) |
| Struct adapts | `dbt.stg.fields` scopes; relative keys only; scoped rename keeps path prefix |
| Arrays | Explicit `relations:` only → `stg_{provider}__{source}__{relation}` |
| Nested arrays | `relations.*.relations` (path relative to parent item) → `…__line_items__tax_lines` |
| Relation grain | `grain: [field, …]` → path-qualified spine (`line_items__sku`); empty → `{path}__index` |
| Spine join | Deeper tables carry ancestor spine cols; join on `parent_key` + shared path-qualified keys |
| Relation tests | `not_null` / `unique` / `accepted_values` on the relation (not inherited from parent silver) |
| Depth | Default unlimited; set `flatten.depth: N` only to cap |
| Adapt order | flatten → coalesce → null_sentinels → map → rename → exclude |
| Collisions | Scaffold fails if flattened name clashes |
| Relation mat. | `relations.*.materialized` applies to **both** child stg and silver (default `view`) |
| BigQuery layout | Opt-in `dbt.silver.bigquery` / `relations.*.bigquery` (`partition_by`, `cluster_by`, `require_partition_filter`); scaffold emits only under `target.name == 'bigquery'`; requires `table`/`incremental` (not `view`). Distinct from Iceberg `destination.partition`. |
| Engine | DuckDB macros (`det_json_path_*`, unnest); bronze stays wire-faithful |

Silver relation `unique_key` is `[parent_key] + spine columns` (not `__rel_index*`).

Example join (tax → line):

```sql
on t.id = li.id
and t.line_items__sku = li.line_items__sku
```

Scaffold stg applies that adapt order; `exclude` drops from stg select only.
Final SELECT order: identity (`id` / `*_id` / `key` / `*_key` plus non-meta
`unique_key`) A–Z → other payload A–Z → meta (`__row_hash` first, then A–Z).
**`read_json` columns** are widened to schema ∪ coalesce sources so historical keys
in older JSONL remain visible (`union_by_name=true`). Object/array roots use DuckDB
`JSON` typing for path extract.

`det scaffold-dbt` / `det dbt -p` sample the lake and **warn** when a view relation
looks large (`view_warn`); they do not fail.

Bronze recreate after renames: identity migrate (no growing Python normalize mapper).

## Bronze → stg env

| Var | Role |
| --- | --- |
| `DET_LAKE_PATH` | Lake root for schema-aware `read_json` or `iceberg_scan` (default `./data/lake`; `s3://` / `gs://` for Iceberg bronze) |
| `DET_BRONZE_SOURCE` | `iceberg` (default lake), `filesystem` (JSONL opt-in), or `duckdb` |
| `DET_BRONZE_SCHEMA` | SQL schema when bronze is DuckDB/Postgres |
| `DET_ANALYTICS_DUCKDB` | Absolute analytics DB path (prefer in Airflow/Compose) |
| `DET_OPS_DUCKDB` | Absolute ops DB path for `--target ops` (separate from analytics) |
| `DET_DBT_TARGET` | Optional profile target when `--target` omitted (`bigquery` for GCS/BigLake) |
| `DET_GCP_PROJECT` / `DET_BQ_DATASET` | Required for profile target `bigquery` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Required for `det dbt` on `s3://` Iceberg lakes (same as extract/load) |
| `AWS_ENDPOINT_URL` | MinIO / custom S3 API endpoint (sets `DET_DUCKDB_S3_ENDPOINT` for dbt) |

On `s3://` lakes, `det dbt` selects profile target **`duckdb_s3`** (loads httpfs/iceberg
+ DuckDB S3 secret). Local filesystem lakes keep target **`duckdb`**. On `gs://`,
do **not** expect DuckDB S3 — set `DET_DBT_TARGET=bigquery` / `--target bigquery`
after registering BigLake tables ([docs/gcp-biglake.md](../../../docs/gcp-biglake.md);
prerequisites, IAM, and sandbox teardown in the same doc).
Silver/ops use **one** `sources.yml` each with inline Jinja (BQ `database` vs
DuckDB `meta.external_location`) — do not scaffold a second `sources_bigquery.yml`
with the same source names (dbt 1.12 duplicate-source parse error). On `gs://`
with `DET_DBT_TARGET=bigquery`, ops builds use the same BQ target (not DuckDB).

Macro `det_bronze_from("table", "bronze_{provider}")` switches stg to a native
DuckDB table when `DET_BRONZE_SOURCE=duckdb`. Iceberg bronze keeps `source()` and
reads `iceberg_scan` from `sources.yml` when `DET_BRONZE_SOURCE=iceberg` (DuckDB).
Pass the source name explicitly so multi-provider projects parse when `det dbt -p`
sets `DET_BRONZE_SCHEMA`. BigQuery is a **reader** of the Iceberg table (BigLake),
not a DET destination — there is no `destination.type: bigquery`.

## Ops models

- Models under `dbt/models/ops/` are tagged `ops` and use schema `ops`.
- Source: Iceberg `{lake}/ops/run_receipts` — DuckDB `iceberg_scan` (local) or
  BigLake `ops.run_receipts` (GCS + `--target bigquery`) after `det runs-materialize`.
- Analytics builds (`det dbt`, `det_dbt_silver_gold`, MCP `dbt_dry_run`) always
  `--exclude tag:ops` unless `--select` explicitly targets ops (`tag:ops`,
  `stg_det__…`, `path:models/ops`).
- Build ops with `det dbt --select tag:ops` — `--target ops` locally, or
  `--target bigquery` / `DET_DBT_TARGET=bigquery` on GCS after BigLake registration.
  Airflow DAG `det_ops_receipts` follows the same rule. Materialize first:
  `det runs-materialize`. Local: query `data/det_ops.duckdb` schema `ops`.
  GCS: query BQ `ops.*` tables.
- **Infra vs SLO:** `tag:ops` builds models **and** seed/tests. For a GCP/model
  smoke (or after mixed failed receipts), use
  `det dbt --select 'tag:ops,resource_type:model'` first. Full `tag:ops` SLO
  tests fail closed against `ops_slo_expected` — error attempts or missing recent
  ok receipts for seeded `(pipeline, command)` pairs fail the build even when
  silver/ops models succeed.
- **This run vs the fleet:** receipts answer “did this extract/load break?”;
  ops dbt answers “have opted-in pipelines been running often/well enough?”
  Declare policy with pipeline `slo:` (opt-in; omit → not in the expected set).
  Shared defaults on `slo:`; sparse `extract` / `load` overlays; `false` skips a
  command. Cadence hours are not inferred from `interval_*` or `dbt.silver.lookback`.
  `det scaffold-dbt` always regenerates `dbt/seeds/ops_slo_expected.csv` from **all**
  pipelines. `det check` errors `slo_seed_stale` on drift. Walk-through: DAG
  `det_ops_receipts` (`dbt build --select tag:ops --target ops`) — mart
  `det__ops_run_daily` plus recency / error-rate / p95 / fail-closed tests.
  Extract/load never read SLOs. Do not add `ops_slo_*` lists to `dbt_project.yml`.

## Agent catalog / Cube

- Physical models: MCP `list_models` / `describe_model` (includes gold grain and ops).
- Certified gold metrics: `cube_load` on `yearly_damage` (`make cube-up`).
- Certified fleet metrics: `cube_load` on `run_daily` (ops DuckDB). p50/p95 are
  already daily quantiles — do not re-sum them.
- Silver or ops row detail: `query_analytics` with `warehouse=analytics` or `ops`.
- This run (did extract/load break?): still `list_runs` / `summarize_runs`.
  Do not run `det dbt` against a DuckDB file Cube has open.

## Hard rules

- MCP `dbt_dry_run` / `scaffold_dbt_dry_run` never write or run mutating dbt builds.
- Do not invent gold models via scaffold.
- Do not suggest `dlt.pipeline` for landing.
