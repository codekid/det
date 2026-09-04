# Silver catch-up (bronze ↔ silver)

DET lands bronze independently of dbt. Incremental silver uses a watermark
(`__extract_run_datetime` by default). When a **later** extract-run lands in
silver before an **earlier** interval’s latest bronze run is visible, a normal
incremental build (`watermark > max(silver)`) never picks up the miss.

## Scope

| Supported | Not supported |
| --- | --- |
| DuckDB analytics (`DET_ANALYTICS_DUCKDB`) for **diff/plan** and **heal** | BigQuery catch-up from a **local** (non-`gs://`) ops lake |
| BigQuery silver when `DET_DBT_TARGET=bigquery` and ops/scm is on **GCS** (`gs://`) | `s3://` ops + BigQuery heal |

Bronze may still be Iceberg/GCS/filesystem. Diff/plan follow `DET_DBT_TARGET`
(DuckDB file vs BigQuery silver). Heal uses the same tiny pointer vars; the SQL
engine differs (DuckDB `read_json` vs BigQuery external table over sibling NDJSON).

## Correctness grain

- Coverage identity is
  `(interval_start, interval_end, extract_run_datetime)`. A run timestamp alone
  is not unique across intervals (parallel extracts can share a second).
- For each `(interval_start, interval_end)`, the **latest** bronze
  `__extract_run_datetime` must appear in silver **for that same interval**.
- Older extract-run siblings for the same interval are **bronze history** only
  (informational `stale_siblings_ignored`). Silver stays deduped via
  `det_dedupe_latest_run` (latest run wins per `unique_key`).

## Flow

1. **Diff (read-only):** `det silver-catchup-diff -p <pipeline>` or
   `--all-pipelines` (MCP: `diff_bronze_silver`). Reads DuckDB analytics silver,
   or BigQuery silver when `DET_DBT_TARGET=bigquery`.
2. **Plan:** `det silver-catchup-plan --dry-run …` → immutable `manifest_id`
   (`scm_…` = silver catch-up manifest) + `content_digest` + `approval_plan`
   (MCP: `silver_catchup_dry_run`).
3. **Apply manifest:** `det approve` then
   `det silver-catchup-plan --apply --manifest-id <scm_…> --content-digest <sha256:…> --approval <id>`
   writes create-once:
   - `{lake}/ops/silver_catchup/<manifest_id>.json`
   - `{lake}/ops/silver_catchup/<manifest_id>.runs.jsonl` (flat NDJSON for BigQuery)
   Apply re-diffs and **fails** if the live coverage digest no longer matches.
4. **Catch-up build:** later turn
   `det dbt --catchup --catchup-manifest <scm_…> --approval <id>` — one process;
   sets `DET_CATCHUP_MANIFEST_PATH` and tiny `--vars`
   (`det_catchup`, `det_catchup_manifest_id`).
   - **DuckDB:** macros `read_json` the scm `.json`.
   - **BigQuery:** requires `gs://` scm path; registers external table
     `_det_catchup_runs_<scm_…>` over the sibling `.runs.jsonl`; sets
     `DET_CATCHUP_BQ_RELATION`. Local-lake → BQ raises.
5. **Verify:** re-run the diff; `catchup_count` should be 0.

Do **not** default to `--full-refresh` for large sources.

## Catch-up SQL (incremental only)

When `var('det_catchup')` is true:

- Filter stg rows whose coverage key appears in the heal set for this
  `pipeline_name` (DuckDB `read_json` + `unnest`, or BigQuery
  `EXISTS` on `DET_CATCHUP_BQ_RELATION`).
- Probe silver with **unique_key + watermark** columns only.
- Apply a row if the key is missing **or**
  `incoming.watermark > silver.watermark` (never demote newer silver rows).
- Then `det_dedupe_latest_run` as usual.

`materialized: table` models full-rebuild every dbt run; they do not need this
branch (a normal `det dbt -p …` heals them).

Scaffolded incremental silver models carry `tags=["det_catchup"]` for
discoverability (`dbt ls --select tag:det_catchup`). Catch-up **selection**
stays manifest-driven — do not use the tag as `--select` for heal builds.

## Manifest location

Optional layout-1 sibling (no `lake_layout` bump):

```text
{DET_LAKE_PATH}/ops/silver_catchup/<manifest_id>.json
{DET_LAKE_PATH}/ops/silver_catchup/<manifest_id>.runs.jsonl
# split layout: {DET_LAKE_PATH_OPS}/ops/silver_catchup/…
```

Each apply creates a **new immutable** pair (`scm_` + 16 hex). There is no
shared mutable `manifest.json` pointer — `det dbt --catchup` must pass
`--catchup-manifest`. Approval plans bind both `--manifest-id` and
`--content-digest` (coverage-key hash) so apply cannot silently heal a different
set than the dry-run.
