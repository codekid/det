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
- Digest and `.runs.jsonl` UTC-normalize all three fields (same as coverage), so
  offset-equivalent timestamps share one `content_digest`.
- For each `(interval_start, interval_end)`, the **latest** bronze
  `__extract_run_datetime` must appear in silver **for that same interval**.
- Older extract-run siblings for the same interval are **bronze history** only
  (informational `stale_siblings_ignored`). Silver stays deduped via
  `det_dedupe_latest_run` (latest run wins per `unique_key`).

## Candidate discovery modes

Same membership rule either way; only which bronze intervals are considered
(and how silver is queried) changes.

| Mode | Flag | Bronze candidates | Silver query | Use when |
| --- | --- | --- | --- | --- |
| **A — routine** | `--extract-lookback 48h` (or `7d`, …) | Intervals touched by bronze extract runs in the lookback (siblings expanded) | Probe only those intervals | Frequent / after load |
| **B — census** | omit lookback; optional `-s`/`-e` | Full lake, or interval-start window | Full `DISTINCT` when unscoped; probe when `-s`/`-e` | Weekly audit / known date range |

Default is **Mode B** (backward compatible). Mode A cannot combine with `-s`/`-e`.
Diff JSON includes `candidate_mode` (`extract_lookback` \| `interval` \| `full`).

Mode A discovers **all** bronze extract runs in the lookback window up to the
apply safety cap (`100_000`), independent of `--limit`. `--limit` only truncates
displayed `catchup_runs` / `ok_intervals` / `stale_siblings_ignored`. Diff fields:
`discovery_cap` = value that bounded discovery (Mode A always, and Mode B
plan/apply `complete` mode: apply safety cap; Mode B inspect: `--limit`);
`truncated` = discovery hit that cap (window may be incomplete);
`display_truncated` = output lists were sliced for display.

Mode A can miss historical holes with **no** recent bronze extract. Mode A
`catchup_count=0` is not a forever census — run Mode B periodically. When
`truncated=true` on Mode A, the lookback window itself was not fully searched.

## Flow

1. **Diff (read-only):** `det silver-catchup-diff -p <pipeline>` or
   `--all-pipelines` (MCP: `diff_bronze_silver`). Prefer
   `--extract-lookback 48h` for routine checks. Reads DuckDB analytics silver,
   or BigQuery silver when `DET_DBT_TARGET=bigquery`.
2. **Plan:** `det silver-catchup-plan --dry-run …` → immutable `manifest_id`
   (`scm_…` = silver catch-up manifest) + `content_digest` + `approval_plan`
   (MCP: `silver_catchup_dry_run`). Pass the same lookback or `-s`/`-e` as the
   diff (include the same `-e` when the diff used `-e`).
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
5. **Verify:** re-run the diff (same mode/flags); `catchup_count` should be 0.
6. **BQ cleanup (optional):** each BigQuery heal registers
   `_det_catchup_runs_<scm_…>` and does **not** auto-drop it. After verify:
   - `det silver-catchup-cleanup --list` / `--list --older-than 7d`
   - MCP `silver_catchup_cleanup_dry_run` (`manifest_id` **or** `older_than`) →
     `det approve` → later
     `det silver-catchup-cleanup --apply --manifest-id <scm_…> --approval <id>`
     or `--created-before <iso> --apply --approval <id>` (dry-run freezes the
     cutoff from `--older-than`; relative duration is not re-evaluated at apply)
   Age uses BQ table `created`. Tables without `created` are skipped under
   retention filters. DuckDB heals never create these tables.

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
