# Silver catch-up (bronze ↔ silver)

DET lands bronze independently of dbt. Incremental silver uses a watermark
(`__extract_run_datetime` by default). When a **later** extract-run lands in
silver before an **earlier** interval’s latest bronze run is visible, a normal
incremental build (`watermark > max(silver)`) never picks up the miss.

## Correctness grain

- For each `(interval_start, interval_end)`, the **latest** bronze
  `__extract_run_datetime` must appear in silver.
- Older extract-run siblings for the same interval are **bronze history** only
  (informational `stale_siblings_ignored`). Silver stays deduped via
  `det_dedupe_latest_run` (latest run wins per `unique_key`).

## Flow

1. **Diff (read-only):** `det silver-catchup-diff -p <pipeline>` or
   `--all-pipelines` (MCP: `diff_bronze_silver`).
2. **Plan:** `det silver-catchup-plan --dry-run …` → `approval_plan`
   (MCP: `silver_catchup_dry_run`).
3. **Apply manifest:** `det approve` then
   `det silver-catchup-plan --apply --approval <id>` writes
   `{lake}/ops/silver_catchup/manifest.json`.
4. **Catch-up build:** later turn
   `det dbt --catchup --approval <id>` — one process; `--select` only silver
   models listed in the manifest; dbt `--vars` pass
   `det_catchup_by_pipeline`.
5. **Verify:** re-run the diff; `catchup_count` should be 0.

Do **not** default to `--full-refresh` for large sources.

## Catch-up SQL (incremental only)

When `det_catchup_by_pipeline[<pipeline>]` is non-empty:

- Read stg rows for those `__extract_run_datetime` values only.
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
{DET_LAKE_PATH}/ops/silver_catchup/manifest.json
# split layout: {DET_LAKE_PATH_OPS}/ops/silver_catchup/manifest.json
```

Replace-on-apply. `det dbt --catchup` loads the file and injects vars (DuckDB and
BigQuery targets).
