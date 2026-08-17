---
name: det-ops
description: >-
  DET agent workflows: debug missing raw partitions, schema validation / contract
  drift, preview bronze prune, and scaffold/init from pipeline configs using MCP
  dry-run tools (or det CLI).
---

# DET ops workflows

Prefer the **det** MCP server (read-only + dry-run). Mutating steps use the `det` CLI
**only after the user explicitly confirms.** Never chain dry-run → apply/write in the
same turn (migrate, prune `--apply`, non-dry-run scaffold/init, extract/load/run).
Install: `uv pip install -e ".[mcp]"`.

## Debug missing raw / bronze gaps

1. `list_pipelines` (MCP) or `det list-pipelines` → pick a canonical id (`noaa.storm_events`).
2. Prefer `diagnose_pipeline` with optional `interval_start` / `interval_end` and
   `sample_limit` (default 5, max 50).
3. Read `findings` codes (`empty_lake`, `raw_without_bronze`, `bronze_without_raw`,
   `schema_invalid`, `ok`) and `suggested_commands` (do not run until user confirms).
4. Dig deeper if needed:
   - `diff_partitions` — raw vs bronze extract-run coverage
   - `sample_raw` (`stage`: wire|rows|named|coerced) — adjust `limit` up/down
   - `validate_sample` — coerce/schema errors as data (raise `limit` toward 50 for
     nested/noisy APIs so rare extra fields show up)
   - `sample_bronze` — landed rows (FS / Iceberg / DuckDB / Postgres); inspection only
   - `read_manifest` on a raw run path
5. If raw is empty: check extract interval (`-s`/`-e`), source plugin, and lake
   (`DET_LAKE_PATH` / default `./data/lake` — not a per-pipeline `destination.path`).
   Airflow/CI logs are JSON (`DET_LOG_FORMAT=json`); grep `pipeline` /
   `extract_run_datetime`. Laptop TTY stays console (`--log-format` / `DET_LOG_FORMAT`).
   Bronze without raw usually means load/migrate was pointed at the wrong interval.
   Rebuild bronze from raw via `det migrate`, not from a bronze payload column.

## Schema invalid / contract drift

`schema_invalid` from `diagnose_pipeline`, failed `validate_sample`, or a red
`det load` / `det run` means the wire and JSON Schema disagree. That is the alert.

1. Read the validation errors (unexpected property, type, required). Prefer a
   **representative** sample — bump `limit` / `sample_limit` (max 50); a 5-row
   peek often misses nested extras (e.g. `availability.*`).
2. Inspect with `sample_raw` (`stage`: `rows` or `coerced`) on the same interval.
3. Decide with the user: add properties to `schemas/…/*.schema.yaml`, or
   consciously open a subtree (`additionalProperties: true`) if junk is expected.
4. **Do not** silently allowlist/strip fields in the source plugin. Enrichment-only
   in `records_from_raw` (e.g. inject `subject_key`) is fine; pruning is not.
5. After schema edits: re-run `validate_sample` / smoke `det load`. True wire breaks
   that need rebuilds → `det-migrate`.

Successful `det load` / migrate also stamps the raw partition
`meta/manifest.json` with `validation.ok` + `schema_sha256` (and row count). That is a
**receipt** for inspectability — missing `validation` is normal for fresh extract and
does **not** fail load. Failures are not stamped (CLI/Airflow exit stays the alert).

## Preview prune

1. `describe_pipeline` — confirm destination type (filesystem / iceberg / duckdb / postgres).
2. `prune_dry_run` with `interval_start`, optional `interval_end`, and `keep`.
3. Show `to_remove` to the user. Prune never touches `raw/`.
   Iceberg bronze (`type: iceberg`) is a Hadoop-style catalog on `DET_LAKE_PATH`
   (install `.[iceberg]`). BigQuery can register that table later as a reader;
   it is not a DET destination.
4. Apply only via CLI when approved: `det prune -p … -s … --keep N --apply`.

## Scaffold / init from pipeline

1. Existing pipeline: `scaffold_dbt_dry_run` → review would_write/would_patch actions.
2. Greenfield: `init_pipeline_dry_run` with `name`, `source_type`, destination knobs.
3. Write with CLI: `det scaffold-dbt -p …` or `det init-pipeline …` (omit `--dry-run`).
4. Preview dbt select/env: `dbt_dry_run` with the pipeline path/name.
5. After editing pipelines/schemas: `det check` (or rely on the Cursor
   `afterFileEdit` hook / CI). Fix errors before extract/load.

## Related skills

| Skill | Use when |
| --- | --- |
| `det-migrate` | Contract change / rebuild bronze from raw |
| `det-new-source` | Greenfield plugin + pipeline + schema |
| `det-airflow` | Local Compose DAGs, backfill, dbt env |
| `det-dbt` | scaffold / `det dbt` / silver knobs |

## Resources

- `det://pipelines/{name}` — YAML text
- `det://schemas/{dataset}/{filename}` — schema YAML
- `det://readme` — MCP pointer + dlt reminder

## Hard rules

- Do not call extract/load/prune-apply through MCP (not exposed in v1).
- Do not suggest `dlt.pipeline` for landing.
