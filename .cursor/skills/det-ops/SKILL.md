---
name: det-ops
description: >-
  DET agent workflows: debug missing raw partitions, preview bronze prune, and
  scaffold/init from pipeline configs using MCP dry-run tools (or det CLI).
---

# DET ops workflows

Prefer the **det** MCP server (read-only + dry-run). Mutating steps use the `det` CLI
after the user confirms. Install: `uv pip install -e ".[mcp]"`.

## Debug missing raw

1. `list_pipelines` (MCP) or `det list-pipelines` → pick a canonical id (`noaa.storm_events`).
2. `describe_pipeline` — note `destination.path` and `dataset`.
3. `list_raw_partitions` — nested lake `raw/{provider}/{source}/` + hive dirs.
4. If a run exists, `read_manifest` on that run path (`…/meta/manifest.json` via run dir).
5. If raw is empty: check extract interval (`-s`/`-e`), source plugin, and lake path.
   Bronze without raw usually means load/migrate was pointed at the wrong interval.

## Preview prune

1. `describe_pipeline` — confirm destination type (filesystem / duckdb / postgres).
2. `prune_dry_run` with `interval_start`, optional `interval_end`, and `keep`.
3. Show `to_remove` to the user. Prune never touches `raw/`.
4. Apply only via CLI when approved: `det prune -p … -s … --keep N --apply`.

## Scaffold / init from pipeline

1. Existing pipeline: `scaffold_dbt_dry_run` → review would_write/would_patch actions.
2. Greenfield: `init_pipeline_dry_run` with `name`, `source_type`, destination knobs.
3. Write with CLI: `det scaffold-dbt -p …` or `det init-pipeline …` (omit `--dry-run`).
4. Preview dbt select/env: `dbt_dry_run` with the pipeline path/name.

## Resources

- `det://pipelines/{name}` — YAML text
- `det://schemas/{dataset}/{filename}` — schema YAML
- `det://readme` — MCP pointer + dlt reminder

## Hard rules

- Do not call extract/load/prune-apply through MCP (not exposed in v1).
- Do not suggest `dlt.pipeline` for landing.
