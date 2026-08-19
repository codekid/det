---
name: det-migrate
description: >-
  Rebuild bronze from raw after a schema/contract change using DET migrate
  mappers, MCP dry-run drafts, and the det migrate CLI.
---

# DET migrate (bronze rebuild from raw)

Prefer the **det** MCP server for inspect + drafts. Mutating migrate uses the `det`
CLI after the user confirms. Install: `uv pip install -e ".[mcp]"`.

## When to use

- Bronze contract changed (new schema file / shared reshape every consumer needs)
- Need to rebuild bronze for an interval from existing `raw/` wire
- Never migrate from a bronze payload column — raw is the source of truth
- Frequent **semantic renames** → prefer `dbt.stg.coalesce` (det-dbt), not a migrate mapper

## Wire version and lake ids

- Lake id is always `{pipeline.name}_v{wire_version}` (including `_v1`).
  Example: `name: noaa.locations` + `wire_version: 1` → `noaa.locations_v1` →
  `raw|bronze/noaa/locations_v1/`, DuckDB `bronze_noaa.locations_v1`.
- Pipeline `name` / `-p` stay unversioned `provider.source`.
- Top-level pipeline `dataset:` (lake override) is **rejected** — bump
  `wire_version` only.
- Leave `destination.dataset` alone (medallion prefix, e.g. `bronze`).
- **Semantic renames:** keep the same `wire_version`; adapt in `dbt.stg` (or rare
  bronze mapper into the **same** `--to-bronze` as `--from-raw`).
- **True wire break:** bump `wire_version` (new extracts land under `_vN`);
  rebuild historical raw with explicit
  `--from-raw provider.source_v1 --to-bronze provider.source_v2`.
- Manifest always stamps `wire_version` (legacy missing field ⇒ `1`) and
  `lake_layout` (current hive/SQL contract; missing ⇒ `1`). See
  `docs/lake-layout.md` for the layout 1 changelog and what bumps each field.
  Bump `wire_version` for payload breaks only — do not use it to rename hive keys.
- Optional filter: `det migrate … --wire-version N` / MCP
  `migrate_dry_run(..., wire_version=N)` — edge case for mixed trees only.

## Workflow

1. `list_pipelines` / `describe_pipeline` — confirm pipeline, schema path, destination.
2. `list_mappers` — reuse an existing mapper when it fits (`identity`,
   `example_api_v1_to_v2`, …).
3. Lake health: `diagnose_pipeline` (optional interval) or `diff_partitions`.
4. If drafting a new mapper from two schemas:
   - `mapper_from_diff_dry_run(from_schema, to_schema, mapper_name)`
   - Show `ops` + `code` + `register_hint` to the user
   - After confirm: add the function next to the source plugin with `@mapper("…")`
     (MCP never writes these files). Do not edit `plugins.py`.
5. New target schema file: write under `schemas/…` (from
   `schema_from_sample_dry_run` or hand-authored), point pipeline / `--schema` at it.
6. Preview loadability: `validate_sample` and/or **`migrate_dry_run`** with
   `to_bronze`, `schema`, `mapper`, interval (`validate_limit` default 50).
   Show partition plan, `ok`, and errors. No bronze is written.
7. **Stop and ask the user to approve the apply.** Do not run a writing
   `det migrate` (omit `--dry-run`) until they explicitly confirm. Same for
   CLI `--dry-run` previews: present results, then wait.
8. Apply only via CLI after that confirmation:

```bash
# Preferred: same lake id for raw and bronze (current wire era)
det migrate -p <pipeline> \
  --to-bronze <provider.source_vN> \
  --schema schemas/<provider>/<source>/<file>.schema.yaml \
  --mapper <mapper_name> \
  -s <interval_start> -e <interval_end>

# History rebuild after a wire bump (config already at v2)
# det migrate -p provider.source \
#   --from-raw provider.source_v1 --to-bronze provider.source_v2 …
```

CLI preview: `det migrate … --dry-run` (optional `--validate-limit N`,
`--wire-version N`).

Optional: `--from-raw`, `--lake-path`, `--ingestion thin`.

## Hard rules

- Do not call extract/load/migrate-write through MCP (`migrate_dry_run` only).
- After any migrate dry-run (MCP or CLI), wait for explicit user approval before
  a writing `det migrate`. Never chain dry-run → apply in one turn.
- Do not suggest `dlt.pipeline` for landing.
- Prune never deletes `raw/`; migrate rebuilds bronze from raw only.
