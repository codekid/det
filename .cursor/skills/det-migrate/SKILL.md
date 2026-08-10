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

- Bronze contract changed (new schema file / renamed fields)
- Need to rebuild bronze for an interval from existing `raw/` wire
- Never migrate from a bronze payload column — raw is the source of truth

## Workflow

1. `list_pipelines` / `describe_pipeline` — confirm pipeline, schema path, destination.
2. `list_mappers` — reuse an existing mapper when it fits (`identity`,
   `storm_events_identity`, `example_api_v1_to_v2`, …).
3. Lake health: `diagnose_pipeline` (optional interval) or `diff_partitions`.
4. If drafting a new mapper from two schemas:
   - `mapper_from_diff_dry_run(from_schema, to_schema, mapper_name)`
   - Show `ops` + `code` + `register_hint` to the user
   - After confirm: add the function next to the source plugin, `register_mapper` in
     `src/det/plugins.py` (MCP never writes these files)
5. New target schema file: write under `schemas/…` (from
   `schema_from_sample_dry_run` or hand-authored), point pipeline / `--schema` at it.
6. Preview loadability: `validate_sample` and/or **`migrate_dry_run`** with
   `to_bronze`, `schema`, `mapper`, interval (`validate_limit` default 50).
   Show partition plan, `ok`, and errors. No bronze is written.
7. Apply only via CLI when approved (omit `--dry-run`):

```bash
det migrate -p <pipeline> \
  --to-bronze <provider.source_vN> \
  --schema schemas/<provider>/<source>/<file>.schema.yaml \
  --mapper <mapper_name> \
  -s <interval_start> -e <interval_end>
```

CLI preview: `det migrate … --dry-run` (optional `--validate-limit N`).

Optional: `--from-raw`, `--lake-path`, `--ingestion thin`.

## Hard rules

- Do not call extract/load/migrate-write through MCP (`migrate_dry_run` only).
- Do not suggest `dlt.pipeline` for landing.
- Prune never deletes `raw/`; migrate rebuilds bronze from raw only.
