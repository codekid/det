---
name: det-new-source
description: >-
  Greenfield DET source plugin + pipeline YAML + schema + dbt scaffold using
  MCP dry-run tools and the det CLI after confirmation.
---

# DET new source

Prefer the **det** MCP server for previews. Writes use the `det` CLI (or confirmed
file edits). Install: `uv pip install -e ".[mcp]"`.

## Plugin checklist

Implement a `SourcePlugin` under `src/det/sources/<provider>/` (see
`src/det/sources/base.py`):

1. `name` — canonical `provider.source`
2. `defaults()` — url, auth env, filters, fixture knobs
3. `extract_to_raw(...)` — write bytes under `data_dir`, return artifact descriptors
4. `records_from_raw(...)` — yield `SourceRow` (source-native; no naming/meta)
5. Register in `src/det/plugins.py` via `register_source`
6. Optional migrate mapper → `register_mapper`

dlt may help HTTP (`RESTClient`, `@dlt.resource` as iterator). **Never**
`dlt.pipeline` / `pipeline.run` for bronze landing.

## Workflow

1. `list_sources` — ensure the id is free / match existing plugins.
2. `init_pipeline_dry_run` with `name` == `source_type` == `provider.source`,
   destination knobs (`filesystem` / `duckdb` / `postgres`).
3. After user confirms, CLI: `det init-pipeline --name … --source-type …` (omit
   `--dry-run`). Or apply the dry-run actions manually.
4. Draft a real schema from fixtures/raw:
   - Extract a sample interval (CLI) or use inline `records`
   - `schema_from_sample_dry_run` → review `yaml` / `would_write`
   - Write schema only after confirm; keep `additionalProperties: false`
5. `scaffold_dbt_dry_run` → then `det scaffold-dbt -p …` if needed.
6. Smoke: `det run -p … -s …` (or extract/load), then `diagnose_pipeline` /
   `validate_sample`. Lake layout: `raw|bronze/{provider}/{source}/`.

## Hard rules

- MCP is dry-run / inspect only — no extract/load through MCP.
- Canonical id is `provider.source` everywhere (pipeline name, source type, lake).
- Do not suggest `dlt.pipeline` for landing.
