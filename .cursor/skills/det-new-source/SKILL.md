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

Implement a `SourcePlugin` under `src/det/sources/<provider>/<source>.py` (see
`src/det/sources/base.py`). The file path **is** the registration:

1. Path `src/det/sources/<provider>/<source>.py` → id `provider.source`
2. `name` — must equal `{provider}.{source}` (exactly one dot)
3. `defaults()` — url, auth env **name**, filters, fixture knobs
4. `extract_to_raw(...)` — write bytes under `data_dir`, return artifact descriptors
5. `records_from_raw(...)` — yield `SourceRow` (source-native; no naming/meta)
6. Optional migrate mapper: `@mapper("…")` on a function in the **same module**
   (do not edit `plugins.py`)

See [docs/contract-triangle.md](../../docs/contract-triangle.md) for how schema
YAML, dbt `sources.yml`, and pipeline `dbt.stg` stay aligned.

Provider-local helpers belong in `_`-prefixed sibling modules (e.g.
`src/det/sources/noaa/_csv.py`); they are not discovered. Shared HTTP helpers stay
at `src/det/sources/http.py` / `http_json.py`.

Do not list in-tree plugins in `pyproject.toml` entry points (`det.sources` /
`det.mappers` are for out-of-tree packages only).

dlt may help HTTP (`RESTClient`, `@dlt.resource` as iterator). **Never**
`dlt.pipeline` / `pipeline.run` for bronze landing. After extract (and at load /
`det check`), DET refuses `_dlt_*` keys and leftover `_dlt_loads` /
`_dlt_pipeline_state` / `_dlt_version` paths under the pipeline lake prefix.

## Credentials

Config holds names, the environment holds values.

- Authenticated source: call `source_bearer_token(config, source_name=self.name)`
  from `det.sources.http_json`. It tries `auth_env`, then `DET_<PROVIDER>`, then
  `<PROVIDER>`, and raises `SecretNotSetError` when unresolved — never fetch
  unauthenticated as a fallback.
- Public source: declare `"auth_env": None` in `defaults()` so the plugin says so
  out loud and no lookup happens.
- Skip the lookup entirely when `fixture_records` is set (offline demos stay
  runnable with nothing exported).
- Using `det.sources.http.http_get` / `http_get_file`? Pass `refresh_headers=`
  (a callable that calls `invalidate_secret` then re-resolves) so a rotation
  mid-backfill retries once on 401/403 instead of failing the run.
- Postgres destinations use `destination.connection_env: DET_POSTGRES_DSN`; never
  a DSN literal. `det check` fails a passwordful DSN or a credential literal in
  `source.overrides`. See the README Secrets section.

## Workflow

1. `list_sources` — ensure the id is free / match existing plugins.
2. `init_pipeline_dry_run` with `name` == `source_type` == `provider.source`,
   destination knobs (`iceberg` default lake / `filesystem` JSONL / `duckdb` / `postgres`). For postgres pass
   `connection` as the **env var name** (e.g. `DET_POSTGRES_DSN`); a DSN with a
   password is refused.
3. After user confirms, CLI: `det init-pipeline --name … --source-type …` (omit
   `--dry-run`). Or apply the dry-run actions manually.
4. Draft a real schema from fixtures/raw:
   - Extract a sample interval (CLI) or use inline `records`
   - `schema_from_sample_dry_run` → review `yaml` / `would_write`
   - Write schema only after confirm; keep `additionalProperties: false`
5. `scaffold_dbt_dry_run` → then `det scaffold-dbt -p …` if needed.
   Use `dbt.stg` for coalesce/sentinels/maps (see det-dbt); keep bronze wire-faithful.
6. Smoke: `det run -p … -s …` (or extract/load), then `diagnose_pipeline` /
   `validate_sample`. Lake layout: `raw|bronze/{provider}/{source}_v{N}/`
   (lake id = `{name}_v{wire_version}`).
7. Leave `wire_version: 1` (init default). Bump only on true wire breaks — lake
   paths move to `_vN` automatically (see det-migrate).

## Where may we reshape?

- Land near-wire bytes in raw. Schema validation owns the contract — unexpected
  properties should **fail load** (do not silently allowlist/strip in the source).
- Enrichment-only in `records_from_raw` is OK (e.g. inject `subject_key`). Analytics
  adapts belong in `dbt.stg`. Prefer `det.sources.http_json` for JSON page artifacts.

State interval mode on the plugin docstring: `year_files` | `query_params` |
`partition_only`.

## Hard rules

- MCP is dry-run / inspect only — no extract/load through MCP.
- Pipeline `name` / `-p` stay `provider.source`; lake / SQL ids are
  `{name}_v{wire_version}` (including `_v1`). Top-level `dataset:` is rejected.
- Do not suggest `dlt.pipeline` for landing. Prefer `ingestion.library: det`
  (`dlt` is a deprecated alias, not removed yet).
