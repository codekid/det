---
name: det-ops
description: >-
  DET agent workflows: debug missing raw partitions, failed or slow extract/load
  runs, schema validation / contract drift, preview bronze prune, fleet SLOs
  (pipeline slo: → ops dbt tests), and scaffold/init from pipeline configs using
  MCP dry-run tools (or det CLI).
---

# DET ops workflows

Prefer the **det** MCP server (read-only + dry-run). Mutating steps use the `det` CLI
**only after the user explicitly confirms**, then `det approve`, then a later turn
with `--approval <id>`. Never chain dry-run → apply/write in the same turn (migrate,
prune `--apply`, non-dry-run scaffold/init, extract/load/run).
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
   - `list_runs` / `summarize_runs` — extract/load attempts (failures included)
5. If raw is empty: check extract interval (`-s`/`-e`), source plugin, and lake
   (`DET_LAKE_MODE` + `DET_LAKE_PATH` / default `./data/lake` — single root; not a
   per-pipeline `destination.path` and not dual buckets).
   Lake **layout 1** is the current hive (`raw|bronze/{provider}/{source}_vN/…`),
   SQL names, and siblings (`locks/`, `runs/`, `ops/`). Full contract:
   `docs/lake-layout.md`. `wire_version` is only a payload/dataset-era bump.
   Manifests and receipts stamp `lake_layout`.
   Airflow/CI logs are JSON (`DET_LOG_FORMAT=json`); grep `pipeline` /
   `extract_run_datetime`. Laptop TTY stays console (`--log-format` / `DET_LOG_FORMAT`).
   `LeaseHeldError` / grep lake lease: another extract/load holds `(pipeline, interval)`.
   Kill that worker, then `det lock-release -p … -s … --force` (or DAG `det_clear_lock`).
   Do not force-clear while the job is still running.    TTL: `--lock-ttl-sec` / `DET_LOCK_TTL_SEC` / Airflow conf `lock_ttl_sec`.
   Backend: lake (default, CAS on s3/gs) or `DET_LOCK_BACKEND=postgres` /
   pipeline `lease.backend: postgres` (DSN in `DET_LOCK_PG_DSN`).
   `DET_LOCK=0` disables the lease (unsafe; tests only). MCP must not delete locks.
   For a failed or slow run, prefer `list_runs` / `summarize_runs` (or `det runs`)
   over grepping task logs: receipts survive a failed extract's partition rmtree
   and carry `status`, `duration_ms`, `error_code`, and `owner`. Manifest remains
   the authority for what landed. Bronze without raw usually means load/migrate was
   pointed at the wrong interval.
   Rebuild bronze from raw via `det migrate`, not from a bronze payload column.

## Silver lag (bronze present, silver missing latest run)

When bronze has data but incremental silver appears behind (watermark hole):

1. MCP `diff_bronze_silver` — `catchup_runs` are **latest bronze extract-run per
   interval** not yet in silver (DuckDB analytics, or BigQuery when
   `DET_DBT_TARGET=bigquery`). Ignore
   `stale_siblings_ignored` (older siblings; silver stays deduped).
2. MCP `silver_catchup_dry_run` → show immutable `manifest_id` / `content_digest`
   + `approval_plan` → **stop**.
3. After confirm: `det approve` then later
   `det silver-catchup-plan --apply --manifest-id <scm_…> --content-digest <sha256:…> --approval <id>`
   (writes immutable `.json` + sibling `.runs.jsonl`).
4. MCP `dbt_dry_run` with `catchup=True` and `catchup_manifest=<scm_…>` → show its
   separate `approval_plan` → **stop**. After confirm: `det approve` then later
   `det dbt --catchup --catchup-manifest <scm_…> --approval <dbt_id>` (distinct id;
   one build; DuckDB `read_json` via `DET_CATCHUP_MANIFEST_PATH`, or BQ external
   table via `DET_CATCHUP_BQ_RELATION` when ops is `gs://`; local→BQ fails;
   not `--full-refresh`).
5. Re-diff to verify `catchup_count=0`.

See [docs/silver-catchup.md](../../docs/silver-catchup.md).

## Debug a failed or slow run

Receipts under `{lake}/runs/` are observability (what happened). `meta/manifest.json`
is still the authority for what landed.

1. `summarize_runs` (optional `-p`, attempt-date `since`/`until`, default last 7 days)
   — attempts, ok/error, `error_codes`, p50/p95 `duration_ms`.
2. `list_runs` with `status=error` (or `command=extract|load`) for the failing
   `error_code` / scrubbed `error_message` / `owner`.
3. CLI equivalent: `det runs -p … --status error` or `det runs --summary`.
4. Airflow: `owner` is `airflow:{dag_id}:{run_id}` when the DAG set `DET_LOCK_OWNER`.
5. Do not treat a missing receipt as a missing partition; `DET_RUN_RECEIPTS=0`
   disables writing. A write failure never fails extract/load.
6. Warehouse projection: `det runs-materialize` (or DAG `det_ops_receipts`) writes
   Iceberg `{lake}/ops/run_receipts`. Local: dbt `tag:ops` / `--target ops` →
   `DET_OPS_DUCKDB`. GCS: prerequisites in [gcp-biglake.md](../../../docs/gcp-biglake.md)
   (connection + bucket IAM) — run `det biglake-register --dry-run` for IAM hint,
   then register BigLake `ops.run_receipts` (`det biglake-register --apply`),
   set `DET_DBT_TARGET=bigquery`, then `det dbt --select tag:ops`. For infra-only
   smoke (models without SLO tests), use
   `det dbt --select 'tag:ops,resource_type:model'` — mixed failed receipts fail
   seeded SLO tests even when models succeed. Disposable sandbox teardown:
   [gcp-biglake.md#teardown-full-disposable-sandbox](../../../docs/gcp-biglake.md#teardown-full-disposable-sandbox).
   JSON under `runs/` remains the attempt log.

## Fleet metrics (ops DuckDB / BigQuery / Cube)

Ops marts live in `DET_OPS_DUCKDB` (local) or BQ dataset `ops` (GCS), not analytics.

1. Certified fleet metrics: MCP `cube_load` on `run_daily` (`make cube-up`).
   Grain is `attempt_date`, `pipeline`, `command`. Do not re-sum `p50_ms` / `p95_ms`.
2. Row detail: MCP `query_analytics` with `warehouse=ops` (capped SELECT on `ops.*`).
3. Catalog: `list_models` / `describe_model` for `det__ops_run_daily` and
   `stg_det__run_receipts`.
4. This run (single attempt): still `list_runs` / `summarize_runs` on `{lake}/runs/`.

## This run vs the fleet

Receipts / `det runs` answer **this run** (did extract or load break?). Ops dbt
answers **the fleet** (have opted-in pipelines been running often and well enough?).
A paused extract DAG cannot self-check; `det_ops_receipts` is the walk-through.

1. Policy is `slo:` on pipeline YAML — **opt-in**. No `slo:` → not in
   `ops_slo_expected`. Shared defaults on `slo:`; sparse `extract` / `load`
   overlays; `extract: false` / `load: false` skips a command. Cadence hours live
   in Python (not `interval_*` / `dbt.silver.lookback`).
2. `det scaffold-dbt` always regenerates `dbt/seeds/ops_slo_expected.csv` from all
   pipelines. Embedders without in-tree ops models: `det scaffold-ops` (MCP
   `scaffold_ops_dry_run`) copies models/tests/macros into `./dbt` and regenerates
   the same seed. `det check` errors `slo_seed_stale` on drift. MCP
   `scaffold_dbt_dry_run` includes the seed action.
3. Alert path: DAG `det_ops_receipts` → `dbt build --select tag:ops` (target `ops`
   locally, `bigquery` on GCS when `DET_DBT_TARGET=bigquery`)
   (seed + `det__ops_run_daily` + recency / error-rate / p95 / fail-closed).
   Fail-closed codes: `schema_invalid`, `integrity_error`, `secret_not_set`
   (`lease_held` excluded). Extract/load never read SLOs.

## Schema invalid / contract drift

`schema_invalid` from `diagnose_pipeline`, failed `validate_sample`, or a red
`det load` / `det run` means the wire and JSON Schema disagree. That is the alert.

### Validation ladder

`validate_sample` runs the **same coerce + JSON Schema path as load** (no writes).
A 50-row cap is a **sample**, not proof the partition is clean. Load stays
**fail-closed** — there is no quarantine; fix wire/schema/mapper.

| Step | Action | Gate |
| --- | --- | --- |
| 1 | `diagnose_pipeline` + `validate_sample` **`limit=50`** | — |
| 2 | `validate_sample` on other runs from `diff_partitions` | — |
| 3 | `migrate_dry_run` **`validate_limit=50`** (default) | — |
| 4 | User confirms; set **`DET_ALLOW_FULL_VALIDATE=1`**; `migrate_dry_run` **`validate_limit=0`** + **`confirm_full_validate=true`** | **Required** |
| 4b | Operator alternative: CLI `det migrate --dry-run` without `--validate-limit` | No MCP env gate |
| 5 | Fix schema / `@mapper`; repeat 3–4 until `ok=true` | — |
| 6 | Smoke `det load` on one interval | — |
| 7 | `det migrate` (omit `--dry-run`; `--approval` when Path B) or backfill load | Path B if required |

Notes:

- MCP full-partition validate scans up to **100k rows per partition** and sets
  `validate_capped` / partition `truncated` when capped — **not a full census**.
  Narrow `-s`/`-e` or use CLI uncapped dry-run for operators.
- `det check` warns `full_validate_gated` when `DET_ALLOW_FULL_VALIDATE` is unset.
- Full validate cannot run quietly: missing `confirm_full_validate` or env → hard error.

1. Read the validation errors (unexpected property, type, required). Prefer a
   **representative** sample — bump `limit` / `sample_limit` (max 50); a 5-row
   peek often misses nested extras (e.g. `availability.*`).
2. Inspect with `sample_raw` (`stage`: `rows` or `coerced`) on the same interval.
3. Decide with the user: add properties to `schemas/…/*.schema.yaml`, or
   consciously open a subtree (`additionalProperties: true`) if junk is expected.
4. **Do not** silently allowlist/strip fields in the source plugin. Enrichment-only
   in `records_from_raw` (e.g. inject `subject_key`) is fine; pruning is not.
5. After schema edits: re-run the ladder (steps 3–4) / smoke `det load`. True wire
   breaks that need rebuilds → `det-migrate`.

Successful `det load` / migrate also stamps the raw partition
`meta/manifest.json` with `validation.ok` + `schema_sha256` (and row count). That is a
**receipt** for inspectability — missing `validation` is normal for fresh extract and
does **not** fail load. Failures are not stamped (CLI/Airflow exit stays the alert).
Extract/load attempts (including those failures) are in `{lake}/runs/` — `list_runs`
/ `det runs --status error` for `error_code=schema_invalid`.

## Secrets are unset / auth failures

`SecretNotSetError: secret is not set: tried …` names every candidate it tried.
Config carries names only (`auth_env`, `destination.connection_env`); export the
value, do not paste it into YAML.

1. `describe_pipeline` shows `connection_env` (the **name**). MCP resolves names
   from process env only and never returns a value.
2. Export the provider secret (`DET_EXAMPLE_API`, `DET_POSTGRES_DSN`) or, for
   local debugging, set `DET_SECRETS_BACKEND=file` + `DET_SECRETS_FILE` pointing
   at a gitignored `NAME=value` file. Env always wins over the file.
3. A source that declares auth fails the run when it cannot resolve; it never
   falls back to an unauthenticated request. After a rotation, a 401/403 is
   re-resolved and retried once, and cached values expire (`DET_SECRETS_TTL_SEC`).
4. `det check` code `secret_in_config` = a credential landed in YAML (passwordful
   DSN, userinfo in `destination.path`, credential literal in `source.overrides`).

## Preview prune

1. `describe_pipeline` — confirm destination type (iceberg default lake / filesystem JSONL / duckdb / postgres).
2. `prune_dry_run` with `interval_start`, optional `interval_end`, and `keep`.
3. Show `to_remove` to the user. Prune never touches `raw/`.
   Iceberg bronze (`type: iceberg`) is a Hadoop-style catalog on `DET_LAKE_PATH`
   (install `.[iceberg]`). BigQuery can register that table later as a reader;
   it is not a DET destination.
4. Dry-run payloads include `approval_plan`. After the user confirms, they run
   `det approve --plan <json> --approved-by <id>` (or `--command` / `--argv-json`).
   Apply in a **later** turn: `det prune -p … -s … --keep N --apply --approval apr_…`.
   `DET_REQUIRE_APPROVAL=1` (or `--require-approval`) makes `--approval` mandatory.
   Airflow prune-apply uses the same approval id in DagRun conf
   (`{"approval":"apr_…"}` with `DET_PRUNE_APPLY=1`); MCP never triggers that.

## Scaffold / init from pipeline

1. Existing pipeline: `scaffold_dbt_dry_run` → review would_write/would_patch actions.
2. Greenfield: `init_pipeline_dry_run` with `name`, `source_type`, destination knobs.
3. Write with CLI: `det scaffold-dbt -p …` or `det init-pipeline …` (omit `--dry-run`).
4. Preview dbt select/env: `dbt_dry_run` with the pipeline path/name.
5. After editing pipelines/schemas: MCP `check` (optional `pipeline`), or
   `det check`. The Cursor `afterFileEdit` hook / CI also run this. Fix errors
   before extract/load.

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
- After a dry-run, wait for the operator to `det approve`. Writing CLI belongs in a
  later turn and must pass `--approval` when `DET_REQUIRE_APPROVAL=1`.
- Approvals are **audit and intent-binding, not authorization** — the same shell can
  run `det approve`, so never present the gate as a security boundary. It guarantees
  the record describes the command that runs.
- Run the writing command with exactly the flags in the approved `argv`. Every flag
  that changes what or where data is written is in `plan_digest`, so changing
  `--lake-path`, `--set`, `--full-refresh`, `--target`, or `--ingestion` needs a new
  approval. An unrecognized flag is rejected as `approval_unbound_flag`.
- Ref and interval form do not matter (`noaa/storm_events` == `noaa.storm_events`;
  `2026-08-06` == `2026-08-06T00:00:00+00:00`) — both are canonicalized.
- Approvals are single-use and claimed atomically. If a run crashes mid-write the
  record stays `claimed` and never expires, so it is hidden from the default
  listing — find it with `det list-approvals --status claimed` or MCP
  `list_approvals` `status="claimed"`. Recovery is a fresh approval, or the
  operator running `det approval-release <id> --force` once the worker is
  confirmed dead. Do not propose releasing an approval whose run may still be
  alive, and do not delete records by hand.
