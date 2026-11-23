# Changelog

All notable changes to **`det-elt`** (import package `det`) are documented here.

The SemVer public surface is `det.__all__` and the guarantees in
[docs/api.md](docs/api.md). This file is for embedders and operators scanning
breaks — not a full commit log.

Releases older than **0.9.0**: [GitHub Releases](https://github.com/codekid/det/releases).

## [0.12.0] - Unreleased

### Added

- `destination.type: bigquery` — native bronze writer. Project from
  `DET_GCP_PROJECT` (never YAML). Dataset defaults to `{prefix}_{provider}`
  (`bronze_noaa` for `noaa.storm_events`); override via `DET_BQ_DATASET_PREFIX`
  or `destination.bq_dataset`. Table is `{source}_v{wire_version}`. Replace-by-
  extract-run via DELETE + BQ load job. Requires `uv sync --extra bigquery`.
- `destination.partition` (`extract_run` | `none`) is DET run grain for
  **iceberg and bigquery** (default `extract_run`). BigQuery maps `extract_run`
  to a time partition on `__extract_run_datetime`.
- `destination.bigquery.partition_by` / `cluster_by` — BigQuery-native source
  layout. When `partition_by` is set it wins over the extract_run default.
- `det export-bq` — push existing filesystem or Iceberg bronze into BigQuery
  (Path B exploration). Same naming and replace-partition writer; gated by
  approvals like other write commands.

## [0.11.2] - 2026-11-09

### Fixed

- `PipelineRunner.run()` raised `ValueError: overrides cannot be applied when
  pipeline is already a PipelineConfig` when `--set` overrides were passed. `run()`
  resolves the pipeline config (with overrides applied) upfront, then forwarded the
  raw overrides sequence into `_extract()` and `_load()` as well, causing a
  double-resolve. The fix drops `overrides=` from both inner calls; the resolved
  `PipelineConfig` is already the source of truth at that point. (#116)

## [0.11.1] - 2026-11-07

### Removed

- Read-only fallback to `{project_root}/.det/approvals`. Approval load/list/claim
  use only the lake ops store (`{ops}/approvals/`) or Postgres
  (`DET_APPROVAL_BACKEND=postgres`). Old project-local approval files are ignored —
  re-run `det approve`.

## [0.11.0] - 2026-10-09

### Added

- `CatchupScope` (`.lookback()` / `.interval()` / `.census()`) on
  `iter_silver_catchup_holes` and `run_silver_catchup_heal`; omit scope still
  defaults to Mode A `48h`. Also exported: `DEFAULT_EXTRACT_LOOKBACK`.

### Changed

- BigQuery catch-up heal works with local and `s3://` ops by loading `.runs.jsonl`
  into a native `_det_catchup_runs_*` table; `gs://` keeps the external-table path.

## [0.10.5] - 2026-10-09

### Added

- Spark SQL renderer for Iceberg maintain plans (`render_iceberg_maintain_spark_sql`)
  plus a log-only DAG submit hook (`submit_log_spark_sql`).

### Changed

- Identifier validation for schema/table/z_order and safer SQL string escaping
  before DDL interpolation.
- Docs clarify GC vs prune, plan-only default, and that rest|glue actionable gating
  is DET plan API only.

## [0.10.4] - 2026-09-22

### Added

- Deep schema inference via genson for MCP `schema_from_sample_dry_run` (nested
  objects, array-of-object items).
- Mode A silver catch-up SemVer heal API and reference Airflow DAG
  (`det_silver_catchup_dag`).

### Changed

- Folds object|scalar `anyOf`/`oneOf` to string; structure-only unions stay for
  review with normalized branches.
- Iceberg inspect bounds: metadata-driven sampling, residual apply before limit,
  run_receipts partition validation.

## [0.10.2] - 2026-09-12

- Mode A boring heal route: MCP `silver_catchup_heal_dry_run` and CLI
  `det silver-catchup heal` / `heal --continue`.
- Apply `next_steps` bind the same scope flags as `approval_plan`.
- Docs/skills lead Mode A with heal; census/fleet/cleanup stay Advanced.

## [0.10.1] - 2026-09-12

- Centralize mutating approval CLI flags in `APPROVAL_BOUND_PARAMS` with
  signature lockstep tests.
- Harden silver catch-up apply/build/cleanup stop language in MCP `next_steps`
  and docs.
- Fix catch-up `dbt_dry_run` `next_steps` to preserve the approved `--command`.

## [0.10.0] - 2026-09-12

### Added

- Shared lake/postgres **approval store** with advisory claim heartbeats (never
  auto-release). Default store `{ops}/approvals/`; `DET_APPROVAL_BACKEND=postgres`
  is opt-in.

### Changed

- Approve/show/list/release take the same `--lake-path` / split roots as writers.
- `DetSettings` approval overrides beat `DET_APPROVAL_*` env; offset-less
  timestamps fail soft on inspect.

## [0.9.0] - 2026-09-11

### Removed

- Lake **layout 1** and `DET_LAKE_LAYOUT` / `--lake-layout`. Layout **2** only:
  `DET_LAKE_PATH` (or split `DET_LAKE_PATH_*` roots) is the supported lake contract.

### Changed

- Silver catch-up happy path: default **48h** lookback, `det silver-catchup` CLI
  group, tighter approval guidance for plan/apply/build.
- Approvals bind effective lake roots into digests; reject combining `--lake-path`
  with split `DET_LAKE_PATH_*` roots.
