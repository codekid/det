# DET — agent contract

DET extracts wire bytes to **raw**, lands typed **bronze**, then **dbt** owns silver/gold.
**dlt never lands bronze** — HTTP helpers only; DET owns validation, meta, and writers.

Canonical pipeline id is `provider.source` (e.g. `noaa.storm_events`). Interval `-s` is
inclusive, `-e` exclusive (default start + 1 day). Lake hive/SQL contract:
[docs/lake-layout.md](docs/lake-layout.md). Prefer `DET_LAKE_MODE` + `DET_LAKE_PATH`
(single root); do not invent dual raw/bronze roots **unless** using layout 2
(`DET_LAKE_PATH_RAW` / `_BRONZE` / `_OPS` — see docs/lake-layout.md).

## Hard rules

- Prefer the **det** MCP server for inspect and dry-run. Never extract/load/run,
  `det migrate` (write), `det prune --apply`, DagRun triggers, or non-dry-run
  scaffold/init through MCP (those tools are not exposed). Agents drive the
  analytics adapter (scaffold / dbt) via MCP dry-run + CLI — not by importing
  scaffold from `det.runtime`.
- Never chain a dry-run preview into a writing CLI command in the same turn. Show
  the plan and wait for the user to confirm. After they confirm, the operator
  runs `det approve --plan` (or `--command` / `--argv-json`) with `--approved-by`.
  A later turn may run writing CLI with `--approval <id>` — never in the same
  turn as the dry-run. `DET_REQUIRE_APPROVAL=1` (or `--require-approval`) makes
  the id mandatory; default is off so local/CI extract is unchanged. Prefer
  `DET_REQUIRE_APPROVAL=1` in agent sessions. Airflow scheduled extract/load
  stay ungated; prune-**apply** on `det_extract_bronze` needs DagRun conf
  `"approval": "apr_…"` (same `.det/approvals/` files as CLI — not MCP triggers).
  Manual `det_backfill_extract_bronze` likewise needs conf `"approval"` for the
  backfill **window** (MCP `preview_backfill_conf` → `det approve`); spawned
  daily extract runs stay ungated. Do not set `DET_REQUIRE_APPROVAL=1` on
  Compose for the scheduler.
- Approvals are **audit and intent-binding, not authorization** — the same shell
  can run `det approve`. Do not describe them as a security boundary. What they
  do guarantee: the record accurately describes the command that runs. Every
  flag that changes what or where data is written is in `plan_digest`, and an
  unrecognized flag is rejected (`approval_unbound_flag`) rather than escaping
  it. So **re-approve when you change any flag**, including `--lake-path`,
  `--set`, `--full-refresh`, `--catchup`, `--target`, and `--ingestion`. Pipeline refs and
  intervals are canonicalized, so `noaa/storm_events` and `noaa.storm_events`,
  or `-s 2026-08-06` and `-s 2026-08-06T00:00:00+00:00`, share one digest.
- A crashed run leaves its approval `claimed`, and a claim never expires, so it
  is hidden from the default listing. Find it with `det list-approvals --status
  claimed` (or MCP `list_approvals` `status="claimed"`). Recovery is either a new
  approval or, once the worker is dead, the operator running
  `det approval-release <id> --force`. Releasing is not a TTL bypass and never
  automatic — never suggest releasing an approval whose run may still be alive.
- Never suggest `dlt.pipeline` / `pipeline.run` for landing.
- Pipeline YAML holds secret **names** (`auth_env`, `connection_env`); values stay in env.
- Source plugins are discovered from `src/det/sources/<provider>/<source>.py`
  (`name` must match the path). Do not list them in `plugins.py`.
- `dbt.stg` is a **closed** scaffold knob set (unknown keys fail at load). Prefer
  hand-edited stg SQL for one-offs; see [docs/contract-triangle.md](docs/contract-triangle.md).

## MCP vs CLI

| Goal | Prefer |
| --- | --- |
| Lake/schema/config inspect | MCP `diagnose_pipeline`, `check`, `sample_*`, `list_runs` |
| dbt catalog (physical models) | MCP `list_models` / `describe_model` |
| Gold / fleet metrics | MCP `cube_meta` / `cube_load` (`make cube-up`) |
| Silver or ops row detail | MCP `query_analytics` (`warehouse=analytics` or `ops`) |
| Draft schema / mapper / migrate / prune / dbt / silver catch-up | MCP `*_dry_run` (`approval_plan`) then `det approve` then CLI `--approval` |
| Bronze↔silver catch-up | MCP `diff_bronze_silver` (inspect) then `silver_catchup_dry_run` (routine: `extract_lookback=48h`; full census: omit) → show `approval_plan`, **stop**; after confirm: `det approve` then later-turn `det silver-catchup-plan --apply --manifest-id <scm_…> --content-digest <sha256:…> --approval <id>`; then MCP `dbt_dry_run(catchup=True, catchup_manifest=<scm_…>)` → show its `approval_plan`, **stop**; after confirm: separate approve → later-turn `det dbt --catchup --catchup-manifest <scm_…> --approval <id>` (immutable `scm_…` + `.runs.jsonl`; DuckDB or BQ-on-GCS; [docs/silver-catchup.md](docs/silver-catchup.md)). After BQ heal verify: MCP `silver_catchup_cleanup_dry_run` → show `approval_plan`, **stop**; after confirm: approve → later-turn `det silver-catchup-cleanup --apply --manifest-id <scm_…> --approval <id>` or `--created-before <iso> --approval <id>` (selector from dry-run `approval_plan`; heal does not auto-drop `_det_catchup_runs_*`). Never chain any of these dry-runs into a write in the same turn. |
| Ops dbt models for embedders | After `runs-materialize`: MCP `scaffold_ops_dry_run` → show plan, wait for confirm → `det approve`; later turn `det scaffold-ops --approval <id>` |
| Extract, load, run, apply prune, write migrate | CLI / Airflow after `det approve` (and `--approval` when required) |

This-run receipts: `list_runs`. Fleet: `cube_load` on `run_daily` or `query_analytics` `warehouse=ops`.
Do not invent SQL for certified gold/ops measures.

MCP prompts (same text as Cursor skills): `det_ops`, `det_new_source`, `det_migrate`,
`det_dbt`, `det_airflow`. Playbooks on disk:
`.cursor/skills/det-ops|det-new-source|det-migrate|det-dbt|det-airflow/SKILL.md`.

## Trajectory evals

Agent sequences (MCP + CLI argv, not live models) are scored by `det.mcp.policy`
against JSON fixtures under `tests/mcp/trajectories/`. A skill path needs a good
trace (inspect/dry-run, then stop) and a bad trace (chain write, skip inspect, or
invent gold/ops SQL). `user_approval` marks a later turn that may run writing CLI (fixtures are not live
approval files). Live writes use `--approval <id>` from `det approve`.

## MCP setup

`uv pip install -e ".[mcp]"`. Cursor: [`.cursor/mcp.json`](.cursor/mcp.json).
If discovery fails with `No module named 'det'` on macOS, `make unhide`.
Details: [`.cursor/rules/det-mcp.mdc`](.cursor/rules/det-mcp.mdc).
