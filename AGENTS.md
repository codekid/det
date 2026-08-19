# DET — agent contract

DET extracts wire bytes to **raw**, lands typed **bronze**, then **dbt** owns silver/gold.
**dlt never lands bronze** — HTTP helpers only; DET owns validation, meta, and writers.

Canonical pipeline id is `provider.source` (e.g. `noaa.storm_events`). Interval `-s` is
inclusive, `-e` exclusive (default start + 1 day). Lake hive/SQL contract:
[docs/lake-layout.md](docs/lake-layout.md).

## Hard rules

- Prefer the **det** MCP server for inspect and dry-run. Never extract/load/run,
  `det migrate` (write), `det prune --apply`, DagRun triggers, or non-dry-run
  scaffold/init through MCP (those tools are not exposed).
- Never chain a dry-run preview into a writing CLI command in the same turn. Show
  the plan and wait for the user to confirm.
- Never suggest `dlt.pipeline` / `pipeline.run` for landing.
- Pipeline YAML holds secret **names** (`auth_env`, `connection_env`); values stay in env.
- Source plugins are discovered from `src/det/sources/<provider>/<source>.py`
  (`name` must match the path). Do not list them in `plugins.py`.

## MCP vs CLI

| Goal | Prefer |
| --- | --- |
| Lake/schema/config inspect | MCP `diagnose_pipeline`, `check`, `sample_*`, `list_runs` |
| dbt catalog (physical models) | MCP `list_models` / `describe_model` |
| Gold / fleet metrics | MCP `cube_meta` / `cube_load` (`make cube-up`) |
| Silver or ops row detail | MCP `query_analytics` (`warehouse=analytics` or `ops`) |
| Draft schema / mapper / migrate / prune / dbt | MCP `*_dry_run` then CLI after confirm |
| Extract, load, run, apply prune, write migrate | CLI / Airflow after explicit approval |

This-run receipts: `list_runs`. Fleet: `cube_load` on `run_daily` or `query_analytics` `warehouse=ops`.
Do not invent SQL for certified gold/ops measures.

MCP prompts (same text as Cursor skills): `det_ops`, `det_new_source`, `det_migrate`,
`det_dbt`, `det_airflow`. Playbooks on disk:
`.cursor/skills/det-ops|det-new-source|det-migrate|det-dbt|det-airflow/SKILL.md`.

## MCP setup

`uv pip install -e ".[mcp]"`. Cursor: [`.cursor/mcp.json`](.cursor/mcp.json).
If discovery fails with `No module named 'det'` on macOS, `make unhide`.
Details: [`.cursor/rules/det-mcp.mdc`](.cursor/rules/det-mcp.mdc).
