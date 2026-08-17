---
name: det-airflow
description: >-
  Local Airflow Compose for DET: DAG roles, env knobs, backfill conf, MCP
  inspect tools, and DuckDB path pitfalls. Extract/load stay on CLI or
  Airflow tasks — not MCP.
---

# DET Airflow (local Compose)

## Bring-up

```bash
make airflow-up          # copies airflow/.env.example → .env if missing
# UI: http://localhost:8080  (airflow / airflow)
make airflow-logs
make airflow-down
```

Config: `airflow/.env` (from `airflow/.env.example`). Shared helpers in
`dags/det_env.py`.

## MCP inspect (read-only)

Prefer these before shelling into Compose. Configure the agent→API client with
`DET_AIRFLOW_*` (defaults match local Compose). **Never** trigger DagRuns via MCP.

| Tool | Use |
| --- | --- |
| `airflow_health` | Is the webserver up? |
| `list_airflow_dags` | DET DAGs paused / import errors |
| `list_airflow_dag_runs` | Recent runs for a `dag_id` |
| `describe_airflow_det_env` | Local `airflow/.env` `DET_*` (passwords redacted) |
| `preview_backfill_conf` | Conf JSON + Compose / generic trigger strings |

| Agent client knob | Default |
| --- | --- |
| `DET_AIRFLOW_BASE_URL` | `http://localhost:8080` |
| `DET_AIRFLOW_USER` / `PASSWORD` | from `airflow/.env` or `airflow` |
| `DET_AIRFLOW_AUTH` | `basic` only (bearer/IAM later for cloud) |
| `DET_AIRFLOW_TIMEOUT_SEC` | `10` |

Unreachable local URL → `make airflow-up`. For a future remote Airflow, point
`DET_AIRFLOW_BASE_URL` (+ creds) only; same tools.

## DAGs

| DAG | Role |
| --- | --- |
| `det_extract_bronze` | extract → load → optional prune (`@daily`) |
| `det_backfill_extract_bronze` | trigger extract once per day for `[start, end)` |
| `det_dbt_silver_gold` | single-process `dbt build` on analytics DuckDB with `--exclude tag:ops` |
| `det_ops_receipts` | materialize `runs/` → Iceberg, then `dbt build --select tag:ops --target ops` on `DET_OPS_DUCKDB` |

Extract, silver/gold, and ops are **decoupled**. Nightly analytics dbt is not limited to
`DET_PIPELINE_CONFIG`. File-backed DuckDB cannot safely fan out per-model writers, so
each dbt DAG is one task.

## Important env (workers / Compose)

| Var | Notes |
| --- | --- |
| `DET_PROJECT_ROOT` | Project mount (Compose default `/opt/det`) |
| `DET_PIPELINE_CONFIG` | Canonical id for extract/load DAGs (not dbt select) |
| `DET_PIPELINE_OVERRIDES` | Comma-separated `dotted.key=value` (same as `det --set`); leave empty for live NOAA |
| `DET_DBT_SELECT` | Optional dbt `--select`; unset = entire analytics project (still excludes `tag:ops`) |
| `DET_BRONZE_SOURCE` / `DET_BRONZE_SCHEMA` | dbt bronze reader |
| `DET_ANALYTICS_DUCKDB` | Prefer **absolute** path in Compose |
| `DET_OPS_DUCKDB` | Prefer **absolute** path for ops dbt target (Compose default `/opt/det/data/det_ops.duckdb`). File stem must not be `ops` (catalog/schema clash). |
| `DET_LOG_FORMAT` | Compose sets `json` so task logs are greppable (`pipeline`, `extract_run_datetime`) |
| `DET_PRUNE` / `DET_PRUNE_APPLY` / `DET_PRUNE_KEEP` | Optional prune after load |
| `DET_LOCK_TTL_SEC` | Lake lease TTL seconds (default 7200). Per-run: DAG conf `lock_ttl_sec` or `det --lock-ttl-sec` |
| `DET_LOCK=0` | Disables the lake lease (unsafe; tests only) |
| `DET_LOCK_OWNER` | Set by `set_lock_owner` to `airflow:{dag_id}:{run_id}`. Correlates lake leases **and** `{lake}/runs/` receipts to a DagRun. `list_runs` / `det runs` filter on `owner`. |
| `DET_RUN_RECEIPTS=0` | Disables extract/load receipt writing (tests only) |

Do **not** set `max_active_runs=1` on `det_extract_bronze` to “fix” locking — that serializes backfill of many days. The lake lease is per `(pipeline, interval)`; cap backfill with a pool / mapped TI limit later.

Wedged lock after a dead worker (TTL still in the future): confirm the DagRun/CLI is dead, then `det lock-release -p … -s … --force` or trigger manual DAG `det_clear_lock` with `force: true`. MCP must not delete locks.

## Backfill

Preview first: MCP `preview_backfill_conf`. Apply only after user confirms:

```bash
cd airflow && docker compose exec airflow-scheduler \
  airflow dags trigger det_backfill_extract_bronze --conf '{
    "interval_start": "2026-08-01",
    "interval_end": "2026-08-08"
  }'
```

DET interval is half-open `[start, end)`.

## Lake debugging

Use MCP lake inspect (`diagnose_pipeline`, `diff_partitions`, `list_runs`, …) against
`DET_PROJECT_ROOT` — **do not** run extract/load via MCP. Receipt `owner` is
`airflow:{dag_id}:{run_id}` when the DAG called `set_lock_owner`.

## Hard rules

- No MCP extract/load/prune-apply/DagRun trigger.
- Do not suggest `dlt.pipeline` for landing.
- Prefer absolute `DET_ANALYTICS_DUCKDB` in Compose.
