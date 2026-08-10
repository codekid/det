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
| `det_dbt_silver_gold` | single-process `dbt build` — **full project** by default (separate schedule) |

Extract and dbt are **decoupled**. Nightly dbt is not limited to `DET_PIPELINE_CONFIG`.
File-backed DuckDB cannot safely fan out per-model writers, so dbt is one task.

## Important env (workers / Compose)

| Var | Notes |
| --- | --- |
| `DET_PROJECT_ROOT` | Project mount (Compose default `/opt/det`) |
| `DET_PIPELINE_CONFIG` | Canonical id for extract/load DAGs (not dbt select) |
| `DET_PIPELINE_OVERRIDES` | Comma-separated `dotted.key=value` (same as `det --set`); leave empty for live NOAA |
| `DET_DBT_SELECT` | Optional dbt `--select`; unset = entire dbt project |
| `DET_BRONZE_SOURCE` / `DET_BRONZE_SCHEMA` | dbt bronze reader |
| `DET_ANALYTICS_DUCKDB` | Prefer **absolute** path in Compose |
| `DET_PRUNE` / `DET_PRUNE_APPLY` / `DET_PRUNE_KEEP` | Optional prune after load |

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

Use MCP lake inspect (`diagnose_pipeline`, `diff_partitions`, …) against
`DET_PROJECT_ROOT` — **do not** run extract/load via MCP.

## Hard rules

- No MCP extract/load/prune-apply/DagRun trigger.
- Do not suggest `dlt.pipeline` for landing.
- Prefer absolute `DET_ANALYTICS_DUCKDB` in Compose.
