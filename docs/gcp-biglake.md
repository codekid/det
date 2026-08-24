# GCS Iceberg lake + BigLake / dbt-BigQuery (architecture C)

**Bronze** stays Iceberg on `gs://` (DET owns extract/load/prune/migrate). **BigQuery**
reads that table via **BigLake Iceberg** — no second bronze copy. **dbt-bigquery**
builds **native** silver/gold/ops in BQ.

```text
SourcePlugin → extract → raw on gs:// → load → Iceberg bronze on gs://
                                              ↓
                                    BigLake Iceberg table (BQ)
                                              ↓
                                    dbt-bigquery → native BQ silver/gold/ops
```

## Non-goals

- No `destination.type: bigquery` (DET does not land bronze in BQ).
- No Iceberg→BQ load/copy for bronze; no dual-write.
- Local/CI DuckDB + MinIO (`s3://`) stay the default analytics path.
- Emulators do **not** cover BigLake Iceberg end-to-end — use a real GCP sandbox
  for register/`SELECT`/dbt-BQ. CI covers GCS + Iceberg write/read via
  `STORAGE_EMULATOR_HOST` (fake-gcs / localgcp) and marker `gcs`.

## Auth

| Concern | How |
| --- | --- |
| DET extract/load (gcsfs / PyIceberg) | Application Default Credentials, or `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account JSON. Emulator: `STORAGE_EMULATOR_HOST` (+ optional `GOOGLE_CLOUD_PROJECT`). |
| dbt-bigquery | Same ADC / SA; profile uses `method: oauth` or `service-account`. |
| YAML | Secret **names** only — never keys in pipeline config. |

**IAM sketch** (sandbox SA):

- GCS: object Admin (or finer read/write) on the lake bucket prefix.
- BigQuery: Job User + Data Editor on silver/gold/ops datasets.
- BigLake / connection: use a Cloud resource connection that can read the GCS
  Iceberg warehouse (see Google BigLake Iceberg docs for the current connector).

## Lake + pipeline

```bash
export DET_LAKE_MODE=cloud
export DET_LAKE_PATH=gs://YOUR_BUCKET/det-lake
# ADC already configured, or:
# export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
# export GOOGLE_CLOUD_PROJECT=your-project

det extract -p example_api.events -s 2026-08-06
det load -p example_api.events -s 2026-08-06
```

Pipeline stays `destination.type: iceberg` (default). Physical table:

`{DET_LAKE_PATH}/bronze/{provider}/{source}_vN/`

SQL naming (BigLake registration): one BQ **dataset** per provider
`bronze_{provider}`, table leaf `{source}_vN` — same contract as
[lake-layout.md](lake-layout.md). Ops receipts register as dataset **`ops`**, table
**`run_receipts`**.

## Register BigLake

### CLI (recommended)

Dry-run (agents / operators preview):

```bash
export DET_GCP_PROJECT=your-project
export DET_BQ_LOCATION=US
export DET_BQ_CONNECTION=det-lake-conn   # optional; default det-lake-conn

det biglake-register --dry-run --lake-path gs://YOUR_BUCKET/det-lake
```

Apply after approval:

```bash
det approve --plan '<approval_plan JSON>' --approved-by you
det biglake-register --apply --lake-path gs://YOUR_BUCKET/det-lake --approval apr_…
```

Register one pipeline only: `--pipeline example_api.events` (skips ops). Skip ops on
full-lake register: `--skip-ops`.

MCP inspect: `biglake_register_dry_run` (never writes).

### Manual (operator)

1. Create dataset `bronze_{provider}` in the GCP project (once per provider).
2. Create a BigLake Iceberg table whose storage URI is the Iceberg warehouse
   directory above (metadata + data under that prefix). Follow current Google
   docs for `CREATE TABLE … WITH CONNECTION` / Iceberg format options — syntax
   evolves; do not copy stale DDL from blogs blindly.
3. Smoke:

```sql
SELECT COUNT(*) FROM `project.bronze_example_api.events_v1`;
```

4. dbt reads via `sources_bigquery.yml` when `--target bigquery` (no DuckDB
   `iceberg_scan`).

## dbt-bigquery

```bash
uv pip install -e ".[dbt,bigquery]"   # or: uv pip install dbt-bigquery
export DET_GCP_PROJECT=your-project
export DET_DBT_TARGET=bigquery
# DET_BQ_DATASET optional (default analytics / silver schema via dbt)
det dbt -p example_api.events --target bigquery
```

Profile target `bigquery` is defined in `dbt/profiles.yml`. On `gs://` lakes,
`det dbt` does **not** force `duckdb_s3` — set `DET_DBT_TARGET=bigquery` (or
`--target bigquery`) for the GCP path. Keep DuckDB targets for local/MinIO.

**Dual sources:** `dbt/models/silver/sources.yml` (DuckDB `iceberg_scan`, enabled
when target ≠ bigquery) and `dbt/models/silver/sources_bigquery.yml` (BigLake BQ
tables, enabled when target = bigquery). Same pattern for ops:
`dbt/models/ops/sources.yml` vs `sources_bigquery.yml`.

**Ops on GCS:** materialize receipts, register BigLake `ops.run_receipts`, then:

```bash
det runs-materialize
det biglake-register --apply …   # includes ops unless --skip-ops
det dbt --select tag:ops         # uses bigquery when DET_DBT_TARGET=bigquery
```

Macros (`det_bronze_cast`, `det_json_path`, `det_dedupe_latest_run`) and silver
incremental models branch on `target.name == 'bigquery'`.

## CI / emulator

| Env | Role |
| --- | --- |
| `STORAGE_EMULATOR_HOST` | GCS API host (`localhost:4443` or `http://…`) — fake-gcs-server / localgcp |
| `GOOGLE_CLOUD_PROJECT` | Emulator project id |
| `DET_GCS_BUCKET` | Bucket name (CI creates `det-ci`) |

Marker: `pytest -m gcs`. Job: `.github/workflows/ci.yml` → **GCS Iceberg soak**.
MinIO soak stays separate (`-m minio`). Optional manual sandbox: `pytest -m bigquery`.

## Related

- [lake-layout.md](lake-layout.md) — hive + SQL names
- [getting-started-library.md](getting-started-library.md) — embedders
- Operator README — destinations table
