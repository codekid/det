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
- This doc is the **BigQuery external registration** path (`hadoop` writes +
  `det biglake-register`). Committing through the Lakehouse **REST** catalog at
  load time is separate — see [iceberg-catalog.md](iceberg-catalog.md).

## Auth

| Concern | How |
| --- | --- |
| DET extract/load (gcsfs / PyIceberg) | Application Default Credentials, or `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account JSON. Emulator: `STORAGE_EMULATOR_HOST` (+ optional `GOOGLE_CLOUD_PROJECT`). |
| dbt-bigquery | Same ADC / SA; profile uses `method: oauth` or `service-account`. |
| YAML | Secret **names** only — never keys in pipeline config. |

**IAM sketch** (sandbox SA):

- GCS: object Admin (or finer read/write) on the lake bucket for **your** ADC /
  workload SA (`det extract` / `det load`).
- BigQuery: Job User + Data Editor on silver/gold/ops datasets for dbt.
- BigLake connection SA: **`roles/storage.objectViewer`** on the lake bucket —
  separate from your user; see [Prerequisites](#prerequisites-before-first-register).

## Prerequisites (before first register)

Three principals — operators often set up only the first:

| Principal | Needs | Used for |
| --- | --- | --- |
| Operator ADC / workload SA | GCS write on lake bucket | `det extract` / `det load` |
| **Connection SA** (`bqcx-…@gcp-sa-bigquery-condel.iam.gserviceaccount.com`) | GCS **read** on lake bucket | BigLake external tables, `SELECT` on bronze/ops |
| Same identity as dbt | BQ Job User + dataset permissions | dbt silver/gold/ops builds |

DET does **not** auto-create connections or auto-grant IAM — operator-owned
provisioning.

**1. Create the BigLake connection** (once per project/region; default
`DET_BQ_CONNECTION=det-lake-conn`):

```bash
export DET_GCP_PROJECT=your-project
export DET_BQ_LOCATION=US
export DET_BQ_CONNECTION=det-lake-conn

# Follow current Google docs for exact flags; example shape:
bq mk --connection --connection_type=CLOUD_RESOURCE \
  --location="$DET_BQ_LOCATION" --project_id="$DET_GCP_PROJECT" \
  "$DET_BQ_CONNECTION"
```

**2. Resolve the connection service account:**

```bash
bq show --connection --location="$DET_BQ_LOCATION" \
  --project_id="$DET_GCP_PROJECT" "$DET_BQ_CONNECTION"
# → serviceAccountId (field name may vary by bq version)
```

**3. Grant bucket read** (minimum for Iceberg metadata + data):

```bash
export DET_GCS_BUCKET=your-bucket   # from DET_LAKE_PATH=gs://BUCKET/det-lake

gcloud storage buckets add-iam-policy-binding "gs://${DET_GCS_BUCKET}" \
  --member="serviceAccount:CONNECTION_SA_EMAIL" \
  --role="roles/storage.objectViewer"
```

Run `det biglake-register --dry-run` before `--apply` — it prints an IAM hint
(with copy-paste `gcloud` when the connection already exists).

## Troubleshooting

- **403 on register or `SELECT` on bronze/ops:** connection SA missing
  `storage.objectViewer` on the bucket, or wrong bucket / connection name.
- **Connection not found:** create with `bq mk --connection` before `--apply`.
- **dbt dataset not found:** run register + dbt; register auto-creates bronze/ops
  **datasets**; dbt creates silver/gold/ops **native** tables.

## Teardown (full disposable sandbox)

Manual only — no `det biglake-teardown`. Iceberg bytes live in GCS; deleting
the bucket removes lake data.

```bash
export DET_GCP_PROJECT=your-project
export DET_BQ_LOCATION=US
export DET_BQ_CONNECTION=det-lake-conn
export DET_GCS_BUCKET=your-bucket   # from DET_LAKE_PATH=gs://BUCKET/…
```

1. **BQ datasets** — drop everything created by register + dbt (adjust if you
   only ran a subset):

```bash
# External tables from register: bronze_*, ops (BigLake)
# Native tables from dbt: silver_*, gold, ops (dbt may replace ops.run_receipts)
for ds in bronze_example_api bronze_noaa bronze_openlibrary \
          silver_example_api silver_noaa silver_openlibrary ops gold; do
  bq rm -r -f -d "${DET_GCP_PROJECT}:${ds}" 2>/dev/null || true
done
```

2. **BigLake connection:**

```bash
bq rm --connection --location="$DET_BQ_LOCATION" \
  --project_id="$DET_GCP_PROJECT" "$DET_BQ_CONNECTION"
```

3. **GCS bucket:**

```bash
gcloud storage rm -r "gs://${DET_GCS_BUCKET}/**" || true
gcloud storage buckets delete "gs://${DET_GCS_BUCKET}" --quiet
```

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
# Prints table plan + IAM hint (connection SA bucket binding when connection exists)
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

4. dbt reads BigLake table refs from the same `sources.yml` when
   `--target bigquery` (DuckDB `iceberg_scan` / `read_json` meta is Jinja-gated
   off for that target).

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

**Single sources file:** `dbt/models/silver/sources.yml` and
`dbt/models/ops/sources.yml` cover both engines. Inline Jinja sets
`database` / schema for BigQuery and leaves DuckDB `meta.external_location`
(`iceberg_scan` / `read_json`) empty when `target.name == 'bigquery'`. Do **not**
add a parallel `sources_bigquery.yml` with the same source names — dbt 1.12
parses both and errors on duplicates (`config.enabled` does not prevent that).

**Ops on GCS:** materialize receipts, register BigLake `ops.run_receipts`, then:

```bash
det runs-materialize
det biglake-register --apply …   # includes ops unless --skip-ops
# Models only (infra smoke): SLO tests need clean seeded receipts
det dbt --select 'tag:ops,resource_type:model'
# Full ops + SLO tests (after green extract/load for pipelines in ops_slo_expected)
det dbt --select tag:ops
```

`DET_DBT_TARGET=bigquery` makes `tag:ops` use the BigQuery profile (not DuckDB
`ops`). Seeded SLOs (`ops_slo_expected`, from pipeline `slo:`) fail closed on
error receipts / missing ok attempts — mixed smoke failures will fail those
tests even when models build.

Macros (`det_bronze_cast`, `det_json_path`, `det_dedupe_latest_run`,
`det_sql_compat`) and silver incremental models branch on
`target.name == 'bigquery'`.

**Silver partition / cluster:** opt-in via pipeline `dbt.silver.bigquery` and
`dbt.stg.relations.*.bigquery` (`partition_by`, `cluster_by`,
`require_partition_filter`). Scaffold wraps those knobs in
`{% if target.name == 'bigquery' %}` so DuckDB builds stay unchanged. This is
**not** Iceberg `destination.partition` (lake layout). Typical choice: partition
on `__extract_run_datetime` (day); cluster parent on identity / relation on
`[parent_key, …spine]`. Requires silver `materialized: table` or `incremental`.

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
- [iceberg-catalog.md](iceberg-catalog.md) — `DET_ICEBERG_CATALOG=hadoop|rest|glue`
  (Lakehouse REST commits at load time vs this doc’s post-hoc BigLake register)
- [getting-started-library.md](getting-started-library.md) — embedders
- Operator README — destinations table
