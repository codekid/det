# GCS Iceberg lake + BigLake / dbt-BigQuery (architecture C)

**Bronze** stays Iceberg on `gs://` (DET owns extract/load/prune/migrate). **BigQuery**
reads that table via **BigLake Iceberg** — no second bronze copy. **dbt-bigquery**
builds **native** silver/gold in BQ.

```text
SourcePlugin → extract → raw on gs:// → load → Iceberg bronze on gs://
                                              ↓
                                    BigLake Iceberg table (BQ)
                                              ↓
                                    dbt-bigquery → native BQ silver/gold
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
- BigQuery: Job User + Data Editor on silver/gold datasets.
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
[lake-layout.md](lake-layout.md).

## Register BigLake (operator)

1. Create dataset `bronze_{provider}` in the GCP project (once per provider).
2. Create a BigLake Iceberg table whose storage URI is the Iceberg warehouse
   directory above (metadata + data under that prefix). Follow current Google
   docs for `CREATE TABLE … WITH CONNECTION` / Iceberg format options — syntax
   evolves; do not copy stale DDL from blogs blindly.
3. Smoke:

```sql
SELECT COUNT(*) FROM `project.bronze_example_api.events_v1`;
```

4. Point dbt `source()` at that BQ table (no DuckDB `iceberg_scan`).

Example dbt source fragment (BigQuery target; not used by the default DuckDB
`sources.yml`):

```yaml
version: 2
sources:
  - name: bronze_example_api
    database: "{{ env_var('DET_GCP_PROJECT') }}"
    schema: bronze_example_api
    tables:
      - name: events_v1
        description: BigLake Iceberg over gs://…/bronze/example_api/events_v1
```

See also `dbt/models/silver/sources_bigquery.yml.example` in this repo.

## dbt-bigquery

```bash
uv pip install -e ".[dbt,bigquery]"   # or: uv pip install dbt-bigquery
export DET_DBT_TARGET=bigquery
export DET_GCP_PROJECT=your-project
# DET_BQ_DATASET optional (default analytics / silver schema via dbt)
det dbt -p example_api.events --target bigquery
```

Profile target `bigquery` is defined in `dbt/profiles.yml`. On `gs://` lakes,
`det dbt` does **not** force `duckdb_s3` — set `DET_DBT_TARGET=bigquery` (or
`--target bigquery`) for the GCP path. Keep DuckDB targets for local/MinIO.

Ops / `tag:ops` remain DuckDB-only (`DET_OPS_DUCKDB`).

## CI / emulator

| Env | Role |
| --- | --- |
| `STORAGE_EMULATOR_HOST` | GCS API host (`localhost:4443` or `http://…`) — fake-gcs-server / localgcp |
| `GOOGLE_CLOUD_PROJECT` | Emulator project id |
| `DET_GCS_BUCKET` | Bucket name (CI creates `det-ci`) |

Marker: `pytest -m gcs`. Job: `.github/workflows/ci.yml` → **GCS Iceberg soak**.
MinIO soak stays separate (`-m minio`).

## Related

- [lake-layout.md](lake-layout.md) — hive + SQL names
- [getting-started-library.md](getting-started-library.md) — embedders
- Operator README — destinations table
