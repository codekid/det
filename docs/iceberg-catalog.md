# Iceberg catalog backends

DET lands Iceberg bronze under `{lake}/bronze/{provider}/{source}_vN/`. The
**catalog** decides how engines discover and commit table metadata:

| `DET_ICEBERG_CATALOG` | Backend | Typical use |
| --- | --- | --- |
| `hadoop` (default) | DET `LakeHadoopCatalog` + `metadata/version-hint.text` | Local / MinIO / GCS with DuckDB `iceberg_scan`; optional [`biglake-register`](gcp-biglake.md) for BigQuery externals |
| `rest` | PyIceberg REST catalog | GCP Lakehouse runtime catalog, Polaris, Glue Iceberg REST, Tabular, … |
| `glue` | PyIceberg Glue catalog | Classic AWS Glue Data Catalog (Athena / EMR) |

Catalog choice is **lake-wide env**, not per-pipeline YAML. Table file layout does
**not** change when you switch catalogs ([lake-layout.md](lake-layout.md)).

SQL / Iceberg identity stays `{medallion}_{provider}.{source}_vN`
(e.g. `bronze_noaa.storm_events_v1`) — see `det.runtime.ids.sql_names_for_config`.

## Environment

| Variable | Role |
| --- | --- |
| `DET_ICEBERG_CATALOG` | `hadoop` \| `rest` \| `glue`. Unset/empty → `hadoop`. If unset but `DET_ICEBERG_REST_URI` is set → soft-default `rest`. Never auto-picks `glue` from `s3://` alone. |
| `DET_ICEBERG_REST_URI` | Required for `rest` — catalog HTTP endpoint |
| `DET_ICEBERG_REST_WAREHOUSE` | Optional warehouse / catalog id (defaults to the lake URI) |
| `DET_ICEBERG_REST_CREDENTIAL` | Optional `client_id:secret` or token. Prefer ADC / IAM on GCP and AWS when possible. **Secret** — do not put in pipeline YAML. |
| `DET_ICEBERG_REST_SCOPE` | Optional OAuth scope (Polaris: `PRINCIPAL_ROLE:ALL`) |
| `DET_ICEBERG_REST_REALM` | Optional `header.Polaris-Realm` (Polaris: `POLARIS`) |
| `DET_ICEBERG_GLUE_ID` | Optional Glue catalog id (cross-account) |
| `AWS_REGION` / `AWS_*` / ADC | FileIO + Glue/REST signing — same as object-store lake I/O |

URI / warehouse / glue id are **config** (non-secret env). Credentials are
**secrets**. Operators must set catalog env before approve/run when
`DET_REQUIRE_APPROVAL=1` (same class of concern as `DET_LAKE_PATH`).

## Register existing tables (`det iceberg-register`)

Tables written under **hadoop** only have lake files + `version-hint`. To publish
them into REST/Glue after flipping `DET_ICEBERG_CATALOG`:

```bash
export DET_ICEBERG_CATALOG=rest
export DET_ICEBERG_REST_URI=…
# … warehouse / credential as needed

det iceberg-register --dry-run --lake-path "$DET_LAKE_PATH"
# Operator: det approve --plan '…' --approved-by you
det iceberg-register --apply --lake-path "$DET_LAKE_PATH" --approval apr_…
```

- Refuses `DET_ICEBERG_CATALOG=hadoop` (no external metastore).
- Default: all bronze tables under `{lake}/bronze/` plus `ops.run_receipts`.
- `--pipeline` / `-p` registers one bronze table and skips ops.
- Idempotent: already-visible tables report `exists`.
- MCP: `iceberg_register_dry_run` (never writes).

This is **not** [`det biglake-register`](gcp-biglake.md) (BigQuery external tables).

## Local Polaris + MinIO

No cloud accounts required. Compose under [`docker/polaris-minio/`](../docker/polaris-minio/):

```bash
make polaris-up          # MinIO :9000 + Polaris :8181, warehouse det_lake
eval "$(make -s polaris-env)"

export DET_LAKE_PATH=s3://det-ci/det-lake
det run -p example_api.events -s 2026-08-06   # with REST env set → greenfield
# or: write with hadoop, then iceberg-register --apply
make polaris-down
```

Printed env includes `DET_ICEBERG_REST_CREDENTIAL=root:s3cr3t` (compose bootstrap
only — not for production).

CI starts the same compose for `-m minio` and `-m polaris` soaks and sets
`DET_ICEBERG_CATALOG=hadoop` at the job level so the soft-default (URI → rest)
does not route the whole pytest suite at Polaris. Polaris tests set
`DET_ICEBERG_CATALOG=rest` themselves. There is **no** live GCP Lakehouse or AWS
Glue account soak.

## Examples

### Local / CI (default)

```bash
# unset DET_ICEBERG_CATALOG → hadoop
export DET_LAKE_PATH="$PWD/data/lake"
det run -p noaa.storm_events -s 2026-08-06
```

DuckDB dbt keeps using path-based `iceberg_scan('…/bronze/noaa/storm_events_v1')`.

### GCP Lakehouse REST

```bash
export DET_LAKE_MODE=cloud
export DET_LAKE_PATH=gs://YOUR_BUCKET/det-lake
export DET_ICEBERG_CATALOG=rest
export DET_ICEBERG_REST_URI=https://biglake.googleapis.com/iceberg/v1/restcatalog
export DET_ICEBERG_REST_WAREHOUSE=bl://projects/PROJECT/catalogs/CATALOG_ID
# ADC / workload identity — usually no DET_ICEBERG_REST_CREDENTIAL
```

This is different from [`det biglake-register`](gcp-biglake.md), which creates
**BigQuery BigLake external** tables over files DET already wrote with `hadoop`.
REST mode commits through the Lakehouse catalog at load time so Spark / BQ / Trino
can share the same metastore.

### AWS Glue (classic)

```bash
export DET_LAKE_MODE=cloud
export DET_LAKE_PATH=s3://YOUR_BUCKET/det-lake
export DET_ICEBERG_CATALOG=glue
# optional: export DET_ICEBERG_GLUE_ID=123456789012
# AWS credential chain + AWS_REGION as for S3 lake I/O
```

### AWS Glue Iceberg REST

Same as any REST catalog — use `DET_ICEBERG_CATALOG=rest` with the Glue REST
endpoint and SigV4 via the AWS credential chain (see current AWS Iceberg REST
docs for `uri` / `warehouse` shape). Not `DET_ICEBERG_CATALOG=glue`.

## Switching modes

| From → to | Data rewrite? | Notes |
| --- | --- | --- |
| `rest` (GCP) → `hadoop` + BigLake register | No if files stay under `{lake}/bronze/…` | Flip env to `hadoop`, run `det biglake-register` |
| `hadoop` → `rest` / `glue` | No if locations match | `det iceberg-register --apply` after flipping catalog env |
| `glue` → Glue REST | No | Flip to `rest` + Glue REST URI/warehouse; confirm same catalog id |

Do not run two write catalogs against the same tables without a clear owner —
metadata will diverge.

## `det check`

- `iceberg_rest_uri_missing` — `CATALOG=rest` without `DET_ICEBERG_REST_URI`
- `iceberg_glue_requires_s3` — `CATALOG=glue` with a non-`s3://` lake
- `lake_cloud_experimental` — cloud lakes still warn; copy mentions whether
  `hadoop` or a managed catalog is selected

## Related

- [lake-layout.md](lake-layout.md) — hive paths; catalog swap does not bump layout
- [gcp-biglake.md](gcp-biglake.md) — GCS + BigQuery external registration
- [publication-contract.md](publication-contract.md) — replace-by-run bronze
- [`docker/polaris-minio/README.md`](../docker/polaris-minio/README.md) — compose details
