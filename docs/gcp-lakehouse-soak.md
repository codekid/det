# GCP Lakehouse REST catalog soak (managed catalog P0)

Proves **DET_ICEBERG_CATALOG=rest** against Google’s Lakehouse Iceberg REST
endpoint on a real `gs://` lake — not the emulator (`-m gcs`) and not post-hoc
[`biglake-register`](gcp-biglake.md) (BigQuery external tables over Hadoop files).

```text
ADC → det extract/load (REST commits) → gs:// bronze + Lakehouse metadata
                                              ↓
                         PyIceberg catalog.load_table + scan (soak test)
```

Official references:

- [Create a catalog](https://cloud.google.com/lakehouse/docs/create-catalog)
- [Iceberg REST catalog endpoint](https://cloud.google.com/lakehouse/docs/lakehouse-iceberg-rest-catalog)

## Non-goals

- Not run in CI (real GCP billing + ADC). CI excludes `-m lakehouse`; the test
  also skips unless `DET_ICEBERG_REST_WAREHOUSE` is a `bl://` URI.
- Not a substitute for [`gcp-biglake.md`](gcp-biglake.md) architecture C.
- Does not auto-teardown — operator runs teardown after the soak.

## Prerequisites

| Principal | Role (sketch) |
| --- | --- |
| Your ADC / workload SA | Storage on lake bucket; BigLake catalog admin for create/delete |
| Lakehouse catalog | `bl://projects/PROJECT/catalogs/CATALOG_ID` warehouse |

Enable APIs and auth as in Google’s Lakehouse quickstart. Use Application Default
Credentials (`gcloud auth application-default login`) or
`GOOGLE_APPLICATION_CREDENTIALS`. Enable the Lakehouse API if needed:

```bash
gcloud services enable biglake.googleapis.com --project="${DET_GCP_PROJECT}"
```

After `gcloud biglake iceberg catalogs create`, grant the printed **BigLake
service account** `roles/storage.objectAdmin` on the lake bucket when using
`--credential-mode=vended-credentials`.

## Setup (disposable sandbox)

Pick names (example):

```bash
export DET_GCP_PROJECT=your-project
export DET_GCS_BUCKET=det-lakehouse-soak-${USER}-$(date +%Y%m%d)
export DET_LAKEHOUSE_CATALOG=det_soak_catalog
export DET_LAKE_PATH=gs://${DET_GCS_BUCKET}/det-lake
export DET_LAKE_MODE=cloud
export DET_ICEBERG_CATALOG=rest
export DET_ICEBERG_REST_URI=https://biglake.googleapis.com/iceberg/v1/restcatalog
export DET_ICEBERG_REST_WAREHOUSE=bl://projects/${DET_GCP_PROJECT}/catalogs/${DET_LAKEHOUSE_CATALOG}
```

**1. GCS bucket**

```bash
gcloud storage buckets create "gs://${DET_GCS_BUCKET}" \
  --project="${DET_GCP_PROJECT}" --location=US
```

**2. Lakehouse catalog** (multiple-bucket / `bl://`, credential vending)

```bash
gcloud biglake iceberg catalogs create "${DET_LAKEHOUSE_CATALOG}" \
  --project="${DET_GCP_PROJECT}" \
  --catalog-type=biglake \
  --default-location="gs://${DET_GCS_BUCKET}" \
  --credential-mode=vended-credentials
```

Grant your ADC identity bucket access if commits fail with 403 (see Google docs
for catalog + bucket IAM).

**3. Run soak**

```bash
uv sync --extra iceberg --extra gcs
pytest tests/integration/test_gcp_lakehouse_iceberg_soak.py -m lakehouse -v
```

Optional manual CLI (Path B if `DET_REQUIRE_APPROVAL=1`):

```bash
det extract -p example_api.events -s 2026-08-06 \
  --set 'source.overrides.fixture_records=[{"id":"e1","occurred_at":"2026-08-06T12:00:00Z","severity":"low","state":"TX","status":"1"}]'
```

## Teardown (full disposable sandbox)

Manual only — same pattern as [`gcp-biglake.md` § Teardown](gcp-biglake.md#teardown-full-disposable-sandbox).

```bash
export DET_GCP_PROJECT=your-project
export DET_GCS_BUCKET=your-bucket
export DET_LAKEHOUSE_CATALOG=det_soak_catalog
```

**1. Drop catalog tables** (catalog delete fails while namespaces exist)

```bash
# From repo root with ADC + same REST env as the soak:
uv run python - <<'PY'
from pyiceberg.catalog.rest import RestCatalog
import os
project = os.environ["DET_GCP_PROJECT"]
warehouse = os.environ["DET_ICEBERG_REST_WAREHOUSE"]
cat = RestCatalog("det", type="rest",
    uri=os.environ["DET_ICEBERG_REST_URI"],
    warehouse=warehouse,
    auth={"type": "google"},
    **{"header.x-goog-user-project": project,
       "header.X-Iceberg-Access-Delegation": "vended-credentials"})
for ns in cat.list_namespaces():
    for tbl in cat.list_tables(ns):
        cat.drop_table(tbl)
    cat.drop_namespace(ns)
PY
```

**2. Delete Lakehouse catalog**

```bash
gcloud biglake iceberg catalogs delete "${DET_LAKEHOUSE_CATALOG}" \
  --project="${DET_GCP_PROJECT}" --quiet
```

**3. Delete GCS bucket** (removes all lake bytes)

```bash
gcloud storage rm -r "gs://${DET_GCS_BUCKET}/**" || true
gcloud storage buckets delete "gs://${DET_GCS_BUCKET}" --quiet
```

If you also ran **BigLake register** or **dbt-BQ** in the same project, drop those
BQ datasets separately (see `gcp-biglake.md`).

## Related

- [iceberg-catalog.md](iceberg-catalog.md) — `DET_ICEBERG_CATALOG=rest` env
- [gcp-biglake.md](gcp-biglake.md) — Hadoop write + BQ external registration
- `tests/integration/test_gcp_lakehouse_iceberg_soak.py` — marker `lakehouse`
