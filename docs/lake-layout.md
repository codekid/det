# Lake layout compatibility

DET separates **three version concepts**. Only one of them is the hive/SQL skeleton
contract documented here.

| Field | Scope | Bumps when | Read from |
| --- | --- | --- | --- |
| **`lake_layout`** | Path keys, partition hive, SQL naming rules, sibling prefixes | Hive key renames, partition encoding changes, SQL schema/table rules change | `meta/manifest.json`, run receipt JSON |
| **`wire_version`** | Dataset era for one pipeline (`{name}_vN`) | True wire/parse breaks; rebuild raw with `det migrate` | Pipeline YAML, manifest, receipt |
| **`receipt_version`** | JSON shape under `{lake}/runs/` | Receipt schema breaking changes | Run receipt JSON only |

Package semver (`det` `0.1.0` in `pyproject.toml`) is **not** lake layout. A DET
release can ship without changing `LAKE_LAYOUT`.

Code constant: `det.runtime.layout.LAKE_LAYOUT` (currently **1**). Writers stamp
`lake_layout` on new extract manifests and run receipts. Readers treat a missing or
invalid value as **1** (`lake_layout_of`).

---

## Layout 1 — stable contract

Layout **1** is what DET writes today. These names and paths are compatibility
promises until **`lake_layout: 2`** is published with a changelog entry below.

### Lake root

- Single root: `DET_LAKE_PATH` / `--lake-path` / rare `destination.path`.
- `DET_LAKE_MODE` (`local`|`cloud`, default `local`) only guards URI shape; it does
  **not** bump `lake_layout`. Still one root with `raw/` + `bronze/` under that URI.
- Object storage uses the same keys under `s3://…` or `gs://…` (no
  `destination.type: s3`). Dual raw/bronze buckets are not supported.
  Custom endpoints (`AWS_ENDPOINT_URL`, e.g. MinIO) are mapped into Iceberg
  FileIO properties (`s3.endpoint`, path-style) as well as s3fs.

### Dataset id (filesystem + Iceberg table path)

- Pipeline canonical id: `provider.source` (e.g. `noaa.storm_events`).
- Lake dataset directory / Iceberg table leaf:
  **`{name}_v{wire_version}`** (always includes `_v1`).
- Filesystem segments under `raw/` and `bronze/`:
  `{provider}/{source}` from the dotted name (e.g. `noaa/storm_events_v1`).

```text
{lake}/raw/noaa/storm_events_v1/
{lake}/bronze/noaa/storm_events_v1/     # Iceberg (default) or JSONL hive
```

Changing **`wire_version`** in pipeline YAML creates a **new sibling dataset**
(`…_v2/`). That is a dataset-era cutover, **not** a layout bump.

### Raw hive (per extract run)

Three partition levels, then payload + manifest:

```text
__interval_start_datetime=<UTC compact Z>/
  __interval_end_datetime=<UTC compact Z>/
    __extract_run_datetime=<UTC compact Z>/
      data/                    # source bytes (files or pages)
      meta/manifest.json       # extract metadata (+ validation after load)
```

- Partition values: compact UTC, no `/` or `:` in path segments
  (e.g. `20260801T000000Z`).
- Re-runs append a **sibling** `__extract_run_datetime=…` folder; they do not
  overwrite prior runs.
- Interval is half-open `[start, end)` in manifest and receipts.

### Bronze (layout 1; destination chooses format)

Same dataset id and interval hive as raw. **`destination.type`** picks the writer;
layout 1 does not change paths when switching JSONL ↔ Iceberg on the same dataset:

| `destination.type` | Layout 1 landing |
| --- | --- |
| **`iceberg`** (default lake) | Hadoop-style table at `{lake}/bronze/{provider}/{source}_vN/` (Parquet + metadata) |
| **`filesystem`** | Hive JSONL: `…/__extract_run_datetime=…/data.jsonl` |
| **`duckdb` / `postgres`** | SQL table `{medallion}_{provider}.{source}_vN` (default medallion `bronze`) |

### SQL and dbt naming (layout 1)

- SQL schema: `{medallion}_{provider}` (e.g. `bronze_noaa`).
- SQL table leaf: `{source}_v{wire_version}` (e.g. `storm_events_v1`).
- dbt model stem from pipeline **name** (unversioned): `stg_{provider}__{source}`,
  `silver_{provider}__{source}`. Bronze `_vN` is wired in `sources.yml` /
  `det_bronze_from`, not in model filenames.
- Top-level pipeline `dataset:` is **rejected**; use `wire_version` for lake/SQL
  era changes.
- **BigLake / BigQuery:** register one BQ dataset per provider (`bronze_{provider}`)
  and table `{source}_vN` over the Iceberg URI
  `{lake}/bronze/{provider}/{source}_vN/`. DET does not land a BQ bronze copy —
  see [gcp-biglake.md](gcp-biglake.md).

### DET meta columns (all bronze destinations)

Every landed row includes (names stable in layout 1):

- `__row_hash`, `__filename`, `__extract_run_datetime`, `__bronze_loaded_at`
- `__interval_start_datetime`, `__interval_end_datetime`, `__data_interval_date`

### Sibling prefixes under `{lake}/`

| Prefix | Role |
| --- | --- |
| `raw/` | Wire bytes + manifests |
| `bronze/` | Typed bronze (Iceberg or JSONL) |
| `locks/` | `(pipeline, interval)` lease files |
| `runs/dt=YYYY-MM-DD/{pipeline}/` | Extract/load attempt receipts (JSON) |
| `ops/` | Materialized receipt Iceberg table (`run_receipts`) for ops dbt |

Additive keys in manifests or receipts, or new optional siblings documented here,
do **not** require a layout bump.

---

## What does **not** bump `lake_layout`

- Bumping **`wire_version`** → new `{name}_vN` tree; migrate from raw.
- Switching **`destination.type`** on the same pipeline (same dataset path; re-extract
  or migrate as needed).
- Iceberg catalog implementation (Hadoop on disk vs REST/Glue in prod) — table
  location under `{lake}/bronze/…` stays layout 1.
- Iceberg **partition spec** (`destination.partition: extract_run` \| `none`) —
  create-time table property, not hive path keys. Changing it does not rename
  raw/bronze directories. Live mismatch **hard-fails** load/migrate until
  `det migrate … --recreate-iceberg` (purges the bronze table, then rewrites
  latest raw per interval in `-s`/`-e`, or `--all-raw` for every interval) or a
  manual wipe of the table location.
- New optional manifest/receipt fields.
- Bumping **`receipt_version`** only affects `{lake}/runs/` JSON consumers.

---

## What **would** bump to layout 2

Requires a new **`LAKE_LAYOUT`**, changelog entry, and an explicit migration or
“wipe lake and re-extract” policy:

- Renaming hive keys (e.g. `__interval_start_datetime` → something else).
- Changing partition value encoding.
- Changing `{medallion}_{provider}` / dbt slug rules.
- Moving `runs/` or `locks/` path schemes.

There is **no layout migrator** in v1. A layout break means a new lake prefix or
full re-extract.

---

## Changelog

### Layout 1 — published 2026-08-17

- First **published** layout contract (this document).
- Raw hive: three-level partitions + `data/` + `meta/manifest.json`.
- Bronze: dataset id `{name}_v{wire_version}` under `bronze/{provider}/…`.
- Default lake bronze: **Iceberg** (`destination.type: iceberg`); JSONL remains
  opt-in (`filesystem`, `thin`).
- Siblings: `locks/`, `runs/dt=…/`, `ops/run_receipts`.
- Writers stamp **`lake_layout: 1`** on manifests and run receipts; missing ⇒ 1.
- **`wire_version`** remains the dataset-era knob (`det migrate` rebuilds bronze
  from raw within an era).

*(Prior lakes without `lake_layout` in JSON are layout 1 by convention.)*

---

## Quick reference

```text
manifest.json     lake_layout, wire_version, interval_*, pipeline, …
receipt JSON      lake_layout, receipt_version, wire_version, status, duration_ms, …
pipeline YAML     wire_version (default 1) — not lake_layout
```

See also: README [Destinations](../README.md#destinations), `det-migrate` skill
(`wire_version` vs layout), `src/det/runtime/layout.py`.
