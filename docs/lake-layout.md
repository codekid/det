# Lake layout compatibility

DET separates **three version concepts**. Only one of them is the hive/SQL skeleton
contract documented here.

| Field | Scope | Bumps when | Read from |
| --- | --- | --- | --- |
| **`lake_layout`** | Path keys, partition hive, SQL naming rules, sibling prefixes | Hive key renames, partition encoding changes, SQL schema/table rules change | `meta/manifest.json`, run receipt JSON |
| **`wire_version`** | Dataset era for one pipeline (`{name}_vN`) | True wire/parse breaks; rebuild raw with `det migrate` | Pipeline YAML, manifest, receipt |
| **`receipt_version`** | JSON shape under `{lake}/runs/` | Receipt schema breaking changes | Run receipt JSON only |

Package semver (`det` `0.8.0` in `pyproject.toml`) is **not** lake layout. A DET
release can ship without changing `LAKE_LAYOUT`.

Code constant: `det.runtime.layout.LAKE_LAYOUT` (currently **2**). Writers stamp
the active layout on new extract manifests and run receipts (**2** by default
when roots are split or derived from `DET_LAKE_PATH`; **1** only when unified
opt-in is set). Readers treat a missing or invalid value as **1**
(`lake_layout_of`).

**Reader boundary:** load refuses `manifest.lake_layout` greater than this
install’s `LAKE_LAYOUT` (`assert_manifest_lake_layout`). A DET with
`LAKE_LAYOUT=1` therefore rejects manifests stamped `2`. **Upgrade** readers
(and writers) to ≥0.8.0 before relying on layout-2 stamps; **rollback** a
fleet that still has layout-1-only installs by keeping writers on layout 1
(`DET_LAKE_LAYOUT=1` / `--lake-layout 1`) until every reader supports 2. Do not
mix layout-2 writers with layout-1-only loaders against the same lake.

---

## Layout 2 — default (split / derived)

Layout **2** is the default. Writers stamp `lake_layout: 2`.

### Lake roots

- **Derived (default):** only `DET_LAKE_PATH` / `--lake-path` / `DetSettings.lake_path`
  → raw = `{path}/raw`, bronze = `{path}/bronze`, ops = `{path}` (so `runs/`,
  `locks/`, `ops/` stay top-level siblings). On-disk paths match the historical
  unified tree; resolution and stamps are layout 2 (flattened dataset dirs under
  each layer root; `destination.path` ignored).
- **Explicit split:** set all three opaque URIs — `DET_LAKE_PATH_RAW`,
  `DET_LAKE_PATH_BRONZE`, `DET_LAKE_PATH_OPS` (or `DetSettings.lake_path_*` /
  `--lake-path-*`). Embedders choose arbitrary bucket names; DET never assigns
  them.
- `DET_LAKE_MODE` (`local`|`cloud`, default `local`) only guards URI shape; it
  does **not** bump `lake_layout`.
- Object storage uses the same keys under `s3://…` or `gs://…` (no
  `destination.type: s3`). Custom endpoints (`AWS_ENDPOINT_URL`, e.g. MinIO) are
  mapped into Iceberg FileIO properties (`s3.endpoint`, path-style) as well as
  s3fs.

```text
{DET_LAKE_PATH}/raw/{provider}/{source}_vN/…          # derived default
{DET_LAKE_PATH}/bronze/{provider}/{source}_vN/…
{DET_LAKE_PATH}/runs/…  locks/…  ops/…

# or explicit:
{DET_LAKE_PATH_RAW}/{provider}/{source}_vN/…
{DET_LAKE_PATH_BRONZE}/{provider}/{source}_vN/…
{DET_LAKE_PATH_OPS}/runs/…  locks/…  ops/…
```

---

## Layout 1 — unified opt-in

Layout **1** is an explicit opt-in: `DET_LAKE_LAYOUT=1` or `--lake-layout 1`
(cannot combine with any of `DET_LAKE_PATH_{RAW,BRONZE,OPS}`). Single root via
`DET_LAKE_PATH` / rare `destination.path`. Dataset dirs use medallion prefixes
(`raw/`, `bronze/`) under that root. Names and paths remain compatibility
promises for unified lakes.

### Lake root (layout 1)

- One URI; `raw/` + `bronze/` under that URI via medallion prefixes.
- `destination.path` applies only in layout 1.

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
| **`filesystem`** | Hive JSONL: `…/__extract_run_datetime=…/data.jsonl` plus commit `meta/manifest.json` (same visibility protocol as raw) |
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
| `locks/` | Interval leases `{pipeline}/{start}_{end}.json`; bronze-dataset RW `{lake}/locks/datasets/…/_lock.json` |
| `runs/dt=YYYY-MM-DD/{pipeline}/` | Extract/load attempt receipts (JSON) |
| `ops/` | Materialized receipt Iceberg table (`run_receipts`) for ops dbt |
| `ops/silver_catchup/` | Optional JSON catch-up manifest (`manifest.json`) for bronze↔silver heal; see [silver-catchup.md](silver-catchup.md) |

Additive keys in manifests or receipts, or new optional siblings documented here,
do **not** require a layout bump.

---

## What does **not** bump `lake_layout`

- Bumping **`wire_version`** → new `{name}_vN` tree; migrate from raw.
- Switching **`destination.type`** on the same pipeline (same dataset path; re-extract
  or migrate as needed).
- Iceberg catalog implementation (Hadoop on disk vs REST/Glue in prod) — table
  location under `{lake}/bronze/…` stays layout 1. See
  [iceberg-catalog.md](iceberg-catalog.md) for `DET_ICEBERG_CATALOG`.
- Iceberg **partition spec** (`destination.partition: extract_run` \| `none`) —
  create-time table property, not hive path keys. Changing it does not rename
  raw/bronze directories. Live mismatch **hard-fails** load/migrate until
  `det migrate … --recreate-iceberg` (purges the bronze table, then rewrites
  latest raw per interval in `-s`/`-e`, or `--all-raw` for every interval) or a
  manual wipe of the table location.
- New optional manifest/receipt fields.
- Bumping **`receipt_version`** only affects `{lake}/runs/` JSON consumers.

---

## What **would** bump to layout 3

Requires a new **`LAKE_LAYOUT`**, changelog entry, and an explicit migration or
“wipe lake and re-extract” policy:

- Renaming hive keys (e.g. `__interval_start_datetime` → something else).
- Changing partition value encoding.
- Changing `{medallion}_{provider}` / dbt slug rules.
- Moving `runs/` or `locks/` path schemes.

---

## Layout 2 — split roots (history)

Layout **2** was first published as opt-in (all three `DET_LAKE_PATH_*`). As of
**0.8.0** it is the **default** (derived from `DET_LAKE_PATH` when layer roots
are unset). Hive keys, SQL names, and DET meta columns are unchanged from layout
1. Cutover between distinct bucket sets is a new set of URIs + re-extract — no
in-place layout migrator.

Production load refuses manifests with `lake_layout` greater than this install’s
`LAKE_LAYOUT`. Missing `lake_layout` still means layout 1.

---

## Changelog

### Layout 2 default — published 2026-09-07 (package 0.8.0)

- Default resolution is layout **2**: `DET_LAKE_PATH` alone derives
  `{path}/raw`, `{path}/bronze`, ops=`{path}`.
- Unified layout **1** requires `DET_LAKE_LAYOUT=1` / `--lake-layout 1`.
- Operator day-2 card + profiles: [operator-runbook.md](operator-runbook.md).

### Layout 2 — published 2026-09-02

- Split lake roots: `DET_LAKE_PATH_{RAW,BRONZE,OPS}` / `DetSettings.lake_path_*`.
- Flattened dataset paths under each layer root (no `raw/` / `bronze/` segment).
- Ops siblings (`runs/`, `locks/`, `ops/`) live on the ops root only.
- Global config only (no per-pipeline lake paths in split mode).
- Load fails closed when `manifest.lake_layout > LAKE_LAYOUT`.

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
