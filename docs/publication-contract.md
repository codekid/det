# Publication, storage, and idempotency contract

This document is normative for DET v1 lake behavior. Path and naming rules live in
[lake-layout.md](lake-layout.md). Concurrency API notes live in [api.md](api.md).
If code and this doc disagree, fix the code or bump a documented exception here.

DET’s invariants:

1. Raw is invisible until `meta/manifest.json` is a valid commit object.
2. Re-extract of a committed prefix conflicts; a new attempt uses a new
   `__extract_run_datetime=` sibling (or an explicit new extract-run id).
3. Load reads only committed raw and writes bronze with replace-by-run semantics.
4. A lease serializes writers for the same `pipeline` + interval identity
   (equality by default; optional Postgres `overlap` mode for intersecting
   intervals).
5. DET owns wire → typed bronze; dbt owns analytical meaning.

---

## 1. Raw publication protocol

Logical phases (not separate on-disk state machines):

```text
IDENTIFIED   extract_run_datetime chosen → hive prefix known
WRITING      bytes under …/data/ (and only there for payloads)
STAGED       data may exist; load/migrate/list-of-committed ignore the prefix
COMMITTED    meta/manifest.json published → prefix is visible
```

### Visibility

- **Committed** iff `meta/manifest.json` exists, is readable JSON, and decodes to
  a `dict` (`is_committed_raw_dir`).
- A sibling `manifest.json.tmp` is **never** a commit.
- Data files without a commit are **orphans**: allowed on disk/object store, not
  inputs to load, migrate, or “latest extract-run” selection.

### Extract rules

| Situation | Behavior |
| --- | --- |
| Prefix already **COMMITTED** | `DetConflictError` — do not overwrite; use a new extract-run id |
| Prefix exists, **not** committed | Delete the prefix, then write (retry after crash) |
| Failure before commit | Delete the incomplete prefix when extract handles the error (`LeaseFencedError` preserves the prefix) |
| Success | Write manifest last — that publish is the commit |

### Manifest commit mechanics

| Lake backend | Commit |
| --- | --- |
| Local filesystem | Write `manifest.json.tmp`, fsync when possible, `os.replace` → `manifest.json` |
| Object store (`s3://`, `gs://`, …) | Single PUT/write of `meta/manifest.json` |

### After load

`validation` on the raw manifest is a **success receipt** stamped after bronze
write succeeds. Absence of `validation` does **not** mean raw is invalid or
uncommitted; it means this extract-run has not yet recorded a successful load
under a schema hash.

---

## 2. Storage capabilities (honest limits)

| Capability | Local | `memory://` | Object store (fsspec) |
| --- | --- | --- | --- |
| Exclusive create (`create_exclusive`) | `O_CREAT\|O_EXCL` | atomic `setdefault` | Conditional put (`If-None-Match: *` / generation 0); **fail closed** if preconditions unavailable |
| Manifest publish | rename replace | key write | single object write |
| List consistency | strong | strong | **not assumed strong** — do not rely on list-after-write for commit |
| Own-key read-after-write | yes | yes | assumed for the manifest key after successful write |
| Prefix delete | `rmtree` | yes | list + delete of that prefix |

### Leases

- Lock identity: `(pipeline, interval_start, interval_end)` — **equality** by
  default (`DET_LOCK_MODE=exact`). Postgres may use `overlap` to block
  intersecting intervals.
- **Lake backend (default):** object `{lake}/locks/{pipeline}/{start}_{end}.json`.
  Acquire / steal / refresh / release use strong conditional writes on
  `s3://` and `gs://` (If-None-Match / If-Match or generation match). Local and
  `memory://` use exclusive create + versioned CAS. Soft exists+`wb` is **not**
  used for cloud lease mutates.
- **Postgres backend (opt-in):** `DET_LOCK_BACKEND=postgres` or pipeline
  `lease.backend: postgres`. Rows in `{DET_LOCK_PG_SCHEMA}.{DET_LOCK_PG_TABLE}`
  (defaults `det_lease.leases`). DSN from `DET_LOCK_PG_DSN` (name overridable via
  `DET_LOCK_PG_DSN_ENV`). Never auto-reuses bronze `destination.connection_env`.
- Live lock → `LeaseHeldError` on acquire. Expired lock may be **stolen** via
  CAS (owner presumed dead). TTL: `DET_LOCK_TTL_SEC` / `--lock-ttl-sec` /
  Airflow conf `lock_ttl_sec` (default 7200).
- Soft `refresh` extends TTL when the token still matches; mismatch is a
  no-op (heartbeat only).
- **Pre-publish fence:** extract (before raw manifest), load/migrate (before
  bronze write), and leased prune call `assert_lease_held`. Lost token /
  version / expired TTL (even with matching token) → `LeaseFencedError`
  (receipt `lease_fenced`). This authorizes the
  side effect at the publish boundary; it is not bound into the Iceberg
  transaction itself, so a residual assert→commit race remains.
- `DET_LOCK=0` disables leases (tests / explicit local break-glass only).

### Bronze-dataset reader/writer lock

- Lock identity: bronze **dataset id** (`example_api.events_v1`) at
  `{lake}/locks/datasets/…/_lock.json` (lake CAS) or Postgres
  `det_lease.dataset_locks` (+ shared holder rows).
- **Shared** (many holders): brief hold around bronze publish (`load`,
  non-recreate `migrate`, leased `prune --apply`). Parallel loads on different
  intervals remain concurrent.
- **Exclusive** (one holder): `migrate --recreate-iceberg` from before
  `purge_iceberg_table` through full rebuild. Blocks new shared acquires;
  waits for active shared holders to drain.
- Lock ordering: acquire interval `pipeline_lease` first, then dataset shared;
  release reverse. Recreate wraps the job in exclusive; interval leases nest
  inside.
- Fence: `assert_dataset_lock_held` before purge and bronze write (same
  `LeaseFencedError` / `lease_fenced` receipt as interval leases).
- Ops: pause pipeline during recreate; force-clear stuck exclusive with
  `det lock-release --pipeline {pipeline} --dataset-id … --force` after confirming the worker is
  dead. `DET_DATASET_LOCK_WAIT_SEC` caps exclusive wait for shared drain
  (default 3600; `0` = fail fast).

**Production note:** lake leases on s3/gs are strong when the store honors
preconditions (fail closed otherwise). Prefer one writer per pipeline+interval
(e.g. Airflow pool for capacity) and treat the lease as the identity mutex
plus pre-publish fence. Do not `--force` release while a worker may still be
alive. Postgres is the portable strong option when you already run a database.

---

## 3. Idempotency and conflicts

| Operation | Same identity again | After crash mid-op |
| --- | --- | --- |
| **extract** committed `(pipeline, interval, extract_run)` | Conflict | N/A — already durable |
| **extract** incomplete same prefix | Cleanup + rewrite | Retry extract (same or new extract_run) |
| **extract** new extract_run, same interval | OK (sibling) | OK |
| **load** same extract_run | Replace-by-run (delete rows/files for that run identity, then write) | Re-load same extract_run |
| **load** without committed raw | `DetNotFoundError` | — |
| **run** | One lease; extract then load share nested lease | Mid-extract → no commit; mid-load → raw committed, re-load |
| **Concurrent** same pipeline+interval | Serialized by lease (when strong); zombie after steal fenced before publish | Loser: `LeaseHeldError`; fenced: `LeaseFencedError` |

Bronze **replace-by-run** is keyed by DET run-identity meta columns (interval +
extract_run), not by “append forever.” Reloading the same extract-run is
intended to converge, not to duplicate.

Attempt **receipts** under `{lake}/runs/` are observability (`ok` / `error`).
They are **not** the raw commit marker.

---

## 4. Failure catalog (operator view)

| Failure | Lake state | What to do |
| --- | --- | --- |
| Crash during `data/` writes | Orphan prefix, not committed | Re-extract (same prefix cleans; or new extract_run) |
| Crash before manifest publish | Same | Same |
| Crash after manifest | **COMMITTED** | `det load` (or complete via load half of run) |
| Crash during bronze write | Raw committed; bronze may be partial | `det load` same extract_run (replace-by-run) |
| Crash after bronze, before `validation` stamp | Bronze may be complete; no validation block | Re-load to stamp, or treat validation as optional proof |
| Lease holder dead | Lock until TTL; then steal | Wait, or `det lock-release … --force` after confirming the worker is gone |
| Corrupt manifest JSON | Treated as not committed | Fix or delete prefix; re-extract |

Orphan incomplete prefixes are cleaned when extract retries **that** prefix.
DET does not promise a background sweeper of all orphans in v1.

**Tests:** failure-injection coverage for this catalog lives in
`tests/contract/test_publication_failures.py`. Overlapping rows are covered by
`tests/unit/test_atomic_extract.py` (crash during `data/` / incomplete retry) and
`tests/unit/test_lease.py` (`test_expired_steal`). Commit visibility asserts:
`tests/contract/test_publication.py`.

---

## 5. Out of scope (v1 of this contract)

- Rich run state machine (`EXTRACTING` / `LOADING` / …) beyond receipts
- Resolved `config_sha256` artifact per attempt
- Strong cross-region list consistency guarantees
