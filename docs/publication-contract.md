# Publication, storage, and idempotency contract

This document is normative for DET v1 lake behavior. Path and naming rules live in
[lake-layout.md](lake-layout.md). Concurrency API notes live in [api.md](api.md).
If code and this doc disagree, fix the code or bump a documented exception here.

DET’s invariants:

1. Raw is invisible until `meta/manifest.json` is a valid commit object.
2. Re-extract of a committed prefix conflicts; a new attempt uses a new
   `__extract_run_datetime=` sibling (or an explicit new extract-run id).
3. Load reads only committed raw and writes bronze with replace-by-run semantics.
4. A lake lease serializes writers for the same `pipeline` + interval
   (equality only — not overlap).
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
| Failure before commit | Delete the incomplete prefix when extract handles the error |
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
| Exclusive create (`create_exclusive`) | `O_CREAT\|O_EXCL` | atomic `setdefault` | Prefer open mode `xb`; if unsupported, may fall back to existence check + `wb` (**weaker under races**) |
| Manifest publish | rename replace | key write | single object write |
| List consistency | strong | strong | **not assumed strong** — do not rely on list-after-write for commit |
| Own-key read-after-write | yes | yes | assumed for the manifest key after successful write |
| Prefix delete | `rmtree` | yes | list + delete of that prefix |

### Leases

- Lock identity: `(pipeline, interval_start, interval_end)` — **equality only**.
- Object: `{lake}/locks/{pipeline}/{start}_{end}.json`.
- Acquire via `create_exclusive`. Live lock → `LeaseHeldError`.
- Expired lock may be **stolen** by overwrite (owner presumed dead).
- `DET_LOCK=0` disables leases (tests / explicit local break-glass only).

**Production note:** on object stores where exclusive create is emulated, leases
are **best-effort**, not a ZooKeeper-class lock. Prefer one writer per
pipeline+interval (e.g. Airflow pool) and treat the lease as a safety net.

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
| **Concurrent** same pipeline+interval | Serialized by lease (when strong) | Loser: `LeaseHeldError` |

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
- Overlap locking for intersecting intervals
