# DET public API

Names in **`det.__all__`** follow SemVer. Anything else under `det.*` is internal
and may move in any release unless a submodule is listed below as stable.

Embedder quickstart: [getting-started-library.md](getting-started-library.md).
Lake paths and `__*` meta: [lake-layout.md](lake-layout.md).

## Product surfaces (one package)

| Surface | What | Import rule |
| --- | --- | --- |
| **Kernel** | Lake, runner, schema validate, writers, `schema_shapes`, `bronze_runs` | Must not import `det.mcp` or `det.scaffold.dbt*` |
| **Analytics adapter** | `det.scaffold` (codegen), `dbt_runner`, silver catch-up | Reads bronze contract; used by CLI/MCP |
| **Operator app** | This repo’s `configs/`, `dbt/`, `cube/`, Airflow | Not SemVer |
| **Agent OS** | `det.mcp`, skills, trajectory evals | Client of kernel + adapter |

In-tree demos (`noaa`, `example_api`, `openlibrary`) list only when
`DET_DISCOVER_EXAMPLES=1`.

---

## Versioning axes

| Axis | What it versions | Who bumps it |
| --- | --- | --- |
| **Package SemVer** | `det.__all__` and documented behavioral guarantees | DET releases |
| **`lake_layout`** | Hive keys, path skeleton, SQL naming, DET meta column names | DET (rare); see [lake-layout.md](lake-layout.md) |
| **`wire_version`** | Per-pipeline dataset era (`{name}_vN`) | Pipeline owners |
| **`receipt_version`** | JSON shape under `{ops}/runs/` | DET |

Package version is **not** lake layout. A DET release can change Python helpers
without bumping `LAKE_LAYOUT`. Bumping `wire_version` in YAML does **not** require
a DET major.

**Implemented:** refuse loads when `manifest.lake_layout` is greater than this
install’s `LAKE_LAYOUT` (`DetContractError`). Missing `lake_layout` means
layout **2**. Split roots: `DetSettings.lake_path_raw` / `_bronze` / `_ops`
or `DET_LAKE_PATH_*` — see [lake-layout.md](lake-layout.md). Cutover is new
buckets (or prefixes) + re-extract; there is no layout migrator.

---

## Top-level `det` (`__all__`)

### Identity

| Name | Role |
| --- | --- |
| `__version__` | Package version string |
| `LAKE_LAYOUT` | Max on-disk contract integer this install supports |
| `LakeRoots` | Resolved layout 2 raw / bronze / ops roots |
| `resolve_lake_roots` | Process-wide lake root resolver |

### Runners and results

| Name | Role |
| --- | --- |
| `PipelineRunner` | extract / load / run |
| `ExtractResult`, `RunResult` | Runner return types |
| `BronzeMigrator`, `MigrateResult`, `MigratePlan`, `PartitionPlan` | Rebuild bronze from raw |
| `BronzePruner`, `PrunePlan`, `BronzeRunRef` | Bronze-only retention |

### Config and plugins

| Name | Role |
| --- | --- |
| `DetSettings` | Frozen embedder settings (`from_env`, lake, locks, secrets callable) |
| `PipelineConfig`, `load_pipeline`, `load_pipeline_config` | Pipeline YAML model; `load_pipeline` accepts canonical id / path / config |
| `IcebergMaintainPlan`, `iter_iceberg_maintain_plans` | Pure Iceberg maintain/reconcile plans for external Airflow/Spark runners |
| `Interval`, `SourceRow`, `SourcePlugin` | Source protocol |
| `mapper`, `merge_source_config`, `identity_mapper` | Config merge and migrate mappers |

`DetSettings.from_env(project_root=…)` reads `DET_LAKE_*`, `DET_LOCK*`, and
`DET_SECRETS_*`. Pass `settings=` into `PipelineRunner` / migrator / pruner.
CLI builds settings then applies `--lake-path` / `--lock-ttl-sec`. Config YAML
still holds secret **names** only; values come from `settings.resolve_secret`
(default: env → optional `.env.secrets`). Object-store lake credentials stay
AWS_/GCP env conventions — not on `DetSettings`.

### dlt boundary

DET never lands bronze via `dlt.pipeline`. HTTP helpers (`RESTClient`, etc.) are
fine inside `extract_to_raw`. Bronze writers are `ingestion.library: det` or
`thin` only (`dlt` as a writer alias was removed in 0.5.0). If raw pages, bronze
rows, or lake prefixes look dlt-managed (`_dlt_*` keys, `_dlt_loads` /
`_dlt_pipeline_state` / `_dlt_version` paths), extract/load raise
`DetContractError` and `det check` emits `dlt_state_on_lake`. Shared helpers live
in `det.runtime.dlt_hygiene` (also used by `det.testing.assert_no_dlt_artifacts`).

### Discovery

| Name | Role |
| --- | --- |
| `list_sources` | Discovered source plugin ids |
| `list_mappers`, `describe_mappers` | Migrate mapper ids (+ docstring summary) |

Registration for production: entry points or project-local `sources/`
(`det init-source`). See [getting-started-library.md](getting-started-library.md).

### Lake, check, receipts, locks

| Name | Role |
| --- | --- |
| `open_lake`, `LakeRef` | Lake I/O |
| `configure_duckdb_s3` | DuckDB httpfs/S3 secret setup (object lakes) |
| `check_project`, `check_pipeline_config`, `Finding` | Structure / config checks |
| `has_errors`, `has_warnings`, `findings_payload` | Check helpers |
| `list_receipts`, `summarize_receipts` | Read run receipts (observability) |
| `Lease`, `LeaseHeldError`, `LeaseFencedError` | Lease types (acquire conflict vs lost-lease fence) |
| `inspect_lease`, `release_lock` | Inspect / force-release lake lock files |

`PipelineRunner`, `BronzeMigrator`, and `BronzePruner` accept a canonical pipeline
id (`provider.source`), a YAML path, or a `PipelineConfig`. Prefer
`load_pipeline(ref, project_root=…)` when you need the config object yourself.

**Prune is bronze-only** — it never deletes raw. Prefer `plan` then `apply`.

**Approvals are CLI/agent-only.** Embedders call `migrate(dry_run=False)`,
`BronzePruner.apply`, and `PipelineRunner` directly; there is no approval file
on the library path. Operators/agents use `det approve` + `--approval` (or
`DET_REQUIRE_APPROVAL=1`) when gating writes from CLI / Airflow prune-apply /
backfill windows. `record_attempt` and `runs-materialize` stay internal / ops
product — not on `__all__`.

**Approvals are an audit and intent-binding mechanism, not an authorization
boundary.** The same shell that runs `det extract --approval` can also run
`det approve`, so the gate is not a privilege boundary. What it does guarantee:

- Every flag that changes *what* or *where* is written is bound into
  `plan_digest`, and `check_approval` requires exact `argv` equality — so the
  record accurately describes the command that runs.
- A flag the argv builders do not encode is **rejected** under an approval
  (`approval_unbound_flag`) rather than silently escaping the digest.
- Claiming is atomic (`claim_approval`), so two concurrent runs cannot both
  write against one approval.

**Store:** default is the lake ops root `{ops}/approvals/apr_….json` (shared by
local CLI and managed Airflow). Opt-in Postgres with `DET_APPROVAL_BACKEND=postgres`
and `DET_APPROVAL_PG_DSN` (schema/table default `det_approval.approvals`). Legacy
`{project_root}/.det/approvals` is **read-only fallback** for one release; new
creates never write there.

A crash between claim and consume leaves the record `claimed`, and a claim never
ages out (TTL gates *claiming*, not finishing), so it is excluded from the default
listing. Use `list_approval_records(root, statuses=("claimed",))` — or
`det list-approvals --status claimed` — to find it. CLI claims refresh an advisory
`heartbeat_at` while the write runs; `describe_approval` / list expose
`heartbeat_status` (`fresh` / `stale` / `unknown`) for triage only — never auto-release.
Recover with a fresh approval (preferred), or `release_approval`
(`det approval-release <id> --force`) after the worker is confirmed dead.
`approval-release` warns when the heartbeat still looks fresh.

Releasing is deliberately **not** a TTL bypass — the record returns to `unused`,
so one that expired while claimed reads as `expired` — and deliberately **not**
automatic, since a TTL-driven release would silently reopen the double-write
window. `det approval-release` is also not itself approval-gated: a recovery path
must not depend on the mechanism that is stuck.

Real authorization requires moving `det approve` out-of-band — a separate user,
machine, or credential from the one running the write.

Lock paths live under the lake; build them with
`det.runtime.lease.lock_path` (advanced) then `inspect_lease` / `release_lock`.


### Logging

| Name | Role |
| --- | --- |
| `configure_logging`, `get_logger` | structlog setup (**call at process edge**; not on import) |
| `drop_secrets`, `scrub_secrets`, `scrub_rendered` | Redaction processors / helpers for BYO structlog |

The library never calls `configure_logging` on import. Embedders either call it
once at startup or configure structlog themselves and still use the redaction
helpers. The CLI configures logging in its Typer callback as today.

### Errors

| Name | Role |
| --- | --- |
| `DetError` | Base for operational failures (`except DetError`) |
| `DetConfigError` | Settings / YAML / secret-store config |
| `DetPluginError` | Source / mapper / ingestion plugin failure (`__cause__` preserved) |
| `DetContractError` | Schema / coerce failures |
| `DetConflictError` | Lease held, committed raw already exists, … |
| `DetNotFoundError` | Missing pipeline, raw partition, plugin id |
| `LeaseHeldError` | Subclass of `DetConflictError` (another writer holds the lease on acquire) |
| `LeaseFencedError` | Subclass of `DetConflictError` (this worker lost the lease before publish) |

`SecretError` / `PluginLoadError` / `SchemaValidationError` fold into this tree
internally; prefer catching the public `Det*` types.

---

## Concurrency

Raw publish (manifest-as-commit), lease limits, and extract/load idempotency are
normative in [publication-contract.md](publication-contract.md).

### Supported (v1)

| Pattern | Notes |
| --- | --- |
| **Many processes**, one lake | Leases serialize the same `pipeline` + interval; other pipelines may run in parallel |
| **One process, sequential** runs | Call `PipelineRunner` / migrator / pruner one after another |

Catch `LeaseHeldError` (or `DetConflictError`) when another writer holds the lock
on acquire; retry or wait — do not disable locks in production (`DET_LOCK=0` is
for tests). Catch `LeaseFencedError` when a mid-run pre-publish fence fails after
a steal or force-release (`assert_lease_held`). Soft refresh is best-effort only.
Default backend is lake files with strong CAS on s3/gs; set
`DET_LOCK_BACKEND=postgres` (or pipeline `lease.backend`) for an external store.
Long extracts: raise TTL via `--lock-ttl-sec` / `DET_LOCK_TTL_SEC` / DagRun
`lock_ttl_sec`. Exclusive recreate waits for shared bronze publishers to finish
(`DET_DATASET_LOCK_WAIT_SEC`, default 3600).

### Unsupported (v1)

- Overlapping **threads** or in-process parallel runners on the same settings /
  registries — use **processes** instead.
- Async-native `PipelineRunner`.

Plugin registries and the process-wide secret scrub set are process-scoped.
`det.testing.isolated_registries` / pytest fixtures isolate registries in tests.

### Secrets naming and caches

| Situation | Approach |
| --- | --- |
| Same source, many tables | Shared `auth_env` / provider secret name — preferred |
| Different tenants / envs | Distinct secret names, a tenant-aware `resolve_secret`, or separate processes |

Each `DetSettings` instance owns its **own** secret-lookup cache. Two settings
objects with different resolvers do not share cached values. Call
`settings.clear_secret_cache()` after forced rotation. The module-level
`det.runtime.secrets` cache is separate (default env/file path without settings).

---

## Stable submodules (not on top-level `det`)

These have their own `__all__` and are SemVer-stable for plugin authors:

| Module | Purpose |
| --- | --- |
| `det.sources.http_json` | JSON page artifacts, bearer token helper |
| `det.sources.http` | Retried GET / file download |
| `det.testing` | Plugin-author test helpers (`TestProject`, `run_extract_load`, …) |

Optional pytest fixtures: `pytest_plugins = ["det.testing.pytest"]` (requires
pytest; not imported by the core helpers).

---

## Not public API

| Area | Notes |
| --- | --- |
| CLI (`det.cli`) | Operator front-door |
| MCP (`det.mcp.*`) | Agent inspect / dry-run |
| Scaffold / `dbt_runner` / catch-up | Analytics adapter (not SemVer); CLI/MCP call `check_project_with_dbt` for scaffold drift |
| Approvals | CLI / agent only — library callers are trusted (`PipelineRunner` / migrator / pruner apply have no approval hook). Audit / intent-binding, not authorization |
| `record_attempt` / receipt writes | Runner-internal |
| `runs-materialize` | Ops product path |
| `read_lock` / `force_release_lock` | Prefer `inspect_lease` / `release_lock` |
| In-tree example sources | Content demos behind `DET_DISCOVER_EXAMPLES`; not SemVer surface |
| `get_source` / `get_mapper` | Advanced; prefer discovery lists + runners |

---

## Planned (linked issues)

| Feature | Issue |
| --- | --- |
| Thin `det.http_json` recipe + eject (optional) | [#35](https://github.com/codekid/det/issues/35) |

Epic: [#24](https://github.com/codekid/det/issues/24).
