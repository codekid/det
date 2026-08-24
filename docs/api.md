# DET public API

Names in **`det.__all__`** follow SemVer. Anything else under `det.*` is internal
and may move in any release unless a submodule is listed below as stable.

Embedder quickstart (when published): [getting-started-library.md](getting-started-library.md)
(issue [#33](https://github.com/codekid/det/issues/33)). Lake paths and `__*` meta:
[lake-layout.md](lake-layout.md).

---

## Versioning axes

| Axis | What it versions | Who bumps it |
| --- | --- | --- |
| **Package SemVer** | `det.__all__` and documented behavioral guarantees | DET releases |
| **`lake_layout`** | Hive keys, path skeleton, SQL naming, DET meta column names | DET (rare); see [lake-layout.md](lake-layout.md) |
| **`wire_version`** | Per-pipeline dataset era (`{name}_vN`) | Pipeline owners |
| **`receipt_version`** | JSON shape under `{lake}/runs/` | DET |

Package version is **not** lake layout. A DET release can change Python helpers
without bumping `LAKE_LAYOUT`. Bumping `wire_version` in YAML does **not** require
a DET major.

**Planned:** refuse loads when `manifest.lake_layout` is greater than this
install’s `LAKE_LAYOUT` (fail closed on newer lakes).

Layout 2 (if ever published) is a data-contract break: new lake prefix or
wipe + re-extract; there is no layout migrator in v1.

---

## Top-level `det` (`__all__`)

### Identity

| Name | Role |
| --- | --- |
| `__version__` | Package version string |
| `LAKE_LAYOUT` | Current on-disk contract integer |

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
| `Interval`, `SourceRow`, `SourcePlugin` | Source protocol |
| `mapper`, `merge_source_config`, `identity_mapper` | Config merge and migrate mappers |

`DetSettings.from_env(project_root=…)` reads `DET_LAKE_*`, `DET_LOCK*`, and
`DET_SECRETS_*`. Pass `settings=` into `PipelineRunner` / migrator / pruner.
CLI builds settings then applies `--lake-path` / `--lock-ttl-sec`. Config YAML
still holds secret **names** only; values come from `settings.resolve_secret`
(default: env → optional `.env.secrets`). Object-store lake credentials stay
AWS_/GCP env conventions — not on `DetSettings`.

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
| `Lease`, `LeaseHeldError` | Lake lease types |
| `inspect_lease`, `release_lock` | Inspect / force-release lock files |

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
| `LeaseHeldError` | Subclass of `DetConflictError` (lake lease) |

`SecretError` / `PluginLoadError` / `SchemaValidationError` fold into this tree
internally; prefer catching the public `Det*` types.

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
| Scaffold / `dbt_runner` | Product integrations |
| Approvals | CLI / agent only — library callers are trusted (`PipelineRunner` / migrator / pruner apply have no approval hook) |
| `record_attempt` / receipt writes | Runner-internal |
| `runs-materialize` | Ops product path |
| `read_lock` / `force_release_lock` | Prefer `inspect_lease` / `release_lock` |
| In-tree example sources | Content demos, not SemVer surface |
| `get_source` / `get_mapper` | Advanced; prefer discovery lists + runners |

---

## Planned (linked issues)

| Feature | Issue |
| --- | --- |
| Operator vs library getting started polish | [#33](https://github.com/codekid/det/issues/33) |

Epic: [#24](https://github.com/codekid/det/issues/24).
