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
| `PipelineConfig`, `load_pipeline_config` | Pipeline YAML model |
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

Registration for production: entry points or (planned) project-local `sources/`.
In-process `register_source` is for tests via **`det.testing`** (planned,
[#30](https://github.com/codekid/det/issues/30)), not top-level `det`.

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

### Logging

| Name | Role |
| --- | --- |
| `configure_logging`, `get_logger` | structlog setup (call at process edge; not on import) |

Redaction helpers remain on `det.logging` for advanced BYO structlog chains.

---

## Stable submodules (not on top-level `det`)

These have their own `__all__` and are SemVer-stable for plugin authors:

| Module | Purpose |
| --- | --- |
| `det.sources.http_json` | JSON page artifacts, bearer token helper |
| `det.sources.http` | Retried GET / file download |

---

## Not public API

| Area | Notes |
| --- | --- |
| CLI (`det.cli`) | Operator front-door |
| MCP (`det.mcp.*`) | Agent inspect / dry-run |
| Scaffold / `dbt_runner` | Product integrations |
| Approvals | CLI / agent only — library callers are trusted |
| `record_attempt` / receipt writes | Runner-internal |
| `runs-materialize` | Ops product path |
| In-tree example sources | Content demos, not SemVer surface |
| `get_source` / `get_mapper` | Advanced; prefer discovery lists + runners |

---

## Planned (linked issues)

| Feature | Issue |
| --- | --- |
| `DetError` tree + plugin wrap | [#28](https://github.com/codekid/det/issues/28) |
| `det.testing` | [#30](https://github.com/codekid/det/issues/30) |
| Project-local `sources/` + `init-source` | [#29](https://github.com/codekid/det/issues/29) |
| Operator vs library getting started | [#33](https://github.com/codekid/det/issues/33) |

Epic: [#24](https://github.com/codekid/det/issues/24).
