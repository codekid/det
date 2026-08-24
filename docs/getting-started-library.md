# Library getting started (embedders)

DET as a library: install the package, put pipelines under **your** project root,
and call `PipelineRunner` (or the `det` CLI with `--project-root`).

Operator-oriented walkthrough for this monorepo: [../README.md](../README.md).
Public SemVer surface: [api.md](api.md).

## Layout

```text
{project_root}/
  configs/pipelines/<provider>/<source>.yaml
  schemas/<provider>/<source>/<source>.schema.yaml
  sources/<provider>/<source>.py    # optional project-local plugins
```

Pass `project_root` explicitly (or set `DET_PROJECT_ROOT`). Prefer
`DetSettings.from_env(project_root=…)` then `PipelineRunner(settings=settings)`.

## Project-local sources

Day-1 plugins do **not** need a published package or entry points:

```bash
det init-source -n myco.feed --project-root .
# writes sources/myco/feed.py + pipeline YAML + schema stub
```

Discovery order (ids must not collide):

1. In-tree examples shipped with DET (`det.sources.*`)
2. `{project_root}/sources/<provider>/<source>.py` (`cls.name` == `provider.source`)
3. Entry points `det.sources` / `det.mappers` for installable shared plugins

```python
from det import DetSettings, PipelineRunner, list_sources

settings = DetSettings.from_env(project_root=".")
assert "myco.feed" in list_sources(project_root=settings.project_root)
PipelineRunner(settings=settings).run("myco.feed", interval_start="2026-01-01")
```

Catch operational failures with `except DetError`. Call `configure_logging()` at
the process edge (or BYO structlog + `drop_secrets` / `scrub_secrets`).

Lake object-store credentials stay AWS_/GCP env conventions — not on `DetSettings`.

## Lake lifecycle (library)

These are on `det.__all__`. Approvals are **not** part of the library path —
call apply/migrate/run directly. Use `det approve` only for CLI/agent gated
writes.

```python
from det import (
    DetSettings,
    PipelineRunner,
    BronzeMigrator,
    BronzePruner,
    check_project,
    open_lake,
    list_receipts,
    inspect_lease,
    release_lock,
    has_errors,
)

settings = DetSettings.from_env(project_root=".")

# Structure check (no lake writes)
findings = check_project(settings.project_root, pipeline="myco.feed")
assert not has_errors(findings)

# Extract + load (canonical id)
PipelineRunner(settings=settings).run("myco.feed", interval_start="2026-01-01")

# Rebuild bronze after a schema/mapper change (dry-run first)
migrator = BronzeMigrator(settings=settings)
plan = migrator.migrate(
    pipeline="myco.feed",
    to_bronze="myco.feed_v2",
    schema_path="schemas/myco/feed/feed.schema.yaml",
    mapper_name="identity",
    interval_start="2026-01-01",
    dry_run=True,
)
# migrator.migrate(..., dry_run=False) when ready — no approval file

# Bronze retention only (never raw)
pruner = BronzePruner(settings=settings)
prune_plan = pruner.plan("myco.feed", interval_start="2026-01-01", keep=3)
pruner.apply("myco.feed", prune_plan)

# Receipts (read-only)
lake = open_lake(settings.lake_path or "data/lake", settings.project_root)
list_receipts(lake, pipeline="myco.feed", limit=20)
```

Locks: `inspect_lease` / `release_lock` are the public names (wrappers over
`read_lock` / `force_release_lock`). Build the lock object path with
`det.runtime.lease.lock_path` when you need to inspect a held lease.

**dlt:** helpers OK inside `extract_to_raw`; never `dlt.pipeline` for landing.
DET fail-closes on `_dlt_*` keys / state paths (see [api.md](api.md)).

## Testing plugins

`det.testing` ships in the base install (no extra). Framework-neutral helpers
plus optional pytest fixtures:

```python
from det.testing import (
    TestProject,
    run_extract_load,
    assert_raw_contract,
    assert_no_dlt_artifacts,
    register_source_for_tests,
    isolated_registries,
)

def test_acme_smoke(tmp_path):
    # project-local plugin already at sources/acme/tickets.py, or:
    with isolated_registries():
        register_source_for_tests("acme.tickets", AcmeTicketsSource)
        proj = TestProject(tmp_path)
        proj.write_minimal_pipeline(
            "acme.tickets",
            fixture_rows=[{"id": "t1", "status": "open"}],
        )
        result = run_extract_load(proj, "acme.tickets", interval_start="2026-08-06")
        assert result.rows == 1
        assert_raw_contract(result.raw_dir)
        assert_no_dlt_artifacts(result.raw_dir)
```

Unit-only (no lake): `extract_fixture` / `records_from_fixture`. Secrets without
mutating env: `secrets_map({"ACME_TOKEN": "x"})` passed to `TestProject.runner`.

Pytest autouse isolation (optional)::

    # conftest.py
    pytest_plugins = ["det.testing.pytest"]
