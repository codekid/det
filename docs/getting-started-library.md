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
