# Library getting started (embedders)

DET as a library: install the package under **your** project, write a source
plugin, and call `PipelineRunner`. This repo’s CLI / dbt / Airflow / MCP path
is separate — see the [operator README](../README.md).

| Doc | Use |
| --- | --- |
| This page | First hour for embedders |
| [api.md](api.md) | SemVer surface (`det.__all__`), concurrency, errors |
| [lake-layout.md](lake-layout.md) | Hive paths, `__*` meta, `lake_layout` |
| [gcp-biglake.md](gcp-biglake.md) | Architecture C: `gs://` Iceberg bronze + BigLake + dbt-BQ |

## 1. Install

Python 3.12–3.13. From PyPI (distribution name **`det-elt`**; import `det`):

```bash
uv pip install "det-elt[iceberg]"
# or pip:
pip install "det-elt[iceberg]"
```

From git (latest main):

```bash
uv pip install "det-elt[iceberg] @ git+https://github.com/codekid/det.git"
# or editable checkout of this repo:
uv pip install -e ".[iceberg]"
```

`iceberg` is the recommended lake bronze. Add extras as needed:

| Extra | When |
| --- | --- |
| `examples` | Import in-tree demos (`example_api`, NOAA HTTP helpers that pull `dlt`) |
| `duckdb` / `postgres` / `s3` / `gcs` | Those destinations / object lakes |
| `dbt` / `bigquery` | Operator dbt (DuckDB local; dbt-bigquery for BigLake) |
| `scaffold` | Jinja scaffolds (`det init-pipeline` templates) |

Base install already includes the runtime, CLI, and `det.testing`.

## 2. Project layout

```text
{project_root}/
  configs/pipelines/<provider>/<source>.yaml
  schemas/<provider>/<source>/<source>.schema.yaml
  sources/<provider>/<source>.py    # optional project-local plugins
```

Pass `project_root` explicitly (or set `DET_PROJECT_ROOT`). Prefer
`DetSettings.from_env(project_root=…)` then `PipelineRunner(settings=settings)`.
Relative lake paths are fine for demos; production should use an absolute path
or `s3://` / `gs://` with `DET_LAKE_MODE=cloud`.

## 3. Plugin + registration

Day-1: project-local file (no package publish):

```bash
det init-source -n myco.feed --project-root .
# writes sources/myco/feed.py + pipeline YAML + schema stub
```

Discovery order (ids must not collide):

1. In-tree examples shipped with DET (`det.sources.*`) — needs `det[examples]` to
   *import* HTTP-heavy demos; discovery of ids still sees them when installed
2. `{project_root}/sources/<provider>/<source>.py` (`cls.name` == `provider.source`)
3. Entry points `det.sources` / `det.mappers` for installable shared plugins

Implement `defaults`, `extract_to_raw`, `records_from_raw` (see
`det.sources.base.SourcePlugin`). Optional migrate mapper: `@mapper("…")` in the
same module. HTTP: `det.sources.http_json` / `http` helpers are fine; **never**
`dlt.pipeline` / `pipeline.run` for landing (DET refuse-closes on `_dlt_*`).

## 4. Pipeline YAML + schema + secrets

Canonical id is `provider.source`. YAML holds secret **names** only:

```yaml
name: myco.feed
source:
  type: myco.feed
  overrides:
    auth_env: MYCO_API_TOKEN   # name; value from env / DetSettings.resolve_secret
schema: schemas/myco/feed/feed.schema.yaml
destination:
  type: iceberg                # or filesystem for JSONL smoke
  path: ./data/lake            # demo; prefer absolute / s3:// in prod
wire_version: 1
```

Schema is a JSON Schema file on disk (v1). Lake object-store credentials stay
AWS_/GCP env conventions — not on `DetSettings`.

## 5. `DetSettings` + `PipelineRunner`

```python
from det import DetSettings, PipelineRunner, list_sources, DetError, configure_logging

configure_logging()  # process edge; or BYO structlog + drop_secrets / scrub_secrets

settings = DetSettings.from_env(project_root=".")
assert "myco.feed" in list_sources(project_root=settings.project_root)

try:
    PipelineRunner(settings=settings).run("myco.feed", interval_start="2026-01-01")
except DetError:
    raise
```

Interval: start inclusive, end exclusive (default start + 1 day). Canonical
pipeline ids work on the runner (same as the CLI).

Custom secrets (no process-env mutation):

```python
settings = DetSettings.from_env(
    project_root=".",
    resolve_secret=lambda name: {"MYCO_API_TOKEN": "…"}).get(name),
)
```

Each `DetSettings` instance has its own secret cache (see [concurrency](api.md#concurrency)).

## 6. `det.testing` smoke

```python
from det.testing import TestProject, run_extract_load, assert_raw_contract

def test_feed(tmp_path):
    proj = TestProject(tmp_path)
    # register plugin or write sources/… then:
    proj.write_minimal_pipeline("myco.feed", fixture_rows=[{"id": 1}])
    result = run_extract_load(proj, "myco.feed", interval_start="2026-08-06")
    assert result.rows == 1
    assert_raw_contract(result.raw_dir)
```

Optional: `pytest_plugins = ["det.testing.pytest"]` for autouse registry isolation.
More helpers: [api.md](api.md) stable submodule `det.testing`.

## 7. Day-2: migrate, check, prune

Library callers are trusted — no approval files on
`PipelineRunner` / `BronzeMigrator` / `BronzePruner.apply`. Operators/agents use
`det approve` for CLI gating only.

```python
from det import BronzeMigrator, BronzePruner, check_project, has_errors

assert not has_errors(check_project(settings.project_root, pipeline="myco.feed"))

BronzeMigrator(settings=settings).migrate(
    pipeline="myco.feed",
    to_bronze="myco.feed_v2",
    schema_path="schemas/myco/feed/feed.schema.yaml",
    mapper_name="identity",
    interval_start="2026-01-01",
    dry_run=True,  # then dry_run=False when ready
)

pruner = BronzePruner(settings=settings)
plan = pruner.plan("myco.feed", interval_start="2026-01-01", keep=3)
pruner.apply("myco.feed", plan)  # bronze-only; never deletes raw
```

Concurrency (leases, processes vs threads): [api.md § Concurrency](api.md#concurrency).

## 8. Out of scope for the library path

| Product piece | Where to look |
| --- | --- |
| dbt silver/gold, `det dbt` | [README](../README.md), [det-dbt skill](../.cursor/skills/det-dbt/SKILL.md) |
| MCP inspect / dry-run | [AGENTS.md](../AGENTS.md), [README MCP](../README.md#mcp-cursor) |
| Airflow Compose DAGs | [README Airflow](../README.md), [det-airflow skill](../.cursor/skills/det-airflow/SKILL.md) |
| Approvals / `det approve` | CLI/agent only — not on embedder runners |
| Cube metrics | Operator product |

Library packaging epic: [#24](https://github.com/codekid/det/issues/24). PyPI
distribution: **`det-elt`** (MIT).
