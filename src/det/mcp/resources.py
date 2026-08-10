"""det:// resource handlers for DET MCP."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import unquote

from det.mcp.context import project_root, resolve_under_root, schemas_dir
from det.runtime.pipelines import resolve_pipeline_ref

_README_POINTER = """\
# DET MCP resources

- `det://pipelines/{name}` — pipeline YAML (`noaa.storm_events` or nested path)
- `det://schemas/{provider}/{source}/{filename}` — nested schema YAML
- `det://schemas/{relative_path}` — same, with `/` as `%2F` in a single segment
- Tools: lake inspect + generate dry-runs + Airflow inspect + prune/dbt/scaffold dry-runs (v1)
- Sample size: `limit` / `sample_limit` (default 5, max 50)
- Generate tools never write files — review drafts, then CLI/manual apply
- Airflow: DET_AIRFLOW_* (Compose defaults); never trigger DagRuns via MCP

Canonical id is `provider.source`. Lake: `raw|bronze/{provider}/{source}/`.
DuckDB/Postgres: `{medallion}_{provider}.{source}` (e.g. `bronze_noaa.storm_events`).
CLI: `det run -p noaa.storm_events` (same resolver; see DET_PROJECT_ROOT).

dlt is extraction tooling only. DET owns validation, meta, and bronze landing.
Never use `dlt.pipeline` / `pipeline.run` for bronze.
"""


def pipeline_yaml(name: str, *, root: Path | None = None) -> str:
    base = root.resolve() if root is not None else project_root()
    path = resolve_pipeline_ref(name, project_root=base).path
    resolve_under_root(path, root=base)
    return path.read_text(encoding="utf-8")


def schema_yaml(relative_path: str, *, root: Path | None = None) -> str:
    base = root.resolve() if root is not None else project_root()
    # FastMCP templates match [^/]+; nested paths may arrive percent-encoded.
    rel = unquote(relative_path).lstrip("/")
    if rel.startswith("schemas/"):
        path = resolve_under_root(rel, root=base)
    else:
        path = resolve_under_root(schemas_dir(base) / rel, root=base)
    if not path.is_file():
        raise FileNotFoundError(f"schema not found: {relative_path}")
    # Must remain under schemas/
    path.relative_to(schemas_dir(base).resolve())
    return path.read_text(encoding="utf-8")


def schema_yaml_nested(
    dataset: str, filename: str, *, root: Path | None = None
) -> str:
    return schema_yaml(f"{dataset}/{filename}", root=root)


def readme_pointer() -> str:
    return _README_POINTER
