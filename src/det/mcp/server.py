"""FastMCP stdio server for DET (read-only + dry-run)."""

from __future__ import annotations

from typing import Any

from det.mcp import resources as res
from det.mcp import tools as t


def create_server():
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "det-mcp requires the optional mcp extra: pip install -e '.[mcp]'"
        ) from exc

    mcp = FastMCP(
        "det",
        instructions=(
            "DET (Data Extract Tool) MCP v1: read-only inspect and dry-run tools. "
            "Inspect: diff_partitions, sample_raw, validate_sample, sample_bronze, "
            "diagnose_pipeline (sample size via limit/sample_limit, default 5, max 50). "
            "Generate (dry-run): schema_from_sample_dry_run, mapper_from_diff_dry_run. "
            "Airflow inspect (read-only): airflow_health, list_airflow_dags, "
            "list_airflow_dag_runs, describe_airflow_det_env, preview_backfill_conf. "
            "Configure via DET_AIRFLOW_BASE_URL/USER/PASSWORD (Compose defaults). "
            "migrate_dry_run previews bronze rebuild from raw (never writes). "
            "Never trigger DagRuns or extract/load/prune-apply/migrate-write via MCP. "
            "dlt is extraction only — never suggest dlt.pipeline for landing."
        ),
    )

    @mcp.tool()
    def list_pipelines() -> dict[str, Any]:
        """List pipeline YAML names under configs/pipelines/."""
        return t.list_pipelines()

    @mcp.tool()
    def list_sources() -> dict[str, Any]:
        """List registered DET source plugins."""
        return t.list_sources_tool()

    @mcp.tool()
    def list_mappers() -> dict[str, Any]:
        """List registered migrate mappers and short summaries."""
        return t.list_mappers_tool()

    @mcp.tool()
    def describe_pipeline(pipeline: str) -> dict[str, Any]:
        """Summarize a pipeline config (name, source, schema, destination, dbt.silver)."""
        return t.describe_pipeline(pipeline)

    @mcp.tool()
    def list_raw_partitions(
        pipeline: str, limit: int = t.DEFAULT_LIST_LIMIT
    ) -> dict[str, Any]:
        """Walk raw/<dataset>/ hive interval + extract-run partitions (capped)."""
        return t.list_raw_partitions(pipeline, limit=limit)

    @mcp.tool()
    def list_bronze_partitions(
        pipeline: str, limit: int = t.DEFAULT_LIST_LIMIT
    ) -> dict[str, Any]:
        """Walk filesystem bronze hive dirs, or return a destination hint for duckdb/postgres."""
        return t.list_bronze_partitions(pipeline, limit=limit)

    @mcp.tool()
    def read_manifest(run_path: str) -> dict[str, Any]:
        """Read meta/manifest.json for a raw extract-run path under the lake."""
        return t.read_manifest(run_path)

    @mcp.tool()
    def prune_dry_run(
        pipeline: str,
        interval_start: str,
        interval_end: str | None = None,
        keep: int = 1,
    ) -> dict[str, Any]:
        """Preview bronze prune candidates (BronzePruner.plan only; never deletes)."""
        return t.prune_dry_run(
            pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            keep=keep,
        )

    @mcp.tool()
    def dbt_dry_run(
        pipeline: str | None = None,
        command: str = "build",
        select: list[str] | None = None,
    ) -> dict[str, Any]:
        """Preview the dbt CLI argv DET would run (dry_run=True)."""
        return t.dbt_dry_run(pipeline, command=command, select=select)

    @mcp.tool()
    def scaffold_dbt_dry_run(pipeline: str, force: bool = False) -> dict[str, Any]:
        """Preview scaffold-dbt file actions without writing."""
        return t.scaffold_dbt_dry_run(pipeline, force=force)

    @mcp.tool()
    def init_pipeline_dry_run(
        name: str,
        source_type: str,
        destination_type: str = "filesystem",
        connection: str | None = None,
        lake_path: str = "./data/lake",
        skip_dbt: bool = False,
    ) -> dict[str, Any]:
        """Preview init-pipeline actions without writing files."""
        return t.init_pipeline_dry_run(
            name,
            source_type,
            destination_type=destination_type,
            connection=connection,
            lake_path=lake_path,
            skip_dbt=skip_dbt,
        )

    @mcp.tool()
    def diff_partitions(
        pipeline: str,
        interval_start: str | None = None,
        interval_end: str | None = None,
        limit: int = t.DEFAULT_LIST_LIMIT,
    ) -> dict[str, Any]:
        """Compare raw vs bronze extract-run keys (hive and/or SQL meta columns)."""
        return t.diff_partitions(
            pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            limit=limit,
        )

    @mcp.tool()
    def sample_raw(
        pipeline: str,
        stage: str = "named",
        limit: int = t.DEFAULT_SAMPLE_LIMIT,
        run_path: str | None = None,
        interval_start: str | None = None,
        interval_end: str | None = None,
        extract_run_datetime: str | None = None,
    ) -> dict[str, Any]:
        """Sample raw at stage wire|rows|named|coerced (limit default 5, max 50)."""
        return t.sample_raw(
            pipeline,
            stage=stage,
            limit=limit,
            run_path=run_path,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )

    @mcp.tool()
    def validate_sample(
        pipeline: str,
        limit: int = t.DEFAULT_SAMPLE_LIMIT,
        max_errors: int = 20,
        run_path: str | None = None,
        interval_start: str | None = None,
        interval_end: str | None = None,
        extract_run_datetime: str | None = None,
    ) -> dict[str, Any]:
        """Coerce + JSON Schema validate a capped raw sample; errors returned as data."""
        return t.validate_sample(
            pipeline,
            limit=limit,
            max_errors=max_errors,
            run_path=run_path,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )

    @mcp.tool()
    def sample_bronze(
        pipeline: str,
        limit: int = t.DEFAULT_SAMPLE_LIMIT,
        run_path: str | None = None,
        interval_start: str | None = None,
        interval_end: str | None = None,
        extract_run_datetime: str | None = None,
    ) -> dict[str, Any]:
        """Sample bronze rows (filesystem JSONL or DuckDB/Postgres LIMIT). Inspection only."""
        return t.sample_bronze(
            pipeline,
            limit=limit,
            run_path=run_path,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
        )

    @mcp.tool()
    def diagnose_pipeline(
        pipeline: str,
        interval_start: str | None = None,
        interval_end: str | None = None,
        sample_limit: int = t.DEFAULT_SAMPLE_LIMIT,
    ) -> dict[str, Any]:
        """Coverage diagnose + optional validate; returns findings and suggested CLI."""
        return t.diagnose_pipeline(
            pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            sample_limit=sample_limit,
        )

    @mcp.tool()
    def schema_from_sample_dry_run(
        pipeline: str | None = None,
        run_path: str | None = None,
        interval_start: str | None = None,
        interval_end: str | None = None,
        extract_run_datetime: str | None = None,
        records: list[dict[str, Any]] | None = None,
        limit: int = t.MAX_SAMPLE_LIMIT,
        schema_out: str | None = None,
    ) -> dict[str, Any]:
        """Infer bronze JSON Schema from sample rows or inline records (never writes)."""
        return t.schema_from_sample_dry_run(
            pipeline,
            run_path=run_path,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_run_datetime=extract_run_datetime,
            records=records,
            limit=limit,
            schema_out=schema_out,
        )

    @mcp.tool()
    def mapper_from_diff_dry_run(
        from_schema: str,
        to_schema: str,
        mapper_name: str,
    ) -> dict[str, Any]:
        """Diff two schema files and draft a mapper stub (never writes)."""
        return t.mapper_from_diff_dry_run(from_schema, to_schema, mapper_name)

    @mcp.tool()
    def airflow_health() -> dict[str, Any]:
        """Airflow /health (DET_AIRFLOW_* / Compose defaults). Read-only."""
        return t.airflow_health()

    @mcp.tool()
    def list_airflow_dags() -> dict[str, Any]:
        """List DET DAGs from Airflow REST (paused / import errors). Read-only."""
        return t.list_airflow_dags()

    @mcp.tool()
    def list_airflow_dag_runs(dag_id: str, limit: int = 10) -> dict[str, Any]:
        """Recent DagRuns for one DAG (limit default 10, max 50). Read-only."""
        return t.list_airflow_dag_runs(dag_id, limit=limit)

    @mcp.tool()
    def describe_airflow_det_env() -> dict[str, Any]:
        """Local airflow/.env DET_* knobs (passwords redacted). Read-only."""
        return t.describe_airflow_det_env()

    @mcp.tool()
    def preview_backfill_conf(
        interval_start: str, interval_end: str
    ) -> dict[str, Any]:
        """Preview backfill conf + trigger command strings; never triggers."""
        return t.preview_backfill_conf(interval_start, interval_end)

    @mcp.tool()
    def migrate_dry_run(
        pipeline: str,
        to_bronze: str,
        schema: str,
        mapper: str,
        interval_start: str,
        interval_end: str | None = None,
        from_raw: str | None = None,
        validate_limit: int = t.MAX_SAMPLE_LIMIT,
    ) -> dict[str, Any]:
        """Preview migrate (name/map/validate); never writes bronze."""
        return t.migrate_dry_run(
            pipeline,
            to_bronze,
            schema,
            mapper,
            interval_start,
            interval_end=interval_end,
            from_raw=from_raw,
            validate_limit=validate_limit,
        )

    @mcp.resource("det://pipelines/{name}")
    def resource_pipeline(name: str) -> str:
        """Pipeline YAML text."""
        return res.pipeline_yaml(name)

    @mcp.resource("det://schemas/{dataset}/{filename}")
    def resource_schema_nested(dataset: str, filename: str) -> str:
        """Schema YAML at schemas/{dataset}/{filename}."""
        return res.schema_yaml_nested(dataset, filename)

    @mcp.resource("det://schemas/{relative_path}")
    def resource_schema(relative_path: str) -> str:
        """Schema YAML under schemas/; use %2F for nested paths in one segment."""
        return res.schema_yaml(relative_path)

    @mcp.resource("det://readme")
    def resource_readme() -> str:
        """Short pointer to DET MCP resources and dlt boundaries."""
        return res.readme_pointer()

    return mcp


def main() -> None:
    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()
