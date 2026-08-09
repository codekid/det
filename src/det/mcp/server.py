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
            "No extract/load/prune-apply/scaffold writes. "
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
