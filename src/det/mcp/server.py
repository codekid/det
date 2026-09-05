"""FastMCP stdio server for DET (read-only + dry-run)."""

from __future__ import annotations

from typing import Any

from det.mcp import params as p
from det.mcp import prompts as pr
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
            "Inspect: check, diff_partitions, diff_bronze_silver, sample_raw, validate_sample, "
            "sample_bronze, diagnose_pipeline (sample size via limit/sample_limit, "
            "default 5, max 50). "
            "Generate (dry-run): schema_from_sample_dry_run, mapper_from_diff_dry_run. "
            "scaffold_ops_dry_run previews ops dbt models (never writes). "
            "silver_catchup_dry_run previews ops/silver_catchup/<scm_id>.json (never writes). "
            "silver_catchup_cleanup_dry_run previews BQ _det_catchup_runs_* drops (never writes). "
            "Airflow inspect (read-only): airflow_health, list_airflow_dags, "
            "list_airflow_dag_runs, describe_airflow_det_env, preview_backfill_conf. "
            "Configure via DET_AIRFLOW_BASE_URL/USER/PASSWORD (Compose defaults). "
            "migrate_dry_run previews bronze rebuild from raw (never writes). "
            "Never trigger DagRuns or extract/load/prune-apply/migrate-write via MCP. "
            "list_runs / summarize_runs read extract/load receipts (observability); "
            "meta/manifest.json is the authority for landed partitions. "
            "check is structure validation (same as det check --json); never writes. "
            "Catalog: list_models / describe_model (dbt YAML). "
            "query_analytics is capped SELECT on gold/silver_* or ops DuckDB. "
            "Certified metrics: cube_meta / cube_load (Cube Core; make cube-up). "
            "This-run receipts: list_runs; fleet: cube_load run_daily or "
            "query_analytics warehouse=ops. "
            "Dry-runs that preview a write include approval_plan (command, argv, "
            "plan_digest). Operator: det approve --plan; agent writes later with "
            "--approval. Inspect records: list_approvals (status=claimed finds one "
            "stuck by a crashed run) / describe_approval. "
            "biglake_register_dry_run previews BigLake registration (never writes). "
            "iceberg_register_dry_run previews REST/Glue catalog registration (never writes). "
            "Prompts det_ops / det_new_source / det_migrate / det_dbt / det_airflow "
            "load .cursor/skills playbooks. "
            "dlt is extraction only — never suggest dlt.pipeline for landing."
        ),
    )

    @mcp.tool()
    def list_pipelines() -> dict[str, Any]:
        """List pipeline YAML names under configs/pipelines/."""
        return t.list_pipelines()

    @mcp.tool()
    def list_sources() -> dict[str, Any]:
        """List discovered DET source plugins (path convention + entry points)."""
        return t.list_sources_tool()

    @mcp.tool()
    def list_mappers() -> dict[str, Any]:
        """List registered migrate mappers and short summaries."""
        return t.list_mappers_tool()

    @mcp.tool()
    def describe_pipeline(pipeline: p.PipelineRef) -> dict[str, Any]:
        """Summarize a pipeline config (name, source, schema, destination, dbt.silver)."""
        return t.describe_pipeline(pipeline)

    @mcp.tool()
    def list_raw_partitions(
        pipeline: p.PipelineRef, limit: p.ListLimit = t.DEFAULT_LIST_LIMIT
    ) -> dict[str, Any]:
        """Walk raw/<dataset>/ hive interval + extract-run partitions (capped)."""
        return t.list_raw_partitions(pipeline, limit=limit)

    @mcp.tool()
    def list_bronze_partitions(
        pipeline: p.PipelineRef, limit: p.ListLimit = t.DEFAULT_LIST_LIMIT
    ) -> dict[str, Any]:
        """Walk filesystem bronze hive dirs, Iceberg extract-runs, or a SQL dest hint."""
        return t.list_bronze_partitions(pipeline, limit=limit)

    @mcp.tool()
    def read_manifest(run_path: p.RunPath) -> dict[str, Any]:
        """Read meta/manifest.json for a raw extract-run path under the lake."""
        return t.read_manifest(run_path)

    @mcp.tool()
    def prune_dry_run(
        pipeline: p.PipelineRef,
        interval_start: p.IntervalStart,
        interval_end: p.IntervalEndOpt = None,
        keep: p.Keep = 1,
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
        pipeline: p.PipelineRefOpt = None,
        command: p.DbtCommand = "build",
        select: p.DbtSelectOpt = None,
        catchup: p.CatchupFlag = False,
        catchup_manifest: p.CatchupManifestOpt = None,
    ) -> dict[str, Any]:
        """Preview the dbt CLI argv DET would run (dry_run=True)."""
        return t.dbt_dry_run(
            pipeline,
            command=command,
            select=select,
            catchup=catchup,
            catchup_manifest=catchup_manifest,
        )

    @mcp.tool()
    def scaffold_dbt_dry_run(pipeline: p.PipelineRef, force: p.Force = False) -> dict[str, Any]:
        """Preview scaffold-dbt file actions without writing."""
        return t.scaffold_dbt_dry_run(pipeline, force=force)

    @mcp.tool()
    def scaffold_ops_dry_run(force: p.Force = False) -> dict[str, Any]:
        """Preview scaffold-ops file actions without writing."""
        return t.scaffold_ops_dry_run(force=force)

    @mcp.tool()
    def init_pipeline_dry_run(
        name: p.PipelineName,
        source_type: p.SourceType,
        destination_type: p.DestinationType = "iceberg",
        connection: p.ConnectionEnv = None,
        lake_path: p.LakePathOpt = None,
        skip_dbt: p.SkipDbt = False,
    ) -> dict[str, Any]:
        """
        Preview init-pipeline actions without writing files.

        For postgres, ``connection`` is the env var name holding the DSN
        (e.g. DET_POSTGRES_DSN) — never paste a DSN with a password.
        """
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
        pipeline: p.PipelineRef,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        limit: p.ListLimit = t.DEFAULT_LIST_LIMIT,
    ) -> dict[str, Any]:
        """Compare raw vs bronze extract-run keys (hive and/or SQL meta columns)."""
        return t.diff_partitions(
            pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            limit=limit,
        )

    @mcp.tool()
    def diff_bronze_silver(
        pipeline: p.PipelineRefOpt = None,
        all_pipelines: bool = False,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        extract_lookback: p.ExtractLookbackOpt = None,
        limit: p.ListLimit = t.DEFAULT_LIST_LIMIT,
    ) -> dict[str, Any]:
        """Latest bronze extract-run per interval vs silver (catch-up candidates)."""
        return t.diff_bronze_silver(
            pipeline,
            all_pipelines=all_pipelines,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
            limit=limit,
        )

    @mcp.tool()
    def silver_catchup_dry_run(
        pipeline: p.PipelineRefOpt = None,
        all_pipelines: bool = False,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        extract_lookback: p.ExtractLookbackOpt = None,
        limit: p.ListLimit = t.DEFAULT_LIST_LIMIT,
    ) -> dict[str, Any]:
        """Preview silver catch-up manifest + approval_plan (never writes)."""
        return t.silver_catchup_dry_run(
            pipeline,
            all_pipelines=all_pipelines,
            interval_start=interval_start,
            interval_end=interval_end,
            extract_lookback=extract_lookback,
            limit=limit,
        )

    @mcp.tool()
    def silver_catchup_cleanup_dry_run(
        manifest_id: p.CatchupManifestIdOpt = None,
        older_than: p.OlderThanOpt = None,
    ) -> dict[str, Any]:
        """Preview BQ catch-up external table drops + approval_plan (never writes)."""
        return t.silver_catchup_cleanup_dry_run(
            manifest_id=manifest_id,
            older_than=older_than,
        )

    @mcp.tool()
    def sample_raw(
        pipeline: p.PipelineRef,
        stage: p.Stage = "named",
        limit: p.SampleLimit = t.DEFAULT_SAMPLE_LIMIT,
        run_path: p.RunPathOpt = None,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        extract_run_datetime: p.ExtractRunOpt = None,
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
        pipeline: p.PipelineRef,
        limit: p.SampleLimit = t.DEFAULT_SAMPLE_LIMIT,
        max_errors: p.MaxErrors = 20,
        run_path: p.RunPathOpt = None,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        extract_run_datetime: p.ExtractRunOpt = None,
    ) -> dict[str, Any]:
        """
        Coerce + JSON Schema validate a capped raw sample; errors returned as data.

        Use when load failed with schema_invalid or diagnose_pipeline flagged drift.
        Raise limit toward 50 for nested APIs so rare extra fields show up.
        """
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
        pipeline: p.PipelineRef,
        limit: p.SampleLimit = t.DEFAULT_SAMPLE_LIMIT,
        run_path: p.RunPathOpt = None,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        extract_run_datetime: p.ExtractRunOpt = None,
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
        pipeline: p.PipelineRef,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        sample_limit: p.SampleLimit = t.DEFAULT_SAMPLE_LIMIT,
    ) -> dict[str, Any]:
        """
        First stop for missing raw/bronze or schema drift.

        Coverage diagnose plus optional validate_sample; returns findings codes
        and suggested CLI (do not run until the user confirms).
        """
        return t.diagnose_pipeline(
            pipeline,
            interval_start=interval_start,
            interval_end=interval_end,
            sample_limit=sample_limit,
        )

    @mcp.tool()
    def schema_from_sample_dry_run(
        pipeline: p.PipelineRefOpt = None,
        run_path: p.RunPathOpt = None,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        extract_run_datetime: p.ExtractRunOpt = None,
        records: p.RecordsOpt = None,
        limit: p.SampleLimit = t.MAX_SAMPLE_LIMIT,
        schema_out: p.SchemaOutOpt = None,
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
        from_schema: p.SchemaPath,
        to_schema: p.SchemaPath,
        mapper_name: p.MapperName,
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
    def list_airflow_dag_runs(dag_id: p.DagId, limit: p.DagRunLimit = 10) -> dict[str, Any]:
        """Recent DagRuns for one DAG (limit default 10, max 50). Read-only."""
        return t.list_airflow_dag_runs(dag_id, limit=limit)

    @mcp.tool()
    def describe_airflow_det_env() -> dict[str, Any]:
        """Local airflow/.env DET_* knobs (passwords redacted). Read-only."""
        return t.describe_airflow_det_env()

    @mcp.tool()
    def preview_backfill_conf(
        interval_start: p.IntervalStart, interval_end: p.IntervalEnd
    ) -> dict[str, Any]:
        """Preview backfill conf + trigger command strings; never triggers."""
        return t.preview_backfill_conf(interval_start, interval_end)

    @mcp.tool()
    def migrate_dry_run(
        pipeline: p.PipelineRef,
        to_bronze: p.ToBronze,
        schema: p.SchemaPath,
        mapper: p.MapperName,
        interval_start: p.IntervalStartOpt = None,
        interval_end: p.IntervalEndOpt = None,
        from_raw: p.FromRawOpt = None,
        validate_limit: p.ValidateLimit = t.MAX_SAMPLE_LIMIT,
        confirm_full_validate: p.ConfirmFullValidate = False,
        wire_version: p.WireVersionOpt = None,
        recreate_iceberg: p.RecreateIceberg = False,
        all_raw: p.AllRaw = False,
        all_raw_runs: p.AllRawRuns = False,
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
            confirm_full_validate=confirm_full_validate,
            wire_version=wire_version,
            recreate_iceberg=recreate_iceberg,
            all_raw=all_raw,
            all_raw_runs=all_raw_runs,
        )

    @mcp.tool()
    def biglake_register_dry_run(
        pipeline: p.PipelineRefOpt = None,
        lake_path: p.LakePathOpt = None,
        project: p.GcpProjectOpt = None,
        location: p.BqLocationOpt = None,
        connection: p.BqConnectionOpt = None,
        skip_ops: bool = False,
    ) -> dict[str, Any]:
        """Preview BigLake Iceberg registration for gs:// lakes (never creates BQ tables)."""
        return t.biglake_register_dry_run(
            pipeline=pipeline,
            lake_path=lake_path,
            project=project,
            location=location,
            connection=connection,
            skip_ops=skip_ops,
        )

    @mcp.tool()
    def iceberg_register_dry_run(
        pipeline: p.PipelineRefOpt = None,
        lake_path: p.LakePathOpt = None,
        skip_ops: bool = False,
    ) -> dict[str, Any]:
        """Preview Iceberg REST/Glue registration (never mutates the catalog)."""
        return t.iceberg_register_dry_run(
            pipeline=pipeline,
            lake_path=lake_path,
            skip_ops=skip_ops,
        )

    @mcp.tool()
    def list_runs(
        pipeline: p.PipelineRefOpt = None,
        since: p.ReceiptSinceOpt = None,
        until: p.ReceiptUntilOpt = None,
        status: p.ReceiptStatusOpt = None,
        command: p.ReceiptCommandOpt = None,
        limit: p.ListLimit = t.DEFAULT_LIST_LIMIT,
    ) -> dict[str, Any]:
        """
        List extract/load run receipts after a failed or slow job.

        Observability only — meta/manifest.json is the authority for landed data.
        """
        return t.list_runs(
            pipeline,
            since=since,
            until=until,
            status=status,
            command=command,
            limit=limit,
        )

    @mcp.tool()
    def summarize_runs(
        pipeline: p.PipelineRefOpt = None,
        since: p.ReceiptSinceOpt = None,
        until: p.ReceiptUntilOpt = None,
        status: p.ReceiptStatusOpt = None,
        command: p.ReceiptCommandOpt = None,
    ) -> dict[str, Any]:
        """Summarize extract/load receipts: attempts, errors, p50/p95 duration, rows."""
        return t.summarize_runs(
            pipeline,
            since=since,
            until=until,
            status=status,
            command=command,
        )

    @mcp.tool()
    def list_models() -> dict[str, Any]:
        """List dbt models (stg/silver/gold/ops) with schema, layer, warehouse, grain."""
        return t.list_models()

    @mcp.tool()
    def describe_model(name: p.ModelName) -> dict[str, Any]:
        """Describe one dbt model including YAML columns and grain."""
        return t.describe_model(name)

    @mcp.tool()
    def query_analytics(
        sql: p.AnalyticsSql,
        warehouse: p.Warehouse = "analytics",
        limit: p.SampleLimit = t.DEFAULT_SAMPLE_LIMIT,
    ) -> dict[str, Any]:
        """
        Capped read-only SELECT for silver/ops row detail.

        warehouse=analytics → gold + silver_*; warehouse=ops → ops only.
        Certified gold/ops metrics should use cube_load, not this tool.
        """
        return t.query_analytics(sql, warehouse=warehouse, limit=limit)

    @mcp.tool()
    def cube_meta() -> dict[str, Any]:
        """
        Cube Core semantic meta (cubes, measures, dimensions).

        Requires `make cube-up`. yearly_damage is gold; run_daily is ops.
        """
        return t.cube_meta()

    @mcp.tool()
    def cube_load(
        measures: p.CubeMeasures,
        dimensions: p.CubeDimensionsOpt = None,
        filters: p.CubeFiltersOpt = None,
        limit: p.SampleLimit = t.DEFAULT_SAMPLE_LIMIT,
    ) -> dict[str, Any]:
        """
        Run a certified Cube metric query (REST /load).

        Gold metrics: yearly_damage.*; fleet ops: run_daily.* .
        If Cube is down, start it with make cube-up — do not invent metric SQL.
        """
        return t.cube_load(
            measures,
            dimensions=dimensions,
            filters=filters,
            limit=limit,
        )

    @mcp.tool()
    def check(pipeline: p.PipelineRefOpt = None) -> dict[str, Any]:
        """
        Structure-check pipeline YAML and schemas (same payload as det check --json).

        Use after editing configs/pipelines or schemas. Never writes; not extract/load.
        """
        return t.check(pipeline)

    @mcp.tool()
    def list_approvals(status: p.ApprovalStatusOpt = None) -> dict[str, Any]:
        """List approval files under .det/approvals/. Never creates them.

        Defaults to unused, unexpired. Pass status="claimed" to find an approval
        stuck by a crashed run; recovery is `det approval-release <id> --force`.
        """
        return t.list_approvals(status)

    @mcp.tool()
    def describe_approval(approval_id: p.ApprovalId) -> dict[str, Any]:
        """Read one approval record by id (apr_…). Expired status is derived at read time."""
        return t.describe_approval(approval_id)

    pr.register_skill_prompts(mcp)

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
