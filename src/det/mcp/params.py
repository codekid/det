"""Annotated MCP parameter types so FastMCP JSON Schema carries descriptions."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field

PipelineRef = Annotated[
    str,
    Field(
        description=(
            "Canonical pipeline id `provider.source` (e.g. noaa.storm_events), "
            "`provider/source`, or a YAML path under configs/pipelines/"
        )
    ),
]
PipelineRefOpt = Annotated[
    str | None,
    Field(
        description=(
            "Canonical pipeline id `provider.source` (e.g. noaa.storm_events), "
            "`provider/source`, or a YAML path; omit to include all pipelines"
        )
    ),
]
IntervalStart = Annotated[
    str,
    Field(
        description=(
            "Inclusive interval start as YYYY-MM-DD or ISO-8601 UTC "
            "(e.g. 2026-08-06 or 2026-08-06T00:00:00Z)"
        )
    ),
]
IntervalStartOpt = Annotated[
    str | None,
    Field(
        description=(
            "Inclusive interval start as YYYY-MM-DD or ISO-8601 UTC; omit to include all starts"
        )
    ),
]
IntervalEnd = Annotated[
    str,
    Field(
        description=(
            "Exclusive interval end as YYYY-MM-DD or ISO-8601 UTC "
            "(e.g. 2026-08-07 or 2026-08-07T00:00:00Z)"
        )
    ),
]
IntervalEndOpt = Annotated[
    str | None,
    Field(
        description=(
            "Exclusive interval end as YYYY-MM-DD or ISO-8601 UTC; "
            "omit to default start + 1 day where the tool supports it"
        )
    ),
]
ExtractLookbackOpt = Annotated[
    str | None,
    Field(
        description=(
            "Catch-up Mode A: only intervals touched by bronze extract runs in "
            "this lookback (e.g. 48h, 7d). Cannot combine with interval_start/end. "
            "Omit for full census (Mode B)."
        )
    ),
]
ExtractRunOpt = Annotated[
    str | None,
    Field(
        description=(
            "Extract-run datetime: ISO-8601 UTC or hive compact "
            "(e.g. 2026-08-06T10:00:00Z or 20260806T100000Z)"
        )
    ),
]
RunPath = Annotated[
    str,
    Field(
        description=(
            "Raw extract-run path under DET_PROJECT_ROOT / the lake (sandbox rejects path escapes)"
        )
    ),
]
RunPathOpt = Annotated[
    str | None,
    Field(
        description=(
            "Optional raw extract-run path under DET_PROJECT_ROOT / the lake; "
            "sandbox rejects path escapes"
        )
    ),
]
SampleLimit = Annotated[
    int,
    Field(description="Row sample size (max 50)", ge=1),
]
ListLimit = Annotated[
    int,
    Field(description="Max hive/receipt rows to return (default 200)", ge=1),
]
Stage = Annotated[
    Literal["wire", "rows", "named", "coerced"],
    Field(
        description=(
            "Raw sample stage: wire (source bytes/pages), rows (parsed), "
            "named, or coerced (pre-schema)"
        )
    ),
]
Keep = Annotated[
    int,
    Field(description="Bronze extract-run siblings to keep per interval (prune)", ge=1),
]
DestinationType = Annotated[
    Literal["iceberg", "filesystem", "duckdb", "postgres"],
    Field(
        description=(
            "Bronze writer: iceberg (default lake), filesystem (JSONL), duckdb, or postgres"
        )
    ),
]
ConnectionEnv = Annotated[
    str | None,
    Field(
        description=(
            "For postgres: env var *name* holding the DSN (e.g. DET_POSTGRES_DSN). "
            "Never paste a passwordful DSN"
        )
    ),
]
LakePathOpt = Annotated[
    str | None,
    Field(description="Lake root override (default ./data/lake or DET_LAKE_PATH)"),
]
SourceType = Annotated[
    str,
    Field(description="Registered source plugin id (`provider.source`)"),
]
PipelineName = Annotated[
    str,
    Field(description="New pipeline canonical id (`provider.source`)"),
]
SchemaPath = Annotated[
    str,
    Field(
        description=(
            "Schema YAML path under the project "
            "(e.g. schemas/noaa/storm_events/storm_events.schema.yaml)"
        )
    ),
]
MapperName = Annotated[
    str,
    Field(description="Registered migrate mapper name (e.g. identity)"),
]
FromRawOpt = Annotated[
    str | None,
    Field(
        description=(
            "Source raw dataset id when different from the pipeline (e.g. noaa.storm_events_v1)"
        )
    ),
]
ToBronze = Annotated[
    str,
    Field(description="Target bronze dataset id (e.g. noaa.storm_events_v2)"),
]
DbtCommand = Annotated[
    str,
    Field(description="dbt subcommand DET would run (default build)"),
]
DbtSelectOpt = Annotated[
    list[str] | None,
    Field(description="Optional dbt --select tokens; omit for pipeline default"),
]
Force = Annotated[
    bool,
    Field(description="Preview overwrite of existing scaffolded files"),
]
SkipDbt = Annotated[
    bool,
    Field(description="Skip previewing dbt scaffold actions"),
]
MaxErrors = Annotated[
    int,
    Field(description="Max coerce/schema errors to return (default 20)", ge=1),
]
ValidateLimit = Annotated[
    int,
    Field(
        description=(
            "Max rows to coerce+validate per partition in the migrate preview. "
            "Default 50; use 0 for full-partition (requires confirm_full_validate "
            "and DET_ALLOW_FULL_VALIDATE=1; capped at 100k rows per partition)"
        )
    ),
]
ConfirmFullValidate = Annotated[
    bool,
    Field(
        description=(
            "Required with validate_limit=0 after validate_sample / "
            "validate_limit=50 and user confirmation"
        )
    ),
]
WireVersionOpt = Annotated[
    int | None,
    Field(description="Optional wire_version filter for mixed lake trees"),
]
RecreateIceberg = Annotated[
    bool,
    Field(
        description=(
            "If true, dry-run/approval plan includes --recreate-iceberg "
            "(purge target Iceberg bronze table before rewrite)"
        )
    ),
]
AllRaw = Annotated[
    bool,
    Field(
        description=(
            "With recreate_iceberg: rewrite every raw interval (no interval_start). "
            "Latest extract per interval unless all_raw_runs"
        )
    ),
]
AllRawRuns = Annotated[
    bool,
    Field(
        description=(
            "Rematerialize every committed raw extract-run sibling "
            "(default: latest only, matching det load)"
        )
    ),
]
SchemaOutOpt = Annotated[
    str | None,
    Field(description="Would-write schema path for the dry-run preview (never written)"),
]
RecordsOpt = Annotated[
    list[dict[str, Any]] | None,
    Field(description="Inline sample records when not reading a raw partition"),
]
DagId = Annotated[
    str,
    Field(description="Airflow DAG id (e.g. det_extract)"),
]
DagRunLimit = Annotated[
    int,
    Field(description="Max DagRuns to return (default 10, max 50)", ge=1),
]
ReceiptSinceOpt = Annotated[
    str | None,
    Field(description="Attempt-date lower bound (YYYY-MM-DD); default last 7 days"),
]
ReceiptUntilOpt = Annotated[
    str | None,
    Field(description="Attempt-date upper bound (YYYY-MM-DD), inclusive"),
]
ReceiptStatusOpt = Annotated[
    str | None,
    Field(description="Filter receipts by status: ok or error"),
]
ReceiptCommandOpt = Annotated[
    str | None,
    Field(description="Filter receipts by command: extract or load"),
]
Warehouse = Annotated[
    Literal["analytics", "ops"],
    Field(
        description=(
            "DuckDB warehouse: analytics (gold + silver_*) or ops (DET_OPS_DUCKDB schema ops only)"
        )
    ),
]
ModelName = Annotated[
    str,
    Field(description="dbt model name (e.g. gold_yearly_damage, det__ops_run_daily)"),
]
AnalyticsSql = Annotated[
    str,
    Field(
        description=(
            "Single SELECT/WITH against schema.table. analytics: gold / silver_*; "
            "ops: ops.* . Certified metrics should use cube_load instead"
        )
    ),
]
CubeMeasures = Annotated[
    list[str],
    Field(
        description=("Cube measures (e.g. yearly_damage.total_property_damage, run_daily.attempts)")
    ),
]
CubeDimensionsOpt = Annotated[
    list[str] | None,
    Field(description=("Cube dimensions (e.g. yearly_damage.state, run_daily.pipeline)")),
]
CubeFiltersOpt = Annotated[
    list[dict[str, Any]] | None,
    Field(description="Optional Cube REST filters list (member, operator, values)"),
]
ApprovalId = Annotated[
    str,
    Field(description="Approval id from `det approve` (apr_ + 16 hex chars)"),
]
ApprovalStatusOpt = Annotated[
    str | None,
    Field(
        description=(
            "Filter approvals by derived status: unused (default), claimed, "
            "consumed, expired, or all. Use claimed to find an approval left "
            "stuck by a crashed run — claimed records never expire"
        )
    ),
]
GcpProjectOpt = Annotated[
    str | None,
    Field(
        description=(
            "GCP project id owning the BigQuery datasets; "
            "omit to read DET_GCP_PROJECT or GOOGLE_CLOUD_PROJECT"
        )
    ),
]
BqLocationOpt = Annotated[
    str | None,
    Field(
        description=(
            "BigQuery location for datasets and the connection (e.g. US, us-central1); "
            "omit to read DET_BQ_LOCATION (default US)"
        )
    ),
]
BqConnectionOpt = Annotated[
    str | None,
    Field(
        description=(
            "BigQuery cloud-resource connection name used to read the gs:// lake; "
            "omit to read DET_BQ_CONNECTION (default det-lake-conn)"
        )
    ),
]
CatchupFlag = Annotated[
    bool,
    Field(
        description=(
            "When true, preview det dbt --catchup (requires catchup_manifest scm_… id)"
        )
    ),
]
CatchupManifestOpt = Annotated[
    str | None,
    Field(
        description=(
            "Immutable catch-up manifest id (scm_ + 16 hex) from "
            "silver_catchup_dry_run / silver-catchup-plan --apply; required when catchup=true"
        )
    ),
]
CatchupManifestIdOpt = Annotated[
    str | None,
    Field(
        description=(
            "Catch-up manifest id (scm_ + 16 hex) for BQ external-table cleanup; "
            "mutually exclusive with older_than"
        )
    ),
]
OlderThanOpt = Annotated[
    str | None,
    Field(
        description=(
            "Relative age for BQ catch-up cleanup preview (e.g. 7d, 48h). "
            "Dry-run freezes created_before in approval_plan for apply; "
            "mutually exclusive with manifest_id"
        )
    ),
]
