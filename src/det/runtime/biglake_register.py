"""Register DET Iceberg tables as BigLake external tables in BigQuery."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from det.ingestion.iceberg_catalog import resolve_metadata_location
from det.logging import get_logger
from det.runtime.approval import ApprovalPlan, make_plan
from det.runtime.config import PipelineConfig, load_pipeline_config, resolve_path
from det.runtime.ids import parse_canonical_id, sql_schema_name
from det.runtime.lake import LakeRef, resolve_lake_roots
from det.runtime.receipts_materialize import OPS_NAMESPACE, OPS_TABLE, ops_run_receipts_location
from det.runtime.settings import get_active_settings

logger = get_logger(__name__)

ENV_GCP_PROJECT = "DET_GCP_PROJECT"
ENV_BQ_LOCATION = "DET_BQ_LOCATION"
ENV_BQ_CONNECTION = "DET_BQ_CONNECTION"
DEFAULT_CONNECTION = "det-lake-conn"


@dataclass(frozen=True)
class BigLakeTablePlan:
    bq_dataset: str
    bq_table: str
    table_location: str
    metadata_uri: str
    kind: Literal["bronze", "ops"]


@dataclass(frozen=True)
class BigLakeRegisterPlan:
    project: str
    location: str
    connection: str
    lake_uri: str
    tables: tuple[BigLakeTablePlan, ...]
    bronze_uri: str | None = None
    ops_uri: str | None = None
    lake_layout: int = 1

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "project": self.project,
            "location": self.location,
            "connection": self.connection,
            "lake_uri": self.lake_uri,
            "lake_layout": self.lake_layout,
            "tables": [
                {
                    "bq_dataset": t.bq_dataset,
                    "bq_table": t.bq_table,
                    "table_location": t.table_location,
                    "metadata_uri": t.metadata_uri,
                    "kind": t.kind,
                }
                for t in self.tables
            ],
        }
        if self.bronze_uri:
            out["bronze_uri"] = self.bronze_uri
        if self.ops_uri:
            out["ops_uri"] = self.ops_uri
        return out


def _lake_uri_str(lake: LakeRef) -> str:
    return str(lake).rstrip("/")


def _metadata_uri_for_table(table_dir: LakeRef) -> str:
    hint_file = table_dir / "metadata" / "version-hint.text"
    if hint_file.exists():
        hint = hint_file.read_text(encoding="utf-8").strip()
        return resolve_metadata_location(str(table_dir), hint)

    meta_dir = table_dir / "metadata"
    if not meta_dir.exists():
        raise FileNotFoundError(f"No Iceberg metadata under {table_dir}")

    candidates = sorted(meta_dir.glob("*.metadata.json"), key=lambda p: p.name)
    if not candidates:
        raise FileNotFoundError(f"No *.metadata.json under {meta_dir}")
    return str(candidates[-1])


def _bronze_table_plans(
    lake: LakeRef, pipeline: PipelineConfig | None, *, layout: int = 1
) -> list[BigLakeTablePlan]:
    bronze_root = lake if layout >= 2 else lake / "bronze"
    if not bronze_root.exists():
        return []

    if pipeline is not None:
        provider, source_part = parse_canonical_id(pipeline.name)
        table_name = f"{source_part}_v{pipeline.wire_version}"
        table_dir = bronze_root / provider / table_name
        if not table_dir.exists():
            return []
        schema = sql_schema_name(pipeline.destination.dataset or "bronze", provider)
        return [
            BigLakeTablePlan(
                bq_dataset=schema,
                bq_table=table_name,
                table_location=str(table_dir),
                metadata_uri=_metadata_uri_for_table(table_dir),
                kind="bronze",
            )
        ]

    out: list[BigLakeTablePlan] = []
    for provider_dir in sorted(bronze_root.iterdir(), key=lambda p: p.name):
        if not provider_dir.is_dir():
            continue
        schema = sql_schema_name("bronze", provider_dir.name)
        for table_dir in sorted(provider_dir.iterdir(), key=lambda p: p.name):
            if not table_dir.is_dir():
                continue
            if not (table_dir / "metadata").exists():
                continue
            try:
                meta_uri = _metadata_uri_for_table(table_dir)
            except FileNotFoundError:
                continue
            out.append(
                BigLakeTablePlan(
                    bq_dataset=schema,
                    bq_table=table_dir.name,
                    table_location=str(table_dir),
                    metadata_uri=meta_uri,
                    kind="bronze",
                )
            )
    return out


def _ops_table_plan(lake: LakeRef) -> BigLakeTablePlan | None:
    table_dir = ops_run_receipts_location(lake)
    if not table_dir.exists() or not (table_dir / "metadata").exists():
        return None
    return BigLakeTablePlan(
        bq_dataset=OPS_NAMESPACE,
        bq_table=OPS_TABLE,
        table_location=str(table_dir),
        metadata_uri=_metadata_uri_for_table(table_dir),
        kind="ops",
    )


def build_biglake_register_plan(
    *,
    project_root: Path,
    lake_path: str | None = None,
    pipeline: PipelineConfig | Path | str | None = None,
    project: str | None = None,
    location: str | None = None,
    connection: str | None = None,
    include_ops: bool = True,
    env: dict[str, str] | None = None,
) -> BigLakeRegisterPlan:
    environ = dict(os.environ if env is None else env)
    root = project_root.resolve()

    config: PipelineConfig | None = None
    if pipeline is not None:
        if isinstance(pipeline, PipelineConfig):
            config = pipeline
        else:
            config = load_pipeline_config(resolve_path(root, str(pipeline)))

    roots = resolve_lake_roots(
        get_active_settings(),
        project_root=root,
        cli_lake_path=lake_path,
        destination_path=config.destination.path if config is not None else None,
        env=environ,
    )
    bronze_lake = roots.bronze
    ops_lake = roots.ops
    lake_uri = _lake_uri_str(bronze_lake)
    if not lake_uri.startswith("gs://") and not _lake_uri_str(ops_lake).startswith("gs://"):
        raise ValueError(
            f"BigLake registration requires a gs:// lake "
            f"(got bronze={lake_uri!r}, ops={_lake_uri_str(ops_lake)!r})"
        )
    if not lake_uri.startswith("gs://"):
        lake_uri = _lake_uri_str(ops_lake)

    gcp_project = (project or environ.get(ENV_GCP_PROJECT) or "").strip()
    if not gcp_project:
        gcp_project = (environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    if not gcp_project:
        raise ValueError(f"{ENV_GCP_PROJECT} or GOOGLE_CLOUD_PROJECT is required")

    bq_location = (location or environ.get(ENV_BQ_LOCATION) or "US").strip()
    conn = (connection or environ.get(ENV_BQ_CONNECTION) or DEFAULT_CONNECTION).strip()

    tables: list[BigLakeTablePlan] = list(
        _bronze_table_plans(bronze_lake, config, layout=roots.layout)
    )
    if include_ops and config is None:
        ops_plan = _ops_table_plan(ops_lake)
        if ops_plan is not None:
            tables.append(ops_plan)

    return BigLakeRegisterPlan(
        project=gcp_project,
        location=bq_location,
        connection=conn,
        lake_uri=lake_uri,
        tables=tuple(tables),
        bronze_uri=_lake_uri_str(bronze_lake),
        ops_uri=_lake_uri_str(ops_lake),
        lake_layout=roots.layout,
    )


def _lake_bucket(lake_uri: str) -> str:
    """Parse gs://bucket/prefix → bucket name."""
    if not lake_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// lake URI (got {lake_uri!r})")
    rest = lake_uri[5:]
    bucket, _, _ = rest.partition("/")
    if not bucket:
        raise ValueError(f"Missing bucket in lake URI {lake_uri!r}")
    return bucket


def _lookup_connection_sa(project: str, location: str, connection: str) -> str | None:
    """Best-effort connection SA lookup; None when offline or connection missing."""
    try:
        from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]
    except ImportError:
        return None

    try:
        client = bigquery.Client(project=project)
        conn_name = f"projects/{project}/locations/{location}/connections/{connection}"
        conn = client.get_connection(conn_name)  # pyright: ignore[reportAttributeAccessIssue]
    except Exception:
        return None

    sa = getattr(conn, "service_account_id", None) or getattr(conn, "serviceAccountId", None)
    if sa:
        return str(sa).strip() or None
    if isinstance(getattr(conn, "cloud_resource", None), dict):
        sa = conn.cloud_resource.get("serviceAccountId")
        if sa:
            return str(sa).strip() or None
    return None


def build_iam_hint(plan: BigLakeRegisterPlan) -> dict[str, Any]:
    """Structured IAM prerequisites for dry-run / MCP (never grants IAM)."""
    connection_sa = _lookup_connection_sa(plan.project, plan.location, plan.connection)
    hint: dict[str, Any] = {
        "connection": plan.connection,
        "lake_uri": plan.lake_uri,
        "lake_layout": plan.lake_layout,
    }
    if plan.lake_layout >= 2 and plan.bronze_uri and plan.ops_uri:
        bronze_bucket = (
            _lake_bucket(plan.bronze_uri)
            if plan.bronze_uri.startswith("gs://")
            else None
        )
        ops_bucket = (
            _lake_bucket(plan.ops_uri) if plan.ops_uri.startswith("gs://") else None
        )
        hint["bronze_uri"] = plan.bronze_uri
        hint["ops_uri"] = plan.ops_uri
        hint["buckets"] = {
            "bronze": bronze_bucket,
            "ops": ops_bucket,
        }
        if connection_sa and bronze_bucket:
            hint["connection_sa"] = connection_sa
            cmds = [
                (
                    f'gcloud storage buckets add-iam-policy-binding "gs://{bronze_bucket}" '
                    f'--member="serviceAccount:{connection_sa}" '
                    f'--role="roles/storage.objectViewer"'
                )
            ]
            if ops_bucket and ops_bucket != bronze_bucket:
                cmds.append(
                    f'gcloud storage buckets add-iam-policy-binding "gs://{ops_bucket}" '
                    f'--member="serviceAccount:{connection_sa}" '
                    f'--role="roles/storage.objectViewer"'
                )
            hint["gcloud_commands"] = cmds
            hint["gcloud_command"] = cmds[0]
            hint["note"] = (
                "Layout 2: grant objectViewer on bronze and ops buckets to the "
                "BigLake connection SA. Extract/load SAs use separate raw/bronze/ops grants "
                "(see docs/gcp-biglake.md)."
            )
        else:
            hint["note"] = (
                "Connection SA not resolved (connection missing, no ADC, or offline). "
                "Create the connection with bq mk --connection, then grant "
                "roles/storage.objectViewer on the bronze and ops buckets to the "
                "connection SA. See docs/gcp-biglake.md."
            )
        return hint

    bucket = _lake_bucket(plan.lake_uri)
    hint["bucket"] = bucket
    if connection_sa:
        hint["connection_sa"] = connection_sa
        hint["gcloud_command"] = (
            f'gcloud storage buckets add-iam-policy-binding "gs://{bucket}" '
            f'--member="serviceAccount:{connection_sa}" '
            f'--role="roles/storage.objectViewer"'
        )
    else:
        hint["note"] = (
            "Connection SA not resolved (connection missing, no ADC, or offline). "
            "Create the connection with bq mk --connection, then grant "
            "roles/storage.objectViewer on the lake bucket to the connection SA. "
            "See docs/gcp-biglake.md#prerequisites-before-first-register."
        )
    return hint


def format_iam_hint(plan: BigLakeRegisterPlan) -> str:
    """Human-readable IAM hint block for CLI dry-run."""
    hint = build_iam_hint(plan)
    lines = ["IAM hint:"]
    if hint.get("lake_layout", 1) >= 2:
        lines.append(f"  layout={hint['lake_layout']}")
        if hint.get("bronze_uri"):
            lines.append(f"  bronze_uri={hint['bronze_uri']}")
        if hint.get("ops_uri"):
            lines.append(f"  ops_uri={hint['ops_uri']}")
        buckets = hint.get("buckets") or {}
        if buckets.get("bronze"):
            lines.append(f"  bronze_bucket=gs://{buckets['bronze']}")
        if buckets.get("ops"):
            lines.append(f"  ops_bucket=gs://{buckets['ops']}")
    else:
        lines.append(f"  bucket=gs://{hint['bucket']}")
    lines.append(f"  connection={hint['connection']}")
    if "connection_sa" in hint:
        lines.append(f"  connection_sa={hint['connection_sa']}")
        for cmd in hint.get("gcloud_commands") or [hint.get("gcloud_command")]:
            if cmd:
                lines.append(f"  gcloud={cmd}")
    if "note" in hint:
        lines.append(f"  note={hint['note']}")
    return "\n".join(lines)


def external_table_ddl(plan: BigLakeRegisterPlan, table: BigLakeTablePlan) -> str:
    conn_fqn = f"`{plan.project}.{plan.location}.{plan.connection}`"
    table_fqn = f"`{plan.project}.{table.bq_dataset}.{table.bq_table}`"
    uri = table.metadata_uri.replace("'", "\\'")
    return (
        f"CREATE OR REPLACE EXTERNAL TABLE {table_fqn}\n"
        f"WITH CONNECTION {conn_fqn}\n"
        f"OPTIONS (\n"
        f"  format = 'ICEBERG',\n"
        f"  uris = ['{uri}']\n"
        f");"
    )


def biglake_register_write_argv(
    *,
    lake_path: str | None = None,
    lake_path_raw: str | None = None,
    lake_path_bronze: str | None = None,
    lake_path_ops: str | None = None,
    pipeline: str | None = None,
    project: str | None = None,
    location: str | None = None,
    connection: str | None = None,
    skip_ops: bool = False,
) -> list[str]:
    argv = ["biglake-register", "--apply"]
    if lake_path:
        argv.extend(["--lake-path", lake_path])
    if lake_path_raw:
        argv.extend(["--lake-path-raw", lake_path_raw])
    if lake_path_bronze:
        argv.extend(["--lake-path-bronze", lake_path_bronze])
    if lake_path_ops:
        argv.extend(["--lake-path-ops", lake_path_ops])
    if pipeline:
        argv.extend(["--pipeline", pipeline])
    if project:
        argv.extend(["--project", project])
    if location:
        argv.extend(["--location", location])
    if connection:
        argv.extend(["--connection", connection])
    if skip_ops:
        argv.append("--skip-ops")
    return argv


def approval_plan_for_register(plan: BigLakeRegisterPlan, argv: list[str]) -> ApprovalPlan:
    return make_plan("biglake-register", argv)


def _ensure_dataset(client: Any, project: str, dataset_id: str, location: str) -> None:
    from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]

    ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ref.location = location
    try:
        client.get_dataset(ref)
    except Exception:
        client.create_dataset(ref, exists_ok=True)


def apply_biglake_register(plan: BigLakeRegisterPlan) -> dict[str, Any]:
    try:
        from google.cloud import bigquery  # pyright: ignore[reportAttributeAccessIssue]
    except ImportError as exc:
        raise RuntimeError(
            'google-cloud-bigquery is required. Install: uv pip install -e ".[bigquery]"'
        ) from exc

    client = bigquery.Client(project=plan.project)
    applied: list[dict[str, str]] = []

    for dataset_id in sorted({t.bq_dataset for t in plan.tables}):
        _ensure_dataset(client, plan.project, dataset_id, plan.location)

    for table in plan.tables:
        ddl = external_table_ddl(plan, table)
        logger.info(
            "biglake register",
            dataset=table.bq_dataset,
            table=table.bq_table,
            metadata_uri=table.metadata_uri,
        )
        job = client.query(ddl, location=plan.location)
        job.result()
        applied.append(
            {
                "bq_dataset": table.bq_dataset,
                "bq_table": table.bq_table,
                "metadata_uri": table.metadata_uri,
            }
        )

    return {"applied": applied, "count": len(applied)}


def format_dry_run(plan: BigLakeRegisterPlan, argv: list[str]) -> str:
    lines = [
        f"DRY-RUN biglake-register project={plan.project} location={plan.location} "
        f"connection={plan.connection} tables={len(plan.tables)}",
    ]
    for table in plan.tables:
        lines.append(
            f"  {table.bq_dataset}.{table.bq_table} ({table.kind}) "
            f"metadata={table.metadata_uri}"
        )
    lines.append("")
    lines.append(format_iam_hint(plan))
    lines.append("")
    lines.append("approval_plan:")
    lines.append(json.dumps(approval_plan_for_register(plan, argv).to_dict(), indent=2))
    return "\n".join(lines)
