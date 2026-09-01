"""Register existing DET Iceberg tables into REST or Glue catalogs.

Publishes tables already on the lake (``version-hint`` / metadata JSON) into the
metastore selected by ``DET_ICEBERG_CATALOG``. Refuse ``hadoop`` — there is no
external metastore to register into. See docs/iceberg-catalog.md.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from det.ingestion.iceberg_catalog import resolve_metadata_location
from det.ingestion.iceberg_catalog_factory import (
    ENV_CATALOG,
    ENV_REST_URI,
    IcebergCatalogKind,
    catalog_kind_from_env,
    resolve_iceberg_catalog,
)
from det.logging import get_logger
from det.runtime.approval import ApprovalPlan, make_plan
from det.runtime.config import PipelineConfig, load_pipeline_config, resolve_path
from det.runtime.ids import parse_canonical_id, sql_schema_name
from det.runtime.lake import LakeRef, open_lake, pick_lake_spec
from det.runtime.receipts_materialize import OPS_NAMESPACE, OPS_TABLE, ops_run_receipts_location

logger = get_logger(__name__)

RegisterKind = Literal["bronze", "ops"]
RegisterStatus = Literal["registered", "exists"]


@dataclass(frozen=True)
class IcebergRegisterTablePlan:
    namespace: str
    table: str
    table_location: str
    metadata_uri: str
    kind: RegisterKind


@dataclass(frozen=True)
class IcebergRegisterPlan:
    catalog_kind: IcebergCatalogKind
    lake_uri: str
    rest_uri_host: str | None
    warehouse: str | None
    glue_id: str | None
    tables: tuple[IcebergRegisterTablePlan, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "catalog_kind": self.catalog_kind,
            "lake_uri": self.lake_uri,
            "rest_uri_host": self.rest_uri_host,
            "warehouse": self.warehouse,
            "glue_id": self.glue_id,
            "tables": [
                {
                    "namespace": t.namespace,
                    "table": t.table,
                    "table_location": t.table_location,
                    "metadata_uri": t.metadata_uri,
                    "kind": t.kind,
                }
                for t in self.tables
            ],
        }


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
    lake: LakeRef, pipeline: PipelineConfig | None
) -> list[IcebergRegisterTablePlan]:
    bronze_root = lake / "bronze"
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
            IcebergRegisterTablePlan(
                namespace=schema,
                table=table_name,
                table_location=str(table_dir),
                metadata_uri=_metadata_uri_for_table(table_dir),
                kind="bronze",
            )
        ]

    out: list[IcebergRegisterTablePlan] = []
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
                IcebergRegisterTablePlan(
                    namespace=schema,
                    table=table_dir.name,
                    table_location=str(table_dir),
                    metadata_uri=meta_uri,
                    kind="bronze",
                )
            )
    return out


def _ops_table_plan(lake: LakeRef) -> IcebergRegisterTablePlan | None:
    table_dir = ops_run_receipts_location(lake)
    if not table_dir.exists() or not (table_dir / "metadata").exists():
        return None
    return IcebergRegisterTablePlan(
        namespace=OPS_NAMESPACE,
        table=OPS_TABLE,
        table_location=str(table_dir),
        metadata_uri=_metadata_uri_for_table(table_dir),
        kind="ops",
    )


def _require_register_catalog(
    environ: dict[str, str], lake_uri: str
) -> tuple[IcebergCatalogKind, str | None, str | None, str | None]:
    kind = catalog_kind_from_env(environ)
    if kind == "hadoop":
        raise ValueError(
            f"{ENV_CATALOG}=hadoop has no external metastore to register into; "
            f"set {ENV_CATALOG}=rest|glue (see docs/iceberg-catalog.md)"
        )

    rest_host: str | None = None
    warehouse: str | None = None
    glue_id: str | None = None

    if kind == "rest":
        uri = (environ.get(ENV_REST_URI) or "").strip()
        if not uri:
            raise ValueError(
                f"{ENV_CATALOG}=rest requires {ENV_REST_URI} "
                "(Iceberg REST catalog endpoint)"
            )
        rest_host = urlparse(uri).netloc or uri
        warehouse = (environ.get("DET_ICEBERG_REST_WAREHOUSE") or "").strip() or None
    else:
        if not lake_uri.startswith("s3://"):
            raise ValueError(
                f"{ENV_CATALOG}=glue requires an s3:// lake "
                f"(got {lake_uri!r})"
            )
        glue_id = (environ.get("DET_ICEBERG_GLUE_ID") or "").strip() or None

    return kind, rest_host, warehouse, glue_id


def build_iceberg_register_plan(
    *,
    project_root: Path,
    lake_path: str | None = None,
    pipeline: PipelineConfig | Path | str | None = None,
    include_ops: bool = True,
    env: dict[str, str] | None = None,
) -> IcebergRegisterPlan:
    environ = dict(os.environ if env is None else env)
    root = project_root.resolve()

    config: PipelineConfig | None = None
    if pipeline is not None:
        if isinstance(pipeline, PipelineConfig):
            config = pipeline
        else:
            config = load_pipeline_config(resolve_path(root, str(pipeline)))

    spec = pick_lake_spec(
        cli_lake_path=lake_path,
        destination_path=config.destination.path if config is not None else None,
        env=environ,
    )
    lake = open_lake(spec, root)
    lake_uri = _lake_uri_str(lake)
    kind, rest_host, warehouse, glue_id = _require_register_catalog(environ, lake_uri)

    tables: list[IcebergRegisterTablePlan] = list(_bronze_table_plans(lake, config))
    if include_ops and config is None:
        ops_plan = _ops_table_plan(lake)
        if ops_plan is not None:
            tables.append(ops_plan)

    return IcebergRegisterPlan(
        catalog_kind=kind,
        lake_uri=lake_uri,
        rest_uri_host=rest_host,
        warehouse=warehouse,
        glue_id=glue_id,
        tables=tuple(tables),
    )


def iceberg_register_write_argv(
    *,
    lake_path: str | None = None,
    pipeline: str | None = None,
    skip_ops: bool = False,
) -> list[str]:
    argv = ["iceberg-register", "--apply"]
    if lake_path:
        argv.extend(["--lake-path", lake_path])
    if pipeline:
        argv.extend(["--pipeline", pipeline])
    if skip_ops:
        argv.append("--skip-ops")
    return argv


def approval_plan_for_register(argv: list[str]) -> ApprovalPlan:
    return make_plan("iceberg-register", argv)


def _ensure_namespace(catalog: Any, namespace: str) -> None:
    try:
        catalog.create_namespace(namespace)
    except Exception as exc:
        # AlreadyExists and similar — idempotent.
        name = type(exc).__name__
        if "AlreadyExists" in name or "NamespaceAlreadyExists" in name:
            return
        # Some catalogs treat create as no-op when present.
        if "already" in str(exc).lower():
            return
        raise


def apply_iceberg_register(
    plan: IcebergRegisterPlan,
    *,
    project_root: Path,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Register plan tables into the configured REST/Glue catalog."""
    from pyiceberg.exceptions import NoSuchTableError

    environ = dict(os.environ if env is None else env)
    root = project_root.resolve()
    lake = open_lake(plan.lake_uri, root, env=environ)
    catalog = resolve_iceberg_catalog(lake, env=environ)

    applied: list[dict[str, str]] = []
    for table in plan.tables:
        ident = (table.namespace, table.table)
        status: RegisterStatus
        try:
            catalog.load_table(ident)
            status = "exists"
        except NoSuchTableError:
            _ensure_namespace(catalog, table.namespace)
            logger.info(
                "iceberg register",
                namespace=table.namespace,
                table=table.table,
                metadata_uri=table.metadata_uri,
                catalog_kind=plan.catalog_kind,
            )
            catalog.register_table(ident, table.metadata_uri)
            status = "registered"
        applied.append(
            {
                "namespace": table.namespace,
                "table": table.table,
                "metadata_uri": table.metadata_uri,
                "status": status,
            }
        )

    return {"applied": applied, "count": len(applied)}


def format_dry_run(plan: IcebergRegisterPlan, argv: list[str]) -> str:
    lines = [
        f"DRY-RUN iceberg-register catalog={plan.catalog_kind} "
        f"lake={plan.lake_uri} tables={len(plan.tables)}",
    ]
    if plan.rest_uri_host:
        lines.append(f"  rest_uri_host={plan.rest_uri_host}")
    if plan.warehouse:
        lines.append(f"  warehouse={plan.warehouse}")
    if plan.glue_id:
        lines.append(f"  glue_id={plan.glue_id}")
    for table in plan.tables:
        lines.append(
            f"  {table.namespace}.{table.table} ({table.kind}) "
            f"metadata={table.metadata_uri}"
        )
    lines.append("")
    lines.append("approval_plan:")
    lines.append(json.dumps(approval_plan_for_register(argv).to_dict(), indent=2))
    return "\n".join(lines)
