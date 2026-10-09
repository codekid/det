"""Plan Iceberg table-property reconcile + maintain knobs for external runners.

DET does not expire/compact or mutate live properties. Embedders and the
operator Airflow reference DAG call :func:`iter_iceberg_maintain_plans` and
submit Spark SQL (see :func:`render_iceberg_maintain_spark_sql`) themselves.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

from det.ingestion.iceberg_catalog_factory import (
    IcebergCatalogKind,
    catalog_kind_from_env,
)
from det.runtime.config import (
    IcebergMaintainConfig,
    PipelineConfig,
    load_pipeline_config,
)
from det.runtime.ids import sql_names_for_config
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref

ENV_EXPIRE_OLDER_THAN = "DET_ICEBERG_MAINTAIN_EXPIRE_OLDER_THAN"
ENV_EXPIRE = "DET_ICEBERG_MAINTAIN_EXPIRE"
ENV_REWRITE_DATA = "DET_ICEBERG_MAINTAIN_REWRITE_DATA"
ENV_REWRITE_MANIFESTS = "DET_ICEBERG_MAINTAIN_REWRITE_MANIFESTS"
ENV_REMOVE_ORPHANS_OLDER_THAN = "DET_ICEBERG_MAINTAIN_REMOVE_ORPHANS_OLDER_THAN"
ENV_SPARK_CATALOG = "DET_ICEBERG_SPARK_CATALOG"
DEFAULT_SPARK_CATALOG = "iceberg"

_DURATION = re.compile(r"^(\d+)([HhDd])$")
_CATALOG_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

__all__ = [
    "DEFAULT_SPARK_CATALOG",
    "ENV_EXPIRE",
    "ENV_EXPIRE_OLDER_THAN",
    "ENV_REMOVE_ORPHANS_OLDER_THAN",
    "ENV_REWRITE_DATA",
    "ENV_REWRITE_MANIFESTS",
    "ENV_SPARK_CATALOG",
    "IcebergMaintainPlan",
    "fleet_maintain_defaults",
    "iter_iceberg_maintain_plans",
    "render_iceberg_maintain_spark_sql",
    "resolve_maintain_config",
]


@dataclass(frozen=True)
class IcebergMaintainPlan:
    """One pipeline's maintain/reconcile intent for an external runner.

    ``table_properties`` and ``maintain`` are independent snapshots: mutating a
    returned plan (or its nested lists) cannot affect fleet defaults or other
    plans.
    """

    pipeline: str
    sql_schema: str
    sql_table: str
    table_properties: Mapping[str, str] = field(default_factory=dict)
    maintain: IcebergMaintainConfig = field(default_factory=IcebergMaintainConfig)
    catalog_kind: IcebergCatalogKind = "hadoop"
    actionable: bool = False
    skip_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "table_properties",
            MappingProxyType(dict(self.table_properties)),
        )
        object.__setattr__(self, "maintain", self.maintain.model_copy(deep=True))

    @property
    def sql_relation(self) -> str:
        return f"{self.sql_schema}.{self.sql_table}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "sql_schema": self.sql_schema,
            "sql_table": self.sql_table,
            "sql_relation": self.sql_relation,
            "table_properties": dict(self.table_properties),
            "maintain": self.maintain.model_dump(),
            "catalog_kind": self.catalog_kind,
            "actionable": self.actionable,
            "skip_reason": self.skip_reason,
        }


def _env_bool(raw: str | None, *, default: bool) -> bool:
    if raw is None or not str(raw).strip():
        return default
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"expected boolean env value, got {raw!r}")


def fleet_maintain_defaults(
    environ: Mapping[str, str] | None = None,
) -> IcebergMaintainConfig:
    """Fleet defaults from ``DET_ICEBERG_MAINTAIN_*`` env (else YAML defaults)."""
    env = os.environ if environ is None else environ
    base = IcebergMaintainConfig()
    expire_older = env.get(ENV_EXPIRE_OLDER_THAN)
    remove_orphans = env.get(ENV_REMOVE_ORPHANS_OLDER_THAN)
    return IcebergMaintainConfig(
        expire_older_than=(
            expire_older.strip()
            if expire_older is not None and str(expire_older).strip()
            else base.expire_older_than
        ),
        expire=_env_bool(env.get(ENV_EXPIRE), default=base.expire),
        rewrite_data=_env_bool(env.get(ENV_REWRITE_DATA), default=base.rewrite_data),
        rewrite_manifests=_env_bool(
            env.get(ENV_REWRITE_MANIFESTS), default=base.rewrite_manifests
        ),
        remove_orphans_older_than=(
            remove_orphans.strip()
            if remove_orphans is not None and str(remove_orphans).strip()
            else base.remove_orphans_older_than
        ),
        z_order=list(base.z_order),
    )


def resolve_maintain_config(
    config: PipelineConfig,
    *,
    fleet: IcebergMaintainConfig | None = None,
) -> IcebergMaintainConfig:
    """Merge per-pipeline ``destination.iceberg.maintain`` over fleet defaults.

    When the pipeline omits ``maintain``, return a deep copy of fleet defaults.
    When present, overlay only fields listed in ``model_fields_set`` so omitted
    keys keep fleet values (not Pydantic field defaults). Always return a deep
    copy so callers cannot mutate shared config objects.
    """
    defaults = fleet if fleet is not None else IcebergMaintainConfig()
    block = config.destination.iceberg
    if block is None or block.maintain is None:
        return defaults.model_copy(deep=True)
    override = block.maintain
    patch: dict[str, Any] = {}
    for name in override.model_fields_set:
        value = getattr(override, name)
        if isinstance(value, list):
            value = list(value)
        patch[name] = value
    return defaults.model_copy(update=patch, deep=True)


def iter_iceberg_maintain_plans(
    project_root: Path,
    *,
    environ: Mapping[str, str] | None = None,
    fleet_defaults: IcebergMaintainConfig | None = None,
) -> Iterator[IcebergMaintainPlan]:
    """Yield maintain plans for every ``destination.type: iceberg`` pipeline.

    Pure planning — no catalog network I/O. When ``DET_ICEBERG_CATALOG`` is
    unset or ``hadoop``, plans are marked ``actionable=False`` (Spark/Athena
    procedures target ``rest`` / ``glue``).
    """
    root = project_root.resolve()
    env = os.environ if environ is None else environ
    kind = catalog_kind_from_env(env)
    fleet = (
        fleet_defaults
        if fleet_defaults is not None
        else fleet_maintain_defaults(env)
    )
    actionable = kind in {"rest", "glue"}
    skip_reason = (
        None
        if actionable
        else (
            f"DET_ICEBERG_CATALOG={kind!r}; maintain procedures require rest|glue"
        )
    )

    for pipe_id in list_pipeline_ids(root):
        resolved = resolve_pipeline_ref(pipe_id, project_root=root)
        config = load_pipeline_config(resolved.path)
        if config.destination.type != "iceberg":
            continue
        schema, table = sql_names_for_config(config)
        props = dict(config.destination.iceberg_table_properties)
        maintain = resolve_maintain_config(config, fleet=fleet)
        yield IcebergMaintainPlan(
            pipeline=config.name,
            sql_schema=schema,
            sql_table=table,
            table_properties=props,
            maintain=maintain,
            catalog_kind=kind,
            actionable=actionable,
            skip_reason=skip_reason,
        )


def _spark_catalog(catalog: str | None, environ: Mapping[str, str] | None) -> str:
    env = os.environ if environ is None else environ
    raw = (catalog if catalog is not None else env.get(ENV_SPARK_CATALOG) or "").strip()
    name = raw or DEFAULT_SPARK_CATALOG
    if not _CATALOG_IDENT.fullmatch(name):
        raise ValueError(
            f"Spark catalog must be a simple SQL identifier, got {name!r} "
            f"(set {ENV_SPARK_CATALOG} or pass catalog=)"
        )
    return name


def _duration_to_spark_interval(duration: str) -> str:
    """Map DET ``Nh`` / ``Nd`` to a Spark interval expression."""
    text = str(duration).strip()
    match = _DURATION.fullmatch(text)
    if not match:
        raise ValueError(f"maintain duration must look like '48h' or '7d', got {duration!r}")
    amount = int(match.group(1))
    if amount < 1:
        raise ValueError(f"maintain duration must be >= 1, got {duration!r}")
    unit = "HOURS" if match.group(2).upper() == "H" else "DAYS"
    return f"INTERVAL {amount} {unit}"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _plan_from_mapping(plan: Mapping[str, Any]) -> IcebergMaintainPlan:
    maintain_raw = plan.get("maintain") or {}
    if isinstance(maintain_raw, IcebergMaintainConfig):
        maintain = maintain_raw
    elif isinstance(maintain_raw, Mapping):
        maintain = IcebergMaintainConfig.model_validate(dict(maintain_raw))
    else:
        raise TypeError(f"plan['maintain'] must be a mapping, got {type(maintain_raw)}")
    props = plan.get("table_properties") or {}
    if not isinstance(props, Mapping):
        raise TypeError(f"plan['table_properties'] must be a mapping, got {type(props)}")
    schema = str(plan.get("sql_schema") or "").strip()
    table = str(plan.get("sql_table") or "").strip()
    if not schema or not table:
        raise ValueError("plan requires non-empty sql_schema and sql_table")
    kind_raw = plan.get("catalog_kind") or "hadoop"
    kind: IcebergCatalogKind
    if kind_raw in {"hadoop", "rest", "glue"}:
        kind = kind_raw  # type: ignore[assignment]
    else:
        raise ValueError(f"unsupported catalog_kind {kind_raw!r}")
    return IcebergMaintainPlan(
        pipeline=str(plan.get("pipeline") or ""),
        sql_schema=schema,
        sql_table=table,
        table_properties={str(k): str(v) for k, v in props.items()},
        maintain=maintain,
        catalog_kind=kind,
        actionable=bool(plan.get("actionable")),
        skip_reason=(
            None
            if plan.get("skip_reason") is None
            else str(plan.get("skip_reason"))
        ),
    )


def render_iceberg_maintain_spark_sql(
    plan: IcebergMaintainPlan | Mapping[str, Any],
    *,
    catalog: str | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[str]:
    """Render Apache Spark Iceberg procedure SQL for one maintain plan.

    Pure string generation — does not open a Spark session. Non-actionable plans
    (``hadoop`` / unset catalog) raise ``ValueError`` with ``skip_reason``.
    Athena/Trino callers should adapt the statements.
    """
    resolved = plan if isinstance(plan, IcebergMaintainPlan) else _plan_from_mapping(plan)
    if not resolved.actionable:
        reason = resolved.skip_reason or "plan is not actionable"
        raise ValueError(reason)

    cat = _spark_catalog(catalog, environ)
    table_ref = f"{resolved.sql_schema}.{resolved.sql_table}"
    qualified = f"{cat}.{table_ref}"
    maintain = resolved.maintain
    stmts: list[str] = []

    if resolved.table_properties:
        props_sql = ", ".join(
            f"{_sql_string(key)}={_sql_string(value)}"
            for key, value in sorted(resolved.table_properties.items())
        )
        stmts.append(f"ALTER TABLE {qualified} SET TBLPROPERTIES ({props_sql})")

    if maintain.expire and maintain.expire_older_than:
        interval = _duration_to_spark_interval(maintain.expire_older_than)
        stmts.append(
            f"CALL {cat}.system.expire_snapshots("
            f"table => {_sql_string(table_ref)}, "
            f"older_than => current_timestamp() - {interval})"
        )

    if maintain.rewrite_data:
        if maintain.z_order:
            for col in maintain.z_order:
                if not _CATALOG_IDENT.fullmatch(str(col)):
                    raise ValueError(
                        f"maintain.z_order entries must be simple SQL identifiers, "
                        f"got {col!r}"
                    )
            cols = ", ".join(maintain.z_order)
            stmts.append(
                f"CALL {cat}.system.rewrite_data_files("
                f"table => {_sql_string(table_ref)}, "
                f"strategy => {_sql_string('sort')}, "
                f"sort_order => {_sql_string(f'zorder({cols})')})"
            )
        else:
            stmts.append(
                f"CALL {cat}.system.rewrite_data_files(table => {_sql_string(table_ref)})"
            )

    if maintain.rewrite_manifests:
        stmts.append(
            f"CALL {cat}.system.rewrite_manifests(table => {_sql_string(table_ref)})"
        )

    if maintain.remove_orphans_older_than:
        interval = _duration_to_spark_interval(maintain.remove_orphans_older_than)
        stmts.append(
            f"CALL {cat}.system.remove_orphan_files("
            f"table => {_sql_string(table_ref)}, "
            f"older_than => current_timestamp() - {interval})"
        )

    return stmts
