"""DET — Data Extract Tool.

Public names are listed in ``__all__`` and follow SemVer. See ``docs/api.md``.
"""

from __future__ import annotations

from det.errors import (
    DetConfigError,
    DetConflictError,
    DetContractError,
    DetError,
    DetNotFoundError,
    DetPluginError,
)
from det.logging import (
    configure_logging,
    drop_secrets,
    get_logger,
    scrub_rendered,
    scrub_secrets,
)
from det.runtime.check import (
    Finding,
    check_pipeline_config,
    check_project,
    findings_payload,
    has_errors,
    has_warnings,
)
from det.runtime.config import PipelineConfig, load_pipeline, load_pipeline_config
from det.runtime.lake import LakeRef, LakeRoots, open_lake, resolve_lake_roots
from det.runtime.layout import LAKE_LAYOUT
from det.runtime.lease import (
    Lease,
    LeaseFencedError,
    LeaseHeldError,
    inspect_lease,
    release_lock,
)
from det.runtime.mappers import identity_mapper
from det.runtime.migrate import (
    BronzeMigrator,
    MigratePlan,
    MigrateResult,
    PartitionPlan,
)
from det.runtime.object_store import configure_duckdb_s3
from det.runtime.prune import BronzePruner, BronzeRunRef, PrunePlan
from det.runtime.receipts import list_receipts, summarize_receipts
from det.runtime.registry import describe_mappers, list_mappers, list_sources
from det.runtime.runner import ExtractResult, PipelineRunner, RunResult
from det.runtime.settings import DetSettings
from det.sources.base import (
    Interval,
    SourcePlugin,
    SourceRow,
    mapper,
    merge_source_config,
)

__version__ = "0.4.0"

__all__ = [
    "LAKE_LAYOUT",
    "BronzeMigrator",
    "BronzePruner",
    "BronzeRunRef",
    "DetConfigError",
    "DetConflictError",
    "DetContractError",
    "DetError",
    "DetNotFoundError",
    "DetPluginError",
    "DetSettings",
    "ExtractResult",
    "Finding",
    "Interval",
    "LakeRef",
    "LakeRoots",
    "Lease",
    "LeaseFencedError",
    "LeaseHeldError",
    "MigratePlan",
    "MigrateResult",
    "PartitionPlan",
    "PipelineConfig",
    "PipelineRunner",
    "PrunePlan",
    "RunResult",
    "SourcePlugin",
    "SourceRow",
    "__version__",
    "check_pipeline_config",
    "check_project",
    "configure_duckdb_s3",
    "configure_logging",
    "describe_mappers",
    "drop_secrets",
    "findings_payload",
    "get_logger",
    "has_errors",
    "has_warnings",
    "identity_mapper",
    "inspect_lease",
    "list_mappers",
    "list_receipts",
    "list_sources",
    "load_pipeline",
    "load_pipeline_config",
    "mapper",
    "merge_source_config",
    "open_lake",
    "release_lock",
    "resolve_lake_roots",
    "scrub_rendered",
    "scrub_secrets",
    "summarize_receipts",
]
