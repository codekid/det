from __future__ import annotations

from pathlib import Path
from typing import Any

from det.ingestion.jsonl import write_jsonl_partition
from det.logging import get_logger
from det.runtime.config import DestinationConfig, PipelineConfig

logger = get_logger(__name__)


class ThinBackend:
    """
    Non-dlt ingestion backend (Option A proof): write JSONL into the hive partition.
    """

    name = "thin"

    def write(
        self,
        records: list[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        partition_dir: Path,
        destination: DestinationConfig,
    ) -> Path:
        if destination.type != "filesystem":
            raise ValueError(
                f"thin backend only supports filesystem destination, got {destination.type}"
            )
        out = write_jsonl_partition(records, partition_dir)
        logger.info(
            "thin backend wrote partition",
            path=str(out),
            rows=len(records),
            pipeline=config.name,
        )
        return partition_dir
