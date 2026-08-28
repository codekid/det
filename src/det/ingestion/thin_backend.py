from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from det.ingestion.jsonl import write_jsonl_partition
from det.logging import get_logger
from det.runtime.config import DestinationConfig, PipelineConfig
from det.runtime.lake import LakeRef

logger = get_logger(__name__)


class ThinBackend:
    """
    Filesystem-only bronze writer (proof / migrate tests).

    Prefer ``DetBackend`` (``library: det``) for real pipelines — it covers
    filesystem, DuckDB, and Postgres. Keep ``thin`` when you explicitly want a
    minimal JSONL-only path.
    """

    name = "thin"

    def write(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        partition_dir: Path | LakeRef,
        destination: DestinationConfig,
        chunk_rows: int | None = None,
        run_identity: tuple[str, str, str] | None = None,
        on_chunk: Callable[[], None] | None = None,
    ) -> Path | LakeRef:
        if destination.type != "filesystem":
            raise ValueError(
                f"thin backend only supports filesystem destination, got {destination.type}"
            )
        size = config.ingestion.chunk_rows if chunk_rows is None else chunk_rows
        write_jsonl_partition(
            records, partition_dir, chunk_rows=size, on_chunk=on_chunk
        )
        logger.info(
            "thin backend wrote partition",
            path=str(partition_dir),
            pipeline=config.name,
        )
        return partition_dir
