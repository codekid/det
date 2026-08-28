from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from det.runtime.config import DestinationConfig, PipelineConfig
from det.runtime.lake import LakeRef

LakePath = Path | LakeRef


class IngestionBackend(Protocol):
    name: str

    def write(
        self,
        records: Iterable[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        partition_dir: LakePath,
        destination: DestinationConfig,
        chunk_rows: int = 10_000,
        run_identity: tuple[str, str, str] | None = None,
        on_chunk: Callable[[], None] | None = None,
    ) -> LakePath:
        """
        Persist records into the bronze hive partition (replace semantics).

        ``run_identity`` is ``(interval_start, interval_end, extract_run)`` from
        the runner. Required for empty SQL/Iceberg writes so replace-by-run still
        deletes the prior slice.
        """
        ...
