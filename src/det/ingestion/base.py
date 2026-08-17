from __future__ import annotations

from collections.abc import Iterable
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
    ) -> LakePath:
        """
        Persist records into the bronze hive partition (replace semantics).
        Returns the partition directory written.
        """
        ...
