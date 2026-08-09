from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from det.runtime.config import DestinationConfig, PipelineConfig


class IngestionBackend(Protocol):
    name: str

    def write(
        self,
        records: list[dict[str, Any]],
        *,
        config: PipelineConfig,
        project_root: Path,
        partition_dir: Path,
        destination: DestinationConfig,
    ) -> Path:
        """
        Persist records into the bronze hive partition (replace semantics).
        Returns the partition directory written.
        """
        ...
