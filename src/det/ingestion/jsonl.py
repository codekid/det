from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from det.logging import get_logger
from det.runtime.lake import LakeRef

logger = get_logger(__name__)

LakePath = Path | LakeRef


def write_jsonl_partition(
    records: Iterable[dict[str, Any]],
    partition_dir: LakePath,
    *,
    chunk_rows: int = 10_000,
    on_chunk: Callable[[], None] | None = None,
) -> LakePath:
    """
    Replace `partition_dir` with a single data.jsonl streamed from *records*.

    Writes the final path in place (no temp-then-copy). Flushes every
    ``chunk_rows`` lines. On error the partition dir is removed so load does
    not see a partial file.

    DET writes bronze itself rather than handing rows to a dlt pipeline. dlt's
    normalizer would reshape nested fields and strip the leading __ from meta
    columns, and its pipeline would persist load/version/state files next to the
    data. Bronze must stay byte-faithful to the contract, and state belongs to DET.
    """
    if chunk_rows < 1:
        raise ValueError("chunk_rows must be >= 1")
    if partition_dir.exists():
        _rmtree(partition_dir)
    partition_dir.mkdir(parents=True, exist_ok=True)
    out = partition_dir / "data.jsonl"
    n = 0
    try:
        with out.open("w", encoding="utf-8") as f:
            for row in records:
                f.write(json.dumps(row, default=str) + "\n")
                n += 1
                if n % chunk_rows == 0:
                    f.flush()
                    if on_chunk is not None:
                        on_chunk()
    except Exception:
        _rmtree(partition_dir, ignore_errors=True)
        raise
    logger.info("jsonl partition written", path=str(out), rows=n)
    return out


def _rmtree(path: LakePath, ignore_errors: bool = False) -> None:
    if isinstance(path, LakeRef):
        path.rmtree(ignore_errors=ignore_errors)
        return
    import shutil

    shutil.rmtree(path, ignore_errors=ignore_errors)
