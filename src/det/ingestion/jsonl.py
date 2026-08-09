from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any


def write_jsonl_partition(records: list[dict[str, Any]], partition_dir: Path) -> Path:
    """
    Replace `partition_dir` with a single data.jsonl holding `records`, and return it.

    DET writes bronze itself rather than handing rows to a dlt pipeline. dlt's
    normalizer would reshape nested fields and strip the leading __ from meta
    columns, and its pipeline would persist load/version/state files next to the
    data. Bronze must stay byte-faithful to the contract, and state belongs to DET.
    """
    if partition_dir.exists():
        shutil.rmtree(partition_dir)
    partition_dir.mkdir(parents=True, exist_ok=True)
    out = partition_dir / "data.jsonl"
    with out.open("w", encoding="utf-8") as f:
        for row in records:
            f.write(json.dumps(row, default=str) + "\n")
    return out
