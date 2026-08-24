#!/usr/bin/env python3
"""Smoke: base install (no extras) can import det and land filesystem bronze."""

from __future__ import annotations

import importlib.resources
import sys
import tempfile
from pathlib import Path


def main() -> int:
    import det
    from det.ingestion.jsonl import write_jsonl_partition

    typed = importlib.resources.files("det").joinpath("py.typed")
    if not typed.is_file():
        print("py.typed missing from package", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory() as tmp:
        partition = Path(tmp) / "bronze" / "run"
        partition.mkdir(parents=True)
        path = write_jsonl_partition(
            [{"id": 1, "name": "minimal"}],
            partition,
            chunk_rows=10,
        )
        if not path.is_file() or path.stat().st_size == 0:
            print(f"expected non-empty JSONL at {path}", file=sys.stderr)
            return 1

    print(f"ok: det {det.__version__}; filesystem bronze smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
