from __future__ import annotations

import json
from pathlib import Path

import pytest

from det.ingestion.jsonl import write_jsonl_partition


def test_write_jsonl_streams_generator(tmp_path: Path):
    def rows():
        for i in range(5):
            yield {"id": i}

    partition = tmp_path / "part"
    write_jsonl_partition(rows(), partition, chunk_rows=2)
    lines = (partition / "data.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in lines] == list(range(5))


def test_write_jsonl_raising_generator_removes_partition(tmp_path: Path):
    def rows():
        yield {"id": 1}
        raise RuntimeError("boom")

    partition = tmp_path / "part"
    with pytest.raises(RuntimeError, match="boom"):
        write_jsonl_partition(rows(), partition, chunk_rows=1)
    assert not partition.exists()
