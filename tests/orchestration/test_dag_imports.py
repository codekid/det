from __future__ import annotations

import ast
from pathlib import Path


def test_dag_files_parse(project_root: Path):
    dags = list((project_root / "dags").glob("*.py"))
    assert dags
    for path in dags:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        assert tree is not None
