from __future__ import annotations

from pathlib import Path

import pytest

from det.mcp.context import PathSandboxError, project_root, resolve_under_root


def test_resolve_relative_under_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DET_PROJECT_ROOT", str(tmp_path))
    (tmp_path / "configs").mkdir()
    resolved = resolve_under_root("configs/pipelines")
    assert resolved == (tmp_path / "configs" / "pipelines").resolve()
    assert project_root() == tmp_path.resolve()


def test_resolve_absolute_under_root_ok(tmp_path: Path):
    target = (tmp_path / "data" / "lake").resolve()
    target.mkdir(parents=True)
    assert resolve_under_root(target, root=tmp_path) == target


def test_resolve_rejects_escape(tmp_path: Path):
    outside = tmp_path.parent / "outside-escape"
    with pytest.raises(PathSandboxError, match="escapes project root"):
        resolve_under_root(outside, root=tmp_path)


def test_resolve_rejects_dotdot_escape(tmp_path: Path):
    with pytest.raises(PathSandboxError, match="escapes project root"):
        resolve_under_root("../outside", root=tmp_path)
