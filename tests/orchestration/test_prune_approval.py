"""Airflow prune-apply approval helpers (no live Airflow)."""

from __future__ import annotations

from pathlib import Path

import pytest

from det.runtime.approval import create_approval, load_approval, prune_write_argv


def _load_det_env(project_root: Path):
    import importlib
    import sys

    dags = str(project_root / "dags")
    if dags not in sys.path:
        sys.path.insert(0, dags)
    if "det_env" in sys.modules:
        del sys.modules["det_env"]
    return importlib.import_module("det_env")


def test_approval_id_from_conf_keys(project_root: Path):
    det_env = _load_det_env(project_root)
    assert det_env.approval_id_from_conf(None) is None
    assert det_env.approval_id_from_conf({}) is None
    assert det_env.approval_id_from_conf({"approval": "  apr_abc  "}) == "apr_abc"
    assert det_env.approval_id_from_conf({"approval_id": "apr_xyz"}) == "apr_xyz"
    assert (
        det_env.approval_id_from_conf({"approval": "apr_a", "approval_id": "apr_b"})
        == "apr_a"
    )
    assert det_env.approval_id_from_conf({"approval": "  "}) is None


def test_merge_dag_conf_conf_wins(project_root: Path):
    det_env = _load_det_env(project_root)
    merged = det_env.merge_dag_conf(
        {"approval": "apr_from_conf", "keep": 2},
        {"approval": "apr_from_params", "other": 1},
    )
    assert merged["approval"] == "apr_from_conf"
    assert merged["other"] == 1
    assert merged["keep"] == 2


def test_gate_prune_apply_requires_id(project_root: Path, tmp_path: Path):
    det_env = _load_det_env(project_root)
    with pytest.raises(ValueError, match="approval_required"):
        det_env.gate_prune_apply_approval(
            tmp_path,
            pipeline="example_api.events",
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            keep=1,
            approval_id=None,
        )


def test_gate_and_consume_prune_approval_round_trip(project_root: Path, tmp_path: Path):
    det_env = _load_det_env(project_root)
    argv = prune_write_argv(
        "example_api.events",
        "2026-08-06",
        interval_end="2026-08-07",
        keep=1,
    )
    rec = create_approval(
        tmp_path,
        command="prune",
        argv=argv,
        approved_by="tester",
    )
    det_env.gate_prune_apply_approval(
        tmp_path,
        pipeline="example_api.events",
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        keep=1,
        approval_id=rec["id"],
    )
    det_env.consume_prune_approval(tmp_path, rec["id"])
    assert load_approval(tmp_path, rec["id"])["status"] == "consumed"
    with pytest.raises(ValueError, match="approval_consumed"):
        det_env.gate_prune_apply_approval(
            tmp_path,
            pipeline="example_api.events",
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            keep=1,
            approval_id=rec["id"],
        )


def test_gate_prune_apply_argv_mismatch(project_root: Path, tmp_path: Path):
    det_env = _load_det_env(project_root)
    argv = prune_write_argv(
        "example_api.events",
        "2026-08-06",
        interval_end="2026-08-07",
        keep=1,
    )
    rec = create_approval(
        tmp_path,
        command="prune",
        argv=argv,
        approved_by="tester",
    )
    with pytest.raises(ValueError, match="approval_argv_mismatch"):
        det_env.gate_prune_apply_approval(
            tmp_path,
            pipeline="example_api.events",
            interval_start="2026-08-06",
            interval_end="2026-08-07",
            keep=2,
            approval_id=rec["id"],
        )
