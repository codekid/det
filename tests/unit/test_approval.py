from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import structlog
from typer.testing import CliRunner

from det.cli import app
from det.logging import configure_logging
from det.runtime.approval import (
    ApprovalError,
    check_approval,
    consume_approval,
    create_approval,
    effective_status,
    list_unused_approvals,
    load_approval,
    make_plan,
    plan_from_mapping,
    prune_write_argv,
)

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


def _create(tmp_path: Path, **kwargs):
    argv = kwargs.pop("argv", prune_write_argv("example_api.events", "2026-08-01"))
    return create_approval(
        tmp_path,
        command=kwargs.pop("command", "prune"),
        argv=argv,
        approved_by=kwargs.pop("approved_by", "tester"),
        now=kwargs.pop("now", NOW),
        **kwargs,
    )


def test_create_load_and_digest(tmp_path: Path):
    argv = prune_write_argv("example_api.events", "2026-08-01", keep=1)
    rec = _create(tmp_path, argv=argv)
    assert rec["id"].startswith("apr_")
    assert len(rec["id"]) == 20
    assert rec["status"] == "unused"
    assert rec["approved_by"] == "tester"
    assert rec["plan_digest"] == make_plan("prune", argv).plan_digest
    loaded = load_approval(tmp_path, rec["id"])
    assert loaded["id"] == rec["id"]
    unused = list_unused_approvals(tmp_path, now=NOW)
    assert [r["id"] for r in unused] == [rec["id"]]


def test_create_requires_approved_by(tmp_path: Path):
    with pytest.raises(ApprovalError) as exc:
        create_approval(
            tmp_path,
            command="prune",
            argv=["prune", "--apply"],
            approved_by="  ",
            now=NOW,
        )
    assert exc.value.code == "approval_identity_required"


def test_expire_derived_at_read(tmp_path: Path):
    rec = _create(tmp_path, ttl_sec=60)
    later = NOW + timedelta(seconds=61)
    loaded = load_approval(tmp_path, rec["id"])
    assert effective_status(loaded, now=later) == "expired"
    assert list_unused_approvals(tmp_path, now=later) == []
    with pytest.raises(ApprovalError) as exc:
        check_approval(
            tmp_path,
            "prune",
            rec["argv"],
            rec["id"],
            require=True,
            now=later,
        )
    assert exc.value.code == "approval_expired"


def test_argv_mismatch(tmp_path: Path):
    rec = _create(tmp_path)
    with pytest.raises(ApprovalError) as exc:
        check_approval(
            tmp_path,
            "prune",
            prune_write_argv("example_api.events", "2026-08-02"),
            rec["id"],
            require=False,
            now=NOW,
        )
    assert exc.value.code == "approval_argv_mismatch"


def test_command_mismatch(tmp_path: Path):
    rec = _create(tmp_path)
    with pytest.raises(ApprovalError) as exc:
        check_approval(
            tmp_path,
            "extract",
            rec["argv"],
            rec["id"],
            require=False,
            now=NOW,
        )
    assert exc.value.code == "approval_command_mismatch"


def test_consume_once(tmp_path: Path):
    rec = _create(tmp_path)
    check_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    consume_approval(tmp_path, rec["id"], now=NOW)
    loaded = load_approval(tmp_path, rec["id"])
    assert loaded["status"] == "consumed"
    assert loaded["consumed_at"]
    with pytest.raises(ApprovalError) as exc:
        check_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert exc.value.code == "approval_consumed"
    with pytest.raises(ApprovalError) as exc:
        consume_approval(tmp_path, rec["id"], now=NOW)
    assert exc.value.code == "approval_consumed"


def test_require_off_skips_without_id(tmp_path: Path):
    assert check_approval(tmp_path, "extract", ["extract"], None, require=False) is None


def test_require_on_without_id_fails(tmp_path: Path):
    with pytest.raises(ApprovalError) as exc:
        check_approval(tmp_path, "extract", ["extract"], None, require=True)
    assert exc.value.code == "approval_required"


def test_missing_id_fails_when_passed(tmp_path: Path):
    with pytest.raises(ApprovalError) as exc:
        check_approval(tmp_path, "prune", ["prune"], "apr_deadbeefdeadbeef", require=False)
    assert exc.value.code == "approval_not_found"


def test_plan_from_mapping_nested_and_digest_check():
    plan = make_plan("prune", prune_write_argv("p.s", "2026-08-01"))
    nested = plan_from_mapping({"approval_plan": plan.to_dict(), "keep": 1})
    assert nested.plan_digest == plan.plan_digest
    with pytest.raises(ApprovalError) as exc:
        plan_from_mapping({"command": "prune", "argv": ["prune"], "plan_digest": "0" * 64})
    assert exc.value.code == "approval_plan_invalid"


def _invoke(args: list[str]):
    runner = CliRunner()
    try:
        return runner.invoke(app, args)
    finally:
        structlog.reset_defaults()
        configure_logging("WARNING")


def _pipe_yaml(tmp_path: Path) -> Path:
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        f"""
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/noaa/storm_events/storm_events.schema.yaml
destination:
  type: filesystem
  path: {tmp_path / "lake"}
""",
        encoding="utf-8",
    )
    return pipeline


def test_cli_approve_show_consume_round_trip(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)
    monkeypatch.delenv("DET_APPROVED_BY", raising=False)
    pipeline = _pipe_yaml(tmp_path)
    argv = prune_write_argv(str(pipeline), "2026-08-01", interval_end="2026-09-01", keep=1)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps({"approval_plan": make_plan("prune", argv).to_dict()}),
        encoding="utf-8",
    )
    created = _invoke(
        [
            "approve",
            "--plan",
            str(plan_path),
            "--approved-by",
            "tester",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert created.exit_code == 0, created.output
    rec = json.loads(created.stdout)
    approval_id = rec["id"]
    shown = _invoke(["approval-show", approval_id, "--project-root", str(tmp_path)])
    assert shown.exit_code == 0, shown.output
    assert json.loads(shown.stdout)["status"] == "unused"
    listed = _invoke(["list-approvals", "--project-root", str(tmp_path)])
    assert listed.exit_code == 0, listed.output
    assert json.loads(listed.stdout)["approvals"][0]["id"] == approval_id

    monkeypatch.setenv("DET_REQUIRE_APPROVAL", "1")
    applied = _invoke(
        [
            "prune",
            "-p",
            str(pipeline),
            "-s",
            "2026-08-01",
            "-e",
            "2026-09-01",
            "--keep",
            "1",
            "--apply",
            "--approval",
            approval_id,
            "--project-root",
            str(tmp_path),
        ]
    )
    assert applied.exit_code == 0, applied.output
    assert "OK prune" in applied.stdout
    shown_after = _invoke(["approval-show", approval_id, "--project-root", str(tmp_path)])
    assert json.loads(shown_after.stdout)["status"] == "consumed"
    again = _invoke(
        [
            "prune",
            "-p",
            str(pipeline),
            "-s",
            "2026-08-01",
            "-e",
            "2026-09-01",
            "--keep",
            "1",
            "--apply",
            "--approval",
            approval_id,
            "--project-root",
            str(tmp_path),
        ]
    )
    assert again.exit_code != 0
    assert "approval_consumed" in again.output


def test_cli_require_approval_without_id(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)
    result = _invoke(
        [
            "extract",
            "-p",
            "noaa.storm_events",
            "-s",
            "2026-08-06",
            "--require-approval",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
    assert "approval_required" in result.output
