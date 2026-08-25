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
    claim_approval,
    consume_approval,
    create_approval,
    dbt_write_argv,
    effective_status,
    extract_write_argv,
    list_approval_records,
    list_unused_approvals,
    load_approval,
    make_plan,
    migrate_write_argv,
    plan_from_mapping,
    prune_write_argv,
    release_approval,
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
    """Write a config where it resolves to the canonical id ``noaa.storm_events``."""
    pipeline = tmp_path / "configs" / "pipelines" / "noaa" / "storm_events.yaml"
    pipeline.parent.mkdir(parents=True, exist_ok=True)
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
    # Approve by canonical id and bare date; invoke by YAML path. Both surfaces
    # normalize to the same argv, so the digest still matches.
    argv = prune_write_argv(
        "noaa.storm_events", "2026-08-01", interval_end="2026-09-01", keep=1
    )
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
    pipeline = _pipe_yaml(tmp_path)
    result = _invoke(
        [
            "extract",
            "-p",
            str(pipeline),
            "-s",
            "2026-08-06",
            "--require-approval",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
    assert "approval_required" in result.output


def test_cli_unresolvable_pipeline_errors_before_approval_gate(tmp_path: Path, monkeypatch):
    """Resolution now precedes gating, so a bad ref reports the real problem."""
    monkeypatch.delenv("DET_REQUIRE_APPROVAL", raising=False)
    result = _invoke(
        [
            "extract",
            "-p",
            "nope.missing",
            "-s",
            "2026-08-06",
            "--require-approval",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
    assert "approval_required" not in result.output


def test_cli_lake_path_under_valid_approval_is_rejected(tmp_path: Path, monkeypatch):
    """--lake-path redirects where data lands, so it must be inside the digest."""
    monkeypatch.delenv("DET_APPROVED_BY", raising=False)
    pipeline = _pipe_yaml(tmp_path)
    argv = prune_write_argv("noaa.storm_events", "2026-08-01", interval_end="2026-09-01", keep=1)
    rec = create_approval(
        tmp_path, command="prune", argv=argv, approved_by="tester", now=None
    )
    monkeypatch.setenv("DET_REQUIRE_APPROVAL", "1")
    result = _invoke(
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
            "--set",
            "destination.path=/tmp/elsewhere",
            "--approval",
            rec["id"],
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
    assert "approval_argv_mismatch" in result.output


def test_cli_unbound_flag_is_rejected_fail_closed(tmp_path: Path, monkeypatch):
    """A flag in neither the bound nor neutral table is rejected under an approval.

    This is the property pure argv binding cannot provide: a flag added to the CLI
    later cannot silently escape plan_digest.
    """
    from det.cli import common

    monkeypatch.delenv("DET_APPROVED_BY", raising=False)
    # Simulate a newly added flag by dropping --keep from prune's bound set.
    monkeypatch.setitem(
        common._BOUND_PARAMS,
        "prune",
        common._BOUND_PARAMS["prune"] - {"keep"},
    )
    pipeline = _pipe_yaml(tmp_path)
    argv = prune_write_argv("noaa.storm_events", "2026-08-01", interval_end="2026-09-01", keep=1)
    rec = create_approval(tmp_path, command="prune", argv=argv, approved_by="tester", now=None)
    monkeypatch.setenv("DET_REQUIRE_APPROVAL", "1")
    result = _invoke(
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
            rec["id"],
            "--project-root",
            str(tmp_path),
        ]
    )
    assert result.exit_code != 0
    assert "approval_unbound_flag" in result.output
    assert "--keep" in result.output


def test_builders_normalize_interval_to_same_digest():
    """A bare date and its ISO-UTC equivalent must not diverge."""
    bare = extract_write_argv("noaa.storm_events", "2026-08-06")
    iso = extract_write_argv("noaa.storm_events", "2026-08-06T00:00:00+00:00")
    offset = extract_write_argv("noaa.storm_events", "2026-08-06T02:00:00+02:00")
    assert bare == iso == offset
    assert make_plan("extract", bare).plan_digest == make_plan("extract", offset).plan_digest


def test_builders_reject_unresolved_pipeline_refs():
    """Path/slash refs must fail loudly rather than yield a divergent digest."""
    for bad in (
        "noaa/storm_events",
        "configs/pipelines/noaa/storm_events.yaml",
        "/tmp/pipe.yaml",
        "pipe.yaml",
        "NOAA.Storm",
        "",
    ):
        with pytest.raises(ValueError, match="resolved pipeline id"):
            extract_write_argv(bad, "2026-08-06")


def test_builders_accept_a_bare_stem_identity():
    """Flat or out-of-tree configs resolve to a stem, which is still stable."""
    argv = extract_write_argv("pipe", "2026-08-06")
    assert argv[:3] == ["extract", "-p", "pipe"]


def test_mutating_flags_change_the_digest():
    base = extract_write_argv("noaa.storm_events", "2026-08-06")
    redirected = extract_write_argv("noaa.storm_events", "2026-08-06", lake_path="s3://other")
    overridden = extract_write_argv("noaa.storm_events", "2026-08-06", set_=["destination.path=/x"])
    digests = {
        make_plan("extract", argv).plan_digest for argv in (base, redirected, overridden)
    }
    assert len(digests) == 3


def test_set_overrides_are_order_independent():
    a = extract_write_argv("noaa.storm_events", "2026-08-06", set_=["b=2", "a=1"])
    b = extract_write_argv("noaa.storm_events", "2026-08-06", set_=["a=1", "b=2"])
    assert a == b


def test_dbt_full_refresh_and_target_are_bound():
    base = dbt_write_argv("noaa.storm_events")
    refreshed = dbt_write_argv("noaa.storm_events", full_refresh=True)
    retargeted = dbt_write_argv("noaa.storm_events", target="prod")
    assert "--full-refresh" in refreshed
    assert ["--target", "prod"] == retargeted[-2:]
    digests = {make_plan("dbt", argv).plan_digest for argv in (base, refreshed, retargeted)}
    assert len(digests) == 3


def test_claim_is_exclusive(tmp_path: Path):
    """Two concurrent claims on one approval: exactly one wins."""
    rec = _create(tmp_path)
    first = claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert first is not None
    assert first["status"] == "claimed"
    assert first["claimed_by"]
    assert load_approval(tmp_path, rec["id"])["status"] == "claimed"
    with pytest.raises(ApprovalError) as exc:
        claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert exc.value.code == "approval_in_flight"


def test_check_rejects_a_claimed_approval(tmp_path: Path):
    rec = _create(tmp_path)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    with pytest.raises(ApprovalError) as exc:
        check_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert exc.value.code == "approval_in_flight"


def test_claim_then_consume_round_trip(tmp_path: Path):
    rec = _create(tmp_path)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    consume_approval(tmp_path, rec["id"], now=NOW)
    assert load_approval(tmp_path, rec["id"])["status"] == "consumed"


def test_consume_still_accepts_an_unclaimed_record(tmp_path: Path):
    """The Airflow check-then-consume path does not claim, so it must still work."""
    rec = _create(tmp_path)
    check_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    consume_approval(tmp_path, rec["id"], now=NOW)
    assert load_approval(tmp_path, rec["id"])["status"] == "consumed"


def test_claim_survives_ttl_expiry(tmp_path: Path):
    """TTL gates claiming, not finishing: a long write must still consume."""
    rec = _create(tmp_path, ttl_sec=60)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    later = NOW + timedelta(seconds=3600)
    assert effective_status(load_approval(tmp_path, rec["id"]), now=later) == "claimed"
    consume_approval(tmp_path, rec["id"], now=later)
    assert load_approval(tmp_path, rec["id"])["status"] == "consumed"


def test_claimed_record_is_not_listed_as_unused(tmp_path: Path):
    rec = _create(tmp_path)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert list_unused_approvals(tmp_path, now=NOW) == []


def test_expired_record_cannot_be_claimed(tmp_path: Path):
    rec = _create(tmp_path, ttl_sec=60)
    with pytest.raises(ApprovalError) as exc:
        claim_approval(
            tmp_path,
            "prune",
            rec["argv"],
            rec["id"],
            require=True,
            now=NOW + timedelta(seconds=61),
        )
    assert exc.value.code == "approval_expired"


def test_claim_without_id_is_a_noop_when_not_required(tmp_path: Path):
    assert claim_approval(tmp_path, "extract", ["extract"], None, require=False) is None


def test_release_returns_a_claimed_approval_to_unused(tmp_path: Path):
    rec = _create(tmp_path)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    released = release_approval(tmp_path, rec["id"], released_by="operator", now=NOW)
    assert released["status"] == "unused"
    assert released["released_by"] == "operator"
    # The torn-down claim is preserved; the point of releasing is the audit trail.
    assert released["released_from_claim"]["claimed_by"]
    assert "claimed_by" not in released
    assert effective_status(load_approval(tmp_path, rec["id"]), now=NOW) == "unused"


def test_release_allows_exactly_one_more_claim(tmp_path: Path):
    rec = _create(tmp_path)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    release_approval(tmp_path, rec["id"], released_by="operator", now=NOW)
    again = claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert again is not None
    assert again["status"] == "claimed"
    # Still exclusive afterwards: release is one retry, not an unlock.
    with pytest.raises(ApprovalError) as exc:
        claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert exc.value.code == "approval_in_flight"


def test_release_is_not_a_ttl_bypass(tmp_path: Path):
    """An approval that expired while claimed stays dead after release."""
    rec = _create(tmp_path, ttl_sec=60)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    later = NOW + timedelta(seconds=3600)
    released = release_approval(tmp_path, rec["id"], released_by="operator", now=later)
    assert released["expires_at"] == rec["expires_at"]
    assert effective_status(load_approval(tmp_path, rec["id"]), now=later) == "expired"
    with pytest.raises(ApprovalError) as exc:
        claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=later)
    assert exc.value.code == "approval_expired"


@pytest.mark.parametrize("bad_state", ["unused", "consumed"])
def test_release_rejects_anything_but_claimed(tmp_path: Path, bad_state: str):
    rec = _create(tmp_path)
    if bad_state == "consumed":
        consume_approval(tmp_path, rec["id"], now=NOW)
    with pytest.raises(ApprovalError) as exc:
        release_approval(tmp_path, rec["id"], released_by="operator", now=NOW)
    assert exc.value.code == "approval_not_claimed"


def test_release_requires_an_identity(tmp_path: Path):
    rec = _create(tmp_path)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    with pytest.raises(ApprovalError) as exc:
        release_approval(tmp_path, rec["id"], released_by="  ", now=NOW)
    assert exc.value.code == "approval_identity_required"


def test_listing_defaults_to_unused_and_can_find_claimed(tmp_path: Path):
    """A claimed record is invisible by default, which is why --status exists."""
    rec = _create(tmp_path)
    assert [r["id"] for r in list_approval_records(tmp_path, statuses=("unused",), now=NOW)] == [
        rec["id"]
    ]
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True, now=NOW)
    assert list_approval_records(tmp_path, statuses=("unused",), now=NOW) == []
    claimed = list_approval_records(tmp_path, statuses=("claimed",), now=NOW)
    assert [r["id"] for r in claimed] == [rec["id"]]
    assert claimed[0]["claimed_at"]
    assert len(list_approval_records(tmp_path, statuses=None, now=NOW)) == 1


def test_cli_release_round_trip(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_APPROVED_BY", raising=False)
    # Real clock: the CLI derives status against utcnow, so a fixed past stamp
    # would read as expired by the time we list it.
    rec = _create(tmp_path, now=None)
    claim_approval(tmp_path, "prune", rec["argv"], rec["id"], require=True)

    stuck = _invoke(["list-approvals", "--status", "claimed", "--project-root", str(tmp_path)])
    assert stuck.exit_code == 0, stuck.output
    assert json.loads(stuck.stdout)["approvals"][0]["id"] == rec["id"]

    # --force is mandatory: the operator asserts the claiming run is dead.
    unforced = _invoke(["approval-release", rec["id"], "--project-root", str(tmp_path)])
    assert unforced.exit_code != 0
    assert "--force" in unforced.output

    released = _invoke(
        [
            "approval-release",
            rec["id"],
            "--force",
            "--released-by",
            "operator",
            "--project-root",
            str(tmp_path),
        ]
    )
    assert released.exit_code == 0, released.output
    assert json.loads(released.stdout)["status"] == "unused"
    back = _invoke(["list-approvals", "--project-root", str(tmp_path)])
    assert json.loads(back.stdout)["approvals"][0]["id"] == rec["id"]


def test_cli_list_approvals_rejects_bad_status(tmp_path: Path):
    result = _invoke(["list-approvals", "--status", "nope", "--project-root", str(tmp_path)])
    assert result.exit_code != 0
    assert "unknown status" in result.output


def test_every_gated_command_param_is_classified():
    """Each writing command's params must be either bound or explicitly neutral.

    This fails at test time when a flag is added to a gated command without
    deciding whether it changes the write, rather than leaving the runtime
    backstop to reject it in front of an operator.
    """
    import typer.main

    from det.cli import app
    from det.cli.common import _BOUND_PARAMS, _NEUTRAL_PARAMS

    group = typer.main.get_command(app)
    unclassified: dict[str, list[str]] = {}
    for name, bound in _BOUND_PARAMS.items():
        cmd = group.commands[name]  # type: ignore[attr-defined]
        allowed = bound | _NEUTRAL_PARAMS
        missing = sorted(p.name for p in cmd.params if p.name and p.name not in allowed)
        if missing:
            unclassified[name] = missing
    assert unclassified == {}, (
        "add these params to _BOUND_PARAMS (they change the write, and the "
        f"argv builder must encode them) or _NEUTRAL_PARAMS: {unclassified}"
    )


def test_gated_commands_are_all_covered():
    """Every command that gates an approval needs an entry in _BOUND_PARAMS."""
    import typer.main

    from det.cli import app
    from det.cli.common import _BOUND_PARAMS

    group = typer.main.get_command(app)
    gated = {
        name
        for name, cmd in group.commands.items()  # type: ignore[attr-defined]
        if any(p.name == "approval" for p in cmd.params)
    }
    assert gated - set(_BOUND_PARAMS) == set()


def test_migrate_write_argv_includes_recreate_iceberg():
    base = migrate_write_argv(
        "example_api.events",
        "example_api.events_v1",
        "schemas/example_api/events/events.schema.yaml",
        "identity",
        "2026-08-06",
        interval_end="2026-08-07",
    )
    assert "--recreate-iceberg" not in base
    with_flag = migrate_write_argv(
        "example_api.events",
        "example_api.events_v1",
        "schemas/example_api/events/events.schema.yaml",
        "identity",
        "2026-08-06",
        interval_end="2026-08-07",
        recreate_iceberg=True,
    )
    assert with_flag[-1] == "--recreate-iceberg"
    assert make_plan("migrate", with_flag).plan_digest != make_plan("migrate", base).plan_digest


def test_migrate_write_argv_all_raw_and_all_raw_runs():
    argv = migrate_write_argv(
        "example_api.events",
        "example_api.events_v1",
        "schemas/example_api/events/events.schema.yaml",
        "identity",
        None,
        recreate_iceberg=True,
        all_raw=True,
        all_raw_runs=True,
    )
    assert "--all-raw" in argv
    assert "--all-raw-runs" in argv
    assert "--recreate-iceberg" in argv
    assert "-s" not in argv
    with_window = migrate_write_argv(
        "example_api.events",
        "example_api.events_v1",
        "schemas/example_api/events/events.schema.yaml",
        "identity",
        "2026-08-06",
        all_raw_runs=True,
    )
    assert "-s" in with_window
    assert "--all-raw-runs" in with_window
    assert make_plan("migrate", argv).plan_digest != make_plan(
        "migrate", with_window
    ).plan_digest
