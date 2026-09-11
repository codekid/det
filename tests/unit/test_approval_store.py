"""Lake approval store, heartbeat triage, and legacy fallback."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from det.runtime.approval import (
    ApprovalError,
    claim_approval,
    create_approval,
    describe_approval_record,
    load_approval,
    prune_write_argv,
    release_approval,
    touch_approval_heartbeat,
)
from det.runtime.approval_store.enrich import enrich_heartbeat_fields
from det.runtime.approval_store.legacy import legacy_approvals_dir
from det.runtime.settings import DetSettings, use_settings

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)


def _settings(tmp_path: Path) -> DetSettings:
    lake = tmp_path / "lake"
    lake.mkdir()
    return DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_override=str(lake)
    )


def test_create_lands_under_ops_approvals(tmp_path: Path):
    settings = _settings(tmp_path)
    with use_settings(settings):
        argv = prune_write_argv("example_api.events", "2026-08-01")
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="tester", now=NOW
        )
        path = tmp_path / "lake" / "approvals" / f"{rec['id']}.json"
        assert path.is_file()
        loaded = load_approval(tmp_path, rec["id"])
        assert loaded["id"] == rec["id"]


def test_claim_sets_heartbeat_and_touch_updates(tmp_path: Path):
    settings = _settings(tmp_path)
    with use_settings(settings):
        argv = prune_write_argv("example_api.events", "2026-08-01")
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="tester", now=NOW
        )
        claimed = claim_approval(
            tmp_path, "prune", argv, rec["id"], require=True, now=NOW
        )
        assert claimed is not None
        assert claimed["status"] == "claimed"
        assert claimed.get("heartbeat_at")
        later = NOW + timedelta(seconds=10)
        touched = touch_approval_heartbeat(tmp_path, rec["id"], now=later)
        assert touched["heartbeat_at"] == "2026-08-18T12:00:10Z"


def test_enrich_heartbeat_fresh_stale_unknown():
    base = {"heartbeat_interval_sec": 30}
    assert enrich_heartbeat_fields(base, now=NOW)["heartbeat_status"] == "unknown"
    fresh = {
        **base,
        "heartbeat_at": "2026-08-18T12:00:00Z",
    }
    assert enrich_heartbeat_fields(fresh, now=NOW)["heartbeat_status"] == "fresh"
    stale = {
        **base,
        "heartbeat_at": "2026-08-18T11:00:00Z",
    }
    assert enrich_heartbeat_fields(stale, now=NOW)["heartbeat_status"] == "stale"


def test_legacy_dot_det_read_fallback(tmp_path: Path):
    settings = _settings(tmp_path)
    legacy_dir = legacy_approvals_dir(tmp_path)
    legacy_dir.mkdir(parents=True)
    from det.runtime.approval import check_approval, make_plan

    argv = prune_write_argv("example_api.events", "2026-08-01")
    plan = make_plan("prune", argv)
    apr = "apr_" + ("ab" * 8)
    record = {
        "id": apr,
        "created_at": "2026-08-18T12:00:00Z",
        "expires_at": "2026-08-18T13:00:00Z",
        "approved_by": "legacy",
        "command": "prune",
        "argv": list(plan.argv),
        "plan_digest": plan.plan_digest,
        "status": "unused",
        "consumed_at": None,
    }
    (legacy_dir / f"{apr}.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    with use_settings(settings):
        loaded = load_approval(tmp_path, apr)
        assert loaded["approved_by"] == "legacy"
        with pytest.raises(ApprovalError) as exc:
            check_approval(tmp_path, "prune", argv, apr, require=True, now=NOW)
        assert exc.value.code == "approval_legacy_readonly"
        with pytest.raises(ApprovalError) as exc:
            claim_approval(tmp_path, "prune", argv, apr, require=True, now=NOW)
        assert exc.value.code == "approval_legacy_readonly"
        with pytest.raises(ApprovalError) as exc:
            release_approval(tmp_path, apr, released_by="ops", now=NOW)
        assert exc.value.code == "approval_legacy_readonly"
        with pytest.raises(ApprovalError) as exc:
            touch_approval_heartbeat(tmp_path, apr, now=NOW)
        assert exc.value.code == "approval_legacy_readonly"


def test_store_uses_settings_lake_override(tmp_path: Path):
    """Claim/consume must honor CLI DetSettings lake roots, not bare env."""
    from det.runtime.approval import consume_approval

    lake_a = tmp_path / "lake_a"
    lake_b = tmp_path / "lake_b"
    lake_a.mkdir()
    lake_b.mkdir()
    settings_a = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_override=str(lake_a)
    )
    settings_b = DetSettings.from_env(project_root=tmp_path).with_overrides(
        lake_override=str(lake_b)
    )
    argv = prune_write_argv("example_api.events", "2026-08-01")
    rec = create_approval(
        tmp_path,
        command="prune",
        argv=argv,
        approved_by="tester",
        now=NOW,
        settings=settings_a,
    )
    assert (lake_a / "approvals" / f"{rec['id']}.json").is_file()
    assert not (lake_b / "approvals" / f"{rec['id']}.json").exists()
    with pytest.raises(ApprovalError) as exc:
        claim_approval(
            tmp_path,
            "prune",
            argv,
            rec["id"],
            require=True,
            now=NOW,
            settings=settings_b,
        )
    assert exc.value.code == "approval_not_found"
    claimed = claim_approval(
        tmp_path,
        "prune",
        argv,
        rec["id"],
        require=True,
        now=NOW,
        settings=settings_a,
    )
    assert claimed is not None and claimed["status"] == "claimed"
    consumed = consume_approval(
        tmp_path, rec["id"], now=NOW, settings=settings_a
    )
    assert consumed["status"] == "consumed"


def test_describe_includes_heartbeat_fields(tmp_path: Path):
    settings = _settings(tmp_path)
    with use_settings(settings):
        argv = prune_write_argv("example_api.events", "2026-08-01")
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="tester", now=NOW
        )
        claim_approval(tmp_path, "prune", argv, rec["id"], require=True, now=NOW)
        desc = describe_approval_record(tmp_path, rec["id"], now=NOW)
        assert desc["status"] == "claimed"
        assert desc["heartbeat_status"] == "fresh"
        assert desc["heartbeat_age_sec"] == 0


def test_release_after_claim(tmp_path: Path):
    settings = _settings(tmp_path)
    with use_settings(settings):
        argv = prune_write_argv("example_api.events", "2026-08-01")
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="tester", now=NOW
        )
        claim_approval(tmp_path, "prune", argv, rec["id"], require=True, now=NOW)
        out = release_approval(tmp_path, rec["id"], released_by="ops", now=NOW)
        assert out["status"] == "unused"
        assert "released_from_claim" in out


def test_orphan_claim_sidecar_can_be_released(tmp_path: Path):
    """Crash between .claim create and JSON update leaves unused + sidecar."""
    from det.runtime.approval_store.lake_store import LakeApprovalStore
    from det.runtime.silver_catchup.paths import resolve_ops_lake

    settings = _settings(tmp_path)
    with use_settings(settings):
        argv = prune_write_argv("example_api.events", "2026-08-01")
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="tester", now=NOW
        )
        ops = resolve_ops_lake(project_root=tmp_path, settings=settings)
        store = LakeApprovalStore(ops)
        claim = store._claim_ref(rec["id"])
        claim.create_exclusive(b'{"orphaned":true}\n')
        # Primary still unused but claim blocks new claims
        with pytest.raises(ApprovalError) as exc:
            claim_approval(tmp_path, "prune", argv, rec["id"], require=True, now=NOW)
        assert exc.value.code == "approval_in_flight"
        released = release_approval(tmp_path, rec["id"], released_by="ops", now=NOW)
        assert released["status"] == "unused"
        assert not claim.exists()
        # Orphan clear does not stamp released_* (sidecar-only recovery).
        claimed = claim_approval(
            tmp_path, "prune", argv, rec["id"], require=True, now=NOW
        )
        assert claimed is not None and claimed["status"] == "claimed"


def test_heartbeat_does_not_resurrect_consumed(tmp_path: Path):
    """CAS conflict must not soft-overwrite consumed status back to claimed."""
    from det.runtime.approval import consume_approval
    from det.runtime.approval_store.lake_store import LakeApprovalStore
    from det.runtime.silver_catchup.paths import resolve_ops_lake

    settings = _settings(tmp_path)
    with use_settings(settings):
        argv = prune_write_argv("example_api.events", "2026-08-01")
        rec = create_approval(
            tmp_path, command="prune", argv=argv, approved_by="tester", now=NOW
        )
        claim_approval(tmp_path, "prune", argv, rec["id"], require=True, now=NOW)
        ops = resolve_ops_lake(project_root=tmp_path, settings=settings)
        store = LakeApprovalStore(ops)
        # Simulate stale heartbeat writer that loaded claimed, then lose the race
        stale, version = store._load_versioned(rec["id"])
        consume_approval(tmp_path, rec["id"], now=NOW + timedelta(seconds=1))
        stale["heartbeat_at"] = "2026-08-18T12:00:05Z"
        with pytest.raises(ApprovalError) as exc:
            store._write_record(stale, expected_version=version)
        assert exc.value.code == "approval_conflict"
        final = store.load(rec["id"])
        assert final["status"] == "consumed"
        with pytest.raises(ApprovalError) as exc:
            touch_approval_heartbeat(tmp_path, rec["id"], now=NOW + timedelta(seconds=2))
        assert exc.value.code == "approval_not_claimed"
