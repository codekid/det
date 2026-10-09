"""Unit tests for silver catch-up SemVer ops (Mode A + CatchupScope Mode B)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from det.runtime.silver_catchup_ops import (
    CatchupScope,
    SilverCatchupHole,
    iter_silver_catchup_holes,
    run_silver_catchup_heal,
)


def test_silver_catchup_hole_to_dict():
    hole = SilverCatchupHole(
        pipeline="noaa.storm_events",
        catchup_count=2,
        extract_lookback="48h",
    )
    assert hole.to_dict() == {
        "pipeline": "noaa.storm_events",
        "catchup_count": 2,
        "extract_lookback": "48h",
        "candidate_mode": "extract_lookback",
        "actionable": True,
    }


def test_catchup_scope_factories():
    assert CatchupScope.lookback().mode == "lookback"
    assert CatchupScope.lookback("7d").extract_lookback == "7d"
    assert CatchupScope.census().candidate_mode() == "full"
    scoped = CatchupScope.interval("2026-01-01", "2026-02-01")
    assert scoped.candidate_mode() == "interval"
    assert scoped.diff_kwargs()["interval_start"] == "2026-01-01"
    assert CatchupScope.census().diff_kwargs()["extract_lookback"] is None
    with pytest.raises(ValueError, match="48h|7d"):
        CatchupScope.lookback("nope")
    with pytest.raises(ValueError, match="non-empty start"):
        CatchupScope.interval("")


def test_iter_silver_catchup_holes_yields_only_positive_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.list_pipeline_ids",
        lambda _root: ["a.one", "b.two", "c.three"],
    )

    class _Resolved:
        def __init__(self, canonical_id: str):
            self.canonical_id = canonical_id

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda pipe, project_root=None: _Resolved(pipe),
    )

    def fake_diff(pipeline: str, **kwargs):
        counts = {"a.one": 0, "b.two": 3, "c.three": 1}
        return {
            "pipeline": pipeline,
            "catchup_count": counts[pipeline],
            "extract_lookback": kwargs.get("extract_lookback") or "48h",
        }

    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver", fake_diff
    )
    holes = list(iter_silver_catchup_holes(tmp_path, extract_lookback="48h"))
    assert [h.pipeline for h in holes] == ["b.two", "c.three"]
    assert holes[0].catchup_count == 3
    assert holes[1].catchup_count == 1


def test_iter_silver_catchup_holes_respects_allowlist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    class _Resolved:
        def __init__(self, canonical_id: str):
            self.canonical_id = canonical_id

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda pipe, project_root=None: _Resolved(pipe),
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver",
        lambda pipeline, **kwargs: {
            "pipeline": pipeline,
            "catchup_count": 2,
            "extract_lookback": "7d",
        },
    )
    holes = list(
        iter_silver_catchup_holes(
            tmp_path, pipelines=["only.pipe"], extract_lookback="7d"
        )
    )
    assert len(holes) == 1
    assert holes[0].pipeline == "only.pipe"
    assert holes[0].extract_lookback == "7d"


def test_iter_silver_catchup_holes_dedupes_aliases_after_resolve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Duplicate allowlist entries / path aliases → one hole per canonical_id."""

    class _Resolved:
        def __init__(self, canonical_id: str):
            self.canonical_id = canonical_id

    aliases = {
        "noaa.storm_events": "noaa.storm_events",
        "noaa/storm_events": "noaa.storm_events",
    }

    def _resolve(pipe: str, project_root=None):
        return _Resolved(aliases[pipe])

    diffs: list[str] = []

    def fake_diff(pipeline: str, **kwargs):
        diffs.append(pipeline)
        return {
            "pipeline": pipeline,
            "catchup_count": 1,
            "extract_lookback": "48h",
        }

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref", _resolve
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver", fake_diff
    )
    holes = list(
        iter_silver_catchup_holes(
            tmp_path,
            pipelines=[
                "noaa.storm_events",
                "noaa/storm_events",
                "noaa.storm_events",
                "  noaa.storm_events  ",
            ],
        )
    )
    assert [h.pipeline for h in holes] == ["noaa.storm_events"]
    assert diffs == ["noaa.storm_events"]


def test_iter_rejects_bad_lookback(tmp_path: Path):
    with pytest.raises(ValueError, match="48h|7d"):
        list(iter_silver_catchup_holes(tmp_path, extract_lookback="nope"))


def test_iter_rejects_scope_and_extract_lookback(tmp_path: Path):
    with pytest.raises(ValueError, match="not both"):
        list(
            iter_silver_catchup_holes(
                tmp_path,
                extract_lookback="48h",
                scope=CatchupScope.census(),
            )
        )


def test_resolve_ops_scope_rejects_unknown_mode(tmp_path: Path):
    bad = CatchupScope(mode="census")  # type: ignore[arg-type]
    object.__setattr__(bad, "mode", "nope")
    with pytest.raises(ValueError, match="lookback, interval, or census"):
        list(iter_silver_catchup_holes(tmp_path, scope=bad))


def test_iter_silver_catchup_holes_census_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    class _Resolved:
        def __init__(self, canonical_id: str):
            self.canonical_id = canonical_id

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda pipe, project_root=None: _Resolved(pipe),
    )
    seen: dict[str, object] = {}

    def fake_diff(pipeline: str, **kwargs):
        seen.update(kwargs)
        return {
            "pipeline": pipeline,
            "catchup_count": 1,
            "extract_lookback": None,
            "candidate_mode": "full",
        }

    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver", fake_diff
    )
    holes = list(
        iter_silver_catchup_holes(
            tmp_path,
            pipelines=["a.one"],
            scope=CatchupScope.census(),
        )
    )
    assert len(holes) == 1
    assert holes[0].candidate_mode == "full"
    assert holes[0].extract_lookback == ""
    assert seen.get("extract_lookback") is None
    assert seen.get("complete") is True


def test_run_silver_catchup_heal_census_passes_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    class _Resolved:
        canonical_id = "noaa.storm_events"
        path = tmp_path / "pipe.yaml"

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda *a, **k: _Resolved(),
    )
    plan_kw: dict[str, object] = {}
    diffs = iter(
        [
            {"catchup_count": 1, "candidate_mode": "full"},
            {"catchup_count": 0, "candidate_mode": "full"},
        ]
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver",
        lambda *a, **k: next(diffs),
    )
    mid = "scm_" + ("aa" * 8)

    def _plan(**kwargs):
        plan_kw.update(kwargs)
        return {
            "manifest_id": mid,
            "content_digest": "sha256:" + ("0" * 64),
            "manifest": {
                "manifest_id": mid,
                "runs": [
                    {
                        "interval_start": "2026-01-01T00:00:00+00:00",
                        "interval_end": "2026-01-02T00:00:00+00:00",
                        "extract_run_datetime": "2026-01-01T12:00:00+00:00",
                    }
                ],
                "content_digest": "sha256:" + ("0" * 64),
            },
            "diff": {"catchup_count": 1},
        }

    monkeypatch.setattr(
        "det.runtime.silver_catchup.plan_catchup_manifest", _plan
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.write_catchup_manifest",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.run_dbt",
        lambda **kwargs: MagicMock(returncode=0),
    )
    out = run_silver_catchup_heal(
        tmp_path,
        pipeline="noaa.storm_events",
        scope=CatchupScope.census(),
    )
    assert out["skipped"] is False
    assert out["candidate_mode"] == "full"
    assert out["extract_lookback"] is None
    assert plan_kw.get("extract_lookback") is None


def test_run_silver_catchup_heal_skips_when_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    class _Resolved:
        canonical_id = "noaa.storm_events"
        path = tmp_path / "pipe.yaml"

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda *a, **k: _Resolved(),
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver",
        lambda *a, **k: {"catchup_count": 0, "extract_lookback": "48h"},
    )
    out = run_silver_catchup_heal(tmp_path, pipeline="noaa.storm_events")
    assert out["skipped"] is True
    assert out["manifest_id"] is None


def test_run_silver_catchup_heal_skips_when_planned_runs_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Stale pre-plan catchup_count>0 must not write/build an empty manifest."""

    class _Resolved:
        canonical_id = "noaa.storm_events"
        path = tmp_path / "pipe.yaml"

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda *a, **k: _Resolved(),
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver",
        lambda *a, **k: {"catchup_count": 2, "extract_lookback": "48h"},
    )
    mid = "scm_" + ("ef" * 8)
    monkeypatch.setattr(
        "det.runtime.silver_catchup.plan_catchup_manifest",
        lambda **kwargs: {
            "manifest_id": mid,
            "content_digest": "sha256:" + ("2" * 64),
            "manifest": {
                "manifest_id": mid,
                "runs": [],
                "content_digest": "sha256:" + ("2" * 64),
            },
            "diff": {"catchup_count": 0, "catchup_runs": []},
        },
    )
    wrote: list[str] = []
    monkeypatch.setattr(
        "det.runtime.silver_catchup.write_catchup_manifest",
        lambda *a, **k: wrote.append("wrote"),
    )
    dbt_calls: list[str] = []
    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.run_dbt",
        lambda **kwargs: dbt_calls.append("dbt") or MagicMock(returncode=0),
    )
    out = run_silver_catchup_heal(tmp_path, pipeline="noaa.storm_events")
    assert out["skipped"] is True
    assert out["reason"] == "nothing_to_do"
    assert out["manifest_id"] is None
    assert wrote == []
    assert dbt_calls == []


def test_run_silver_catchup_heal_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    class _Resolved:
        canonical_id = "noaa.storm_events"
        path = tmp_path / "configs" / "pipelines" / "noaa" / "storm_events.yaml"

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda *a, **k: _Resolved(),
    )
    diffs = iter(
        [
            {"catchup_count": 2, "extract_lookback": "48h"},
            {"catchup_count": 0, "extract_lookback": "48h"},
        ]
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver",
        lambda *a, **k: next(diffs),
    )
    mid = "scm_" + ("ab" * 8)
    digest = "sha256:" + ("0" * 64)
    monkeypatch.setattr(
        "det.runtime.silver_catchup.plan_catchup_manifest",
        lambda **kwargs: {
            "manifest_id": mid,
            "content_digest": digest,
            "manifest": {
                "manifest_id": mid,
                "runs": [
                    {
                        "interval_start": "2026-01-01T00:00:00+00:00",
                        "interval_end": "2026-01-02T00:00:00+00:00",
                        "extract_run_datetime": "2026-01-01T12:00:00+00:00",
                    }
                ],
                "content_digest": digest,
            },
            "diff": {"catchup_count": 2, "catchup_runs": [{}]},
        },
    )
    wrote: list[str] = []
    monkeypatch.setattr(
        "det.runtime.silver_catchup.write_catchup_manifest",
        lambda payload, **kwargs: wrote.append(str(payload.get("manifest_id"))),
    )
    captured: dict[str, object] = {}

    def _fake_dbt(**kwargs):
        captured.update(kwargs)
        return MagicMock(returncode=0, command=["dbt", "build"])

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.run_dbt",
        _fake_dbt,
    )
    out = run_silver_catchup_heal(tmp_path, pipeline="noaa.storm_events")
    assert out["skipped"] is False
    assert out["manifest_id"] == mid
    assert out["catchup_count_before"] == 2
    assert out["catchup_count_after"] == 0
    assert wrote == [mid]
    assert captured.get("pipeline") == _Resolved.path


def test_run_silver_catchup_heal_verify_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    class _Resolved:
        canonical_id = "noaa.storm_events"
        path = tmp_path / "pipe.yaml"

    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.resolve_pipeline_ref",
        lambda *a, **k: _Resolved(),
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.diff_bronze_silver",
        lambda *a, **k: {"catchup_count": 1, "extract_lookback": "48h"},
    )
    mid = "scm_" + ("cd" * 8)
    monkeypatch.setattr(
        "det.runtime.silver_catchup.plan_catchup_manifest",
        lambda **kwargs: {
            "manifest_id": mid,
            "content_digest": "sha256:" + ("1" * 64),
            "manifest": {
                "manifest_id": mid,
                "runs": [
                    {
                        "interval_start": "2026-01-01T00:00:00+00:00",
                        "interval_end": "2026-01-02T00:00:00+00:00",
                        "extract_run_datetime": "2026-01-01T12:00:00+00:00",
                    }
                ],
                "content_digest": "sha256:" + ("1" * 64),
            },
            "diff": {"catchup_count": 1},
        },
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup.write_catchup_manifest",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "det.runtime.silver_catchup_ops.run_dbt",
        lambda **kwargs: MagicMock(returncode=0),
    )
    with pytest.raises(RuntimeError, match="verify failed"):
        run_silver_catchup_heal(tmp_path, pipeline="noaa.storm_events")
