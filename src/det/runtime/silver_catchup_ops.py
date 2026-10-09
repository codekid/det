"""Silver catch-up ops for embedders and the reference Airflow DAG.

Tier 1 library surface: detect holes, then heal one pipeline (apply + dbt build
+ verify). Default scope is Mode A lookback ``48h``. Mode B (census / interval)
uses :class:`CatchupScope` constructors so full-lake scans are intentional.
CLI ``det silver-catchup heal`` remains Mode A–only; Advanced plan/apply stay
on CLI/MCP.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from det.runtime import silver_catchup as silver_catchup
from det.runtime.dbt_runner import analytics_exclude, run_dbt
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref
from det.runtime.silver_catchup.ids import (
    parse_extract_lookback,
    resolve_catchup_candidate_scope,
)

DEFAULT_EXTRACT_LOOKBACK = silver_catchup.DEFAULT_EXTRACT_LOOKBACK

__all__ = [
    "DEFAULT_EXTRACT_LOOKBACK",
    "CatchupScope",
    "SilverCatchupHole",
    "iter_silver_catchup_holes",
    "run_silver_catchup_heal",
]

CandidateMode = Literal["extract_lookback", "interval", "full"]


@dataclass(frozen=True)
class CatchupScope:
    """Typed discovery scope for SemVer silver catch-up (Mode A or Mode B).

    Use factories so full-lake / interval audits are intentional constructions.
    Omit ``scope`` on :func:`iter_silver_catchup_holes` /
    :func:`run_silver_catchup_heal` to default to Mode A lookback ``48h``.
    """

    mode: Literal["lookback", "interval", "census"]
    # Named extract_lookback so the ``lookback()`` factory is not shadowed.
    extract_lookback: str | None = None
    interval_start: str | None = None
    interval_end: str | None = None

    @classmethod
    def lookback(cls, window: str = DEFAULT_EXTRACT_LOOKBACK) -> CatchupScope:
        text = str(window or "").strip() or DEFAULT_EXTRACT_LOOKBACK
        parse_extract_lookback(text)
        return cls(mode="lookback", extract_lookback=text)

    @classmethod
    def interval(cls, start: str, end: str | None = None) -> CatchupScope:
        start_text = str(start or "").strip()
        if not start_text:
            raise ValueError("CatchupScope.interval requires a non-empty start")
        end_text = str(end).strip() if end is not None else None
        if end is not None and not end_text:
            raise ValueError("CatchupScope.interval end must be non-empty when set")
        return cls(mode="interval", interval_start=start_text, interval_end=end_text)

    @classmethod
    def census(cls) -> CatchupScope:
        return cls(mode="census")

    def to_resolve_kwargs(self) -> dict[str, Any]:
        """Kwargs for :func:`resolve_catchup_candidate_scope`."""
        if self.mode == "lookback":
            return {
                "extract_lookback": self.extract_lookback,
                "census": False,
                "interval_start": None,
                "interval_end": None,
            }
        if self.mode == "census":
            return {
                "extract_lookback": None,
                "census": True,
                "interval_start": None,
                "interval_end": None,
            }
        return {
            "extract_lookback": None,
            "census": False,
            "interval_start": self.interval_start,
            "interval_end": self.interval_end,
        }

    def candidate_mode(self) -> CandidateMode:
        if self.mode == "lookback":
            return "extract_lookback"
        if self.mode == "interval":
            return "interval"
        return "full"

    def diff_kwargs(self) -> dict[str, Any]:
        """Kwargs for ``diff_bronze_silver`` / ``plan_catchup_manifest``."""
        flags = self.to_resolve_kwargs()
        resolved = resolve_catchup_candidate_scope(**flags)
        return {
            "extract_lookback": resolved,
            "interval_start": flags["interval_start"],
            "interval_end": flags["interval_end"],
        }


@dataclass(frozen=True)
class SilverCatchupHole:
    """One pipeline that needs a silver catch-up heal for the chosen scope."""

    pipeline: str
    catchup_count: int
    extract_lookback: str
    candidate_mode: CandidateMode = "extract_lookback"
    actionable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "catchup_count": self.catchup_count,
            "extract_lookback": self.extract_lookback,
            "candidate_mode": self.candidate_mode,
            "actionable": self.actionable,
        }


def _normalize_lookback(extract_lookback: str | None) -> str:
    text = (extract_lookback or "").strip() or DEFAULT_EXTRACT_LOOKBACK
    # Validate shape (raises ValueError on bad input).
    parse_extract_lookback(text)
    return text


def _resolve_ops_scope(
    *,
    extract_lookback: str | None,
    scope: CatchupScope | None,
) -> CatchupScope:
    """Resolve ``scope`` / legacy ``extract_lookback`` into one CatchupScope."""
    if scope is not None and extract_lookback is not None:
        raise ValueError("pass scope= or extract_lookback=, not both")
    if scope is not None:
        # Re-run factories so invalid values raise the same errors as construction.
        if scope.mode == "lookback":
            return CatchupScope.lookback(
                scope.extract_lookback or DEFAULT_EXTRACT_LOOKBACK
            )
        if scope.mode == "interval":
            return CatchupScope.interval(
                scope.interval_start or "",
                scope.interval_end,
            )
        if scope.mode == "census":
            return CatchupScope.census()
        raise ValueError(
            f"CatchupScope.mode must be lookback, interval, or census; got {scope.mode!r}"
        )
    return CatchupScope.lookback(_normalize_lookback(extract_lookback))


def iter_silver_catchup_holes(
    project_root: Path,
    *,
    pipelines: Sequence[str] | None = None,
    extract_lookback: str | None = None,
    scope: CatchupScope | None = None,
) -> Iterator[SilverCatchupHole]:
    """Yield holes (``catchup_count > 0``) for fleet or allowlist pipelines.

    Default scope is Mode A lookback ``48h``. Pass ``scope=CatchupScope.census()``
    or ``CatchupScope.interval(...)`` for Mode B. ``extract_lookback=`` remains
    for Mode A callers; do not combine it with ``scope=``.
    """
    root = project_root.resolve()
    resolved_scope = _resolve_ops_scope(
        extract_lookback=extract_lookback, scope=scope
    )
    diff_kw = resolved_scope.diff_kwargs()
    mode = resolved_scope.candidate_mode()
    ids = (
        [str(p).strip() for p in pipelines if str(p).strip()]
        if pipelines is not None
        else list(list_pipeline_ids(root))
    )
    seen: set[str] = set()
    for pipe in ids:
        canonical = resolve_pipeline_ref(pipe, project_root=root).canonical_id
        if canonical in seen:
            continue
        seen.add(canonical)
        diff = silver_catchup.diff_bronze_silver(
            canonical,
            project_root=root,
            complete=True,
            **diff_kw,
        )
        count = int(diff.get("catchup_count") or 0)
        if count <= 0:
            continue
        lookback_out = str(
            diff.get("extract_lookback") or diff_kw.get("extract_lookback") or ""
        )
        raw_mode = str(diff.get("candidate_mode") or mode)
        hole_mode: CandidateMode = (
            cast(CandidateMode, raw_mode)
            if raw_mode in ("extract_lookback", "interval", "full")
            else mode
        )
        yield SilverCatchupHole(
            pipeline=canonical,
            catchup_count=count,
            extract_lookback=lookback_out,
            candidate_mode=hole_mode,
            actionable=True,
        )


def run_silver_catchup_heal(
    project_root: Path,
    *,
    pipeline: str,
    extract_lookback: str | None = None,
    scope: CatchupScope | None = None,
) -> dict[str, Any]:
    """Apply catch-up manifest, dbt build ``--catchup``, then verify no holes remain.

    Default scope is Mode A lookback ``48h``. Mode B via ``CatchupScope.census()``
    / ``.interval(...)``. No approval gate — for trusted ops / Airflow workers
    (do not set ``DET_REQUIRE_APPROVAL=1`` on that path).
    """
    root = project_root.resolve()
    resolved_scope = _resolve_ops_scope(
        extract_lookback=extract_lookback, scope=scope
    )
    diff_kw = resolved_scope.diff_kwargs()
    mode = resolved_scope.candidate_mode()
    lookback_out = diff_kw.get("extract_lookback")
    resolved = resolve_pipeline_ref(pipeline, project_root=root)
    pipe = resolved.canonical_id
    before = silver_catchup.diff_bronze_silver(
        pipe,
        project_root=root,
        complete=True,
        **diff_kw,
    )
    if int(before.get("catchup_count") or 0) == 0:
        return {
            "pipeline": pipe,
            "extract_lookback": lookback_out,
            "candidate_mode": mode,
            "catchup_count_before": 0,
            "catchup_count_after": 0,
            "manifest_id": None,
            "content_digest": None,
            "skipped": True,
            "reason": "nothing_to_do",
        }

    planned = silver_catchup.plan_catchup_manifest(
        project_root=root,
        pipeline=pipe,
        **diff_kw,
    )
    mid = str(planned["manifest_id"])
    digest = str(planned["content_digest"])
    manifest_body = planned["manifest"]
    planned_diff = planned.get("diff") if isinstance(planned.get("diff"), dict) else {}
    runs = list(manifest_body.get("runs") or [])
    before_count = int(planned_diff.get("catchup_count") or len(runs) or 0)
    if not runs:
        return {
            "pipeline": pipe,
            "extract_lookback": lookback_out,
            "candidate_mode": mode,
            "catchup_count_before": 0,
            "catchup_count_after": 0,
            "manifest_id": None,
            "content_digest": None,
            "skipped": True,
            "reason": "nothing_to_do",
        }

    silver_catchup.write_catchup_manifest(manifest_body, project_root=root)

    result = run_dbt(
        project_root=root,
        command="build",
        catchup=True,
        catchup_manifest=mid,
        pipeline=resolved.path,
        exclude=analytics_exclude(None),
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"catch-up dbt build failed for {pipe} manifest_id={mid} exit={result.returncode}"
        )

    after = silver_catchup.diff_bronze_silver(
        pipe,
        project_root=root,
        complete=True,
        **diff_kw,
    )
    after_count = int(after.get("catchup_count") or 0)
    if after_count != 0:
        raise RuntimeError(
            f"catch-up verify failed for {pipe}: catchup_count={after_count} "
            f"after heal manifest_id={mid}"
        )
    return {
        "pipeline": pipe,
        "extract_lookback": lookback_out,
        "candidate_mode": mode,
        "catchup_count_before": before_count,
        "catchup_count_after": after_count,
        "manifest_id": mid,
        "content_digest": digest,
        "dbt_returncode": result.returncode,
        "skipped": False,
    }
