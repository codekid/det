"""Mode A silver catch-up ops for embedders and the reference Airflow DAG.

Tier 1 library surface: detect holes, then heal one pipeline (apply + dbt build
+ verify). Census / interval windows are out of scope — use CLI/MCP Advanced.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from det.runtime.dbt_runner import analytics_exclude, run_dbt
from det.runtime.pipelines import list_pipeline_ids, resolve_pipeline_ref
from det.runtime.silver_catchup import (
    DEFAULT_EXTRACT_LOOKBACK,
    diff_bronze_silver,
    plan_catchup_manifest,
    write_catchup_manifest,
)
from det.runtime.silver_catchup.ids import parse_extract_lookback

__all__ = [
    "DEFAULT_EXTRACT_LOOKBACK",
    "SilverCatchupHole",
    "iter_silver_catchup_holes",
    "run_silver_catchup_heal",
]


@dataclass(frozen=True)
class SilverCatchupHole:
    """One pipeline that needs a Mode A silver catch-up heal."""

    pipeline: str
    catchup_count: int
    extract_lookback: str
    actionable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "catchup_count": self.catchup_count,
            "extract_lookback": self.extract_lookback,
            "actionable": self.actionable,
        }


def _normalize_lookback(extract_lookback: str | None) -> str:
    text = (extract_lookback or "").strip() or DEFAULT_EXTRACT_LOOKBACK
    # Validate shape (raises ValueError on bad input).
    parse_extract_lookback(text)
    return text


def iter_silver_catchup_holes(
    project_root: Path,
    *,
    pipelines: Sequence[str] | None = None,
    extract_lookback: str | None = None,
) -> Iterator[SilverCatchupHole]:
    """Yield Mode A holes (``catchup_count > 0``) for fleet or allowlist pipelines.

    Default lookback is ``48h``. Does not support census or ``-s``/``-e`` windows;
    use CLI/MCP Advanced for those.
    """
    root = project_root.resolve()
    lookback = _normalize_lookback(extract_lookback)
    ids = (
        [str(p).strip() for p in pipelines if str(p).strip()]
        if pipelines is not None
        else list(list_pipeline_ids(root))
    )
    for pipe in ids:
        canonical = resolve_pipeline_ref(pipe, project_root=root).canonical_id
        diff = diff_bronze_silver(
            canonical,
            project_root=root,
            extract_lookback=lookback,
            complete=True,
        )
        count = int(diff.get("catchup_count") or 0)
        if count <= 0:
            continue
        effective = str(diff.get("extract_lookback") or lookback)
        yield SilverCatchupHole(
            pipeline=canonical,
            catchup_count=count,
            extract_lookback=effective,
            actionable=True,
        )


def run_silver_catchup_heal(
    project_root: Path,
    *,
    pipeline: str,
    extract_lookback: str | None = None,
) -> dict[str, Any]:
    """Apply catch-up manifest, dbt build ``--catchup``, then verify no holes remain.

    Mode A only (default lookback ``48h``). No approval gate — for trusted ops /
    Airflow workers (do not set ``DET_REQUIRE_APPROVAL=1`` on that path).
    """
    root = project_root.resolve()
    lookback = _normalize_lookback(extract_lookback)
    resolved = resolve_pipeline_ref(pipeline, project_root=root)
    pipe = resolved.canonical_id
    before = diff_bronze_silver(
        pipe,
        project_root=root,
        extract_lookback=lookback,
        complete=True,
    )
    before_count = int(before.get("catchup_count") or 0)
    if before_count == 0:
        return {
            "pipeline": pipe,
            "extract_lookback": lookback,
            "catchup_count_before": 0,
            "catchup_count_after": 0,
            "manifest_id": None,
            "content_digest": None,
            "skipped": True,
            "reason": "nothing_to_do",
        }

    planned = plan_catchup_manifest(
        project_root=root,
        pipeline=pipe,
        extract_lookback=lookback,
    )
    mid = str(planned["manifest_id"])
    digest = str(planned["content_digest"])
    manifest_body = planned["manifest"]
    write_catchup_manifest(manifest_body, project_root=root)

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
            f"catch-up dbt build failed for {pipe} manifest_id={mid} "
            f"exit={result.returncode}"
        )

    after = diff_bronze_silver(
        pipe,
        project_root=root,
        extract_lookback=lookback,
        complete=True,
    )
    after_count = int(after.get("catchup_count") or 0)
    if after_count != 0:
        raise RuntimeError(
            f"catch-up verify failed for {pipe}: catchup_count={after_count} "
            f"after heal manifest_id={mid}"
        )
    return {
        "pipeline": pipe,
        "extract_lookback": lookback,
        "catchup_count_before": before_count,
        "catchup_count_after": after_count,
        "manifest_id": mid,
        "content_digest": digest,
        "dbt_returncode": result.returncode,
        "skipped": False,
    }
