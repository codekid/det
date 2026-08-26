"""Composite pipeline diagnose helper."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.destinations.models import raw_dataset_dir
from det.mcp.context import resolve_under_root
from det.mcp.errors import sanitize_detail

from ._common import (
    DEFAULT_SAMPLE_LIMIT,
    _load_pipeline,
    _rel,
    _root,
    clamp_sample_limit,
)
from ._partitions import diff_partitions
from ._sample import validate_sample


def diagnose_pipeline(
    pipeline: str,
    *,
    interval_start: str | None = None,
    interval_end: str | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    root: Path | None = None,
) -> dict[str, Any]:
    """Composite inspect: coverage diff + optional validate on latest only_raw run."""
    base = _root(root)
    config, _ = _load_pipeline(pipeline, base)
    capped = clamp_sample_limit(sample_limit)

    diff = diff_partitions(
        pipeline,
        interval_start=interval_start,
        interval_end=interval_end,
        root=base,
    )
    findings: list[dict[str, Any]] = []
    suggested: list[str] = []
    evidence: dict[str, Any] = {"diff": diff}
    validation: dict[str, Any] | None = None

    only_raw = list(diff.get("only_raw") or [])
    only_bronze = list(diff.get("only_bronze") or [])
    both_count = int(diff.get("both_count") or 0)
    raw_total = int(diff.get("only_raw_count") or 0) + both_count
    bronze_total = int(diff.get("only_bronze_count") or 0) + both_count

    if raw_total == 0 and bronze_total == 0:
        findings.append(
            {
                "severity": "error",
                "code": "empty_lake",
                "detail": (
                    "No raw or bronze runs found"
                    + (f" for window starting {interval_start}" if interval_start else "")
                    + f" under {_rel(raw_dataset_dir(config, base), base)}"
                ),
            }
        )
        suggested.append(
            f"det extract -p {config.name} -s <interval_start>"
            if not interval_start
            else f"det extract -p {config.name} -s {interval_start[:10]}"
        )
    else:
        if only_raw:
            findings.append(
                {
                    "severity": "warning",
                    "code": "raw_without_bronze",
                    "detail": (
                        f"{diff['only_raw_count']} raw run(s) have no matching bronze "
                        f"(showing up to {len(only_raw)})"
                    ),
                }
            )
            latest = max(only_raw, key=lambda r: r["extract_run_datetime"])
            start_flag = (latest.get("interval_start") or "")[:10] or (
                interval_start[:10] if interval_start else "<interval_start>"
            )
            suggested.append(f"det load -p {config.name} -s {start_flag}")
            try:
                validation = validate_sample(
                    pipeline,
                    limit=capped,
                    run_path=latest.get("path"),
                    root=base,
                )
                evidence["validation"] = validation
                if not validation.get("ok"):
                    findings.append(
                        {
                            "severity": "error",
                            "code": "schema_invalid",
                            "detail": (
                                f"validate_sample failed on latest only_raw run "
                                f"{latest.get('path')}: "
                                f"{len(validation.get('coerce_errors') or [])} coerce, "
                                f"{len(validation.get('schema_errors') or [])} schema"
                            ),
                        }
                    )
            except Exception as exc:
                findings.append(
                    {
                        "severity": "error",
                        "code": "schema_invalid",
                        "detail": (
                            f"validate_sample error on {latest.get('path')}: "
                            f"{sanitize_detail(exc)}"
                        ),
                    }
                )
            try:
                if latest.get("path"):
                    mdir = resolve_under_root(str(latest["path"]), root=base)
                    # Late import so test monkeypatching via det.mcp.inspect.read_raw_manifest
                    # remains effective (the function's globals must be the inspect package).
                    import det.mcp.inspect as _insp
                    evidence["manifest"] = {
                        "path": _rel(mdir / "meta" / "manifest.json", base),
                        "manifest": _insp.read_raw_manifest(mdir),
                    }
            except Exception as exc:
                evidence["manifest_error"] = sanitize_detail(exc)

        if only_bronze:
            findings.append(
                {
                    "severity": "warning",
                    "code": "bronze_without_raw",
                    "detail": (
                        f"{diff['only_bronze_count']} bronze run(s) have no matching raw "
                        "(orphan, wrong lake, or raw pruned externally — "
                        "migrate rebuilds from raw only)"
                    ),
                }
            )

        if not only_raw and not only_bronze and both_count > 0:
            findings.append(
                {
                    "severity": "info",
                    "code": "ok",
                    "detail": f"raw and bronze coverage match ({both_count} run(s))",
                }
            )

    # Prefer a single summary line.
    codes = [f["code"] for f in findings]
    if "empty_lake" in codes:
        summary = "empty lake — no raw or bronze runs"
    elif "schema_invalid" in codes:
        summary = "raw ahead of bronze; sample failed schema/coerce validation"
    elif "raw_without_bronze" in codes:
        summary = f"raw ahead of bronze by {diff['only_raw_count']} run(s)"
    elif "bronze_without_raw" in codes:
        summary = f"bronze ahead of raw by {diff['only_bronze_count']} run(s)"
    elif "ok" in codes:
        summary = "raw and bronze coverage match"
    else:
        summary = "diagnose complete"

    return {
        "pipeline": config.name,
        "destination_type": config.destination.type,
        "sample_limit": capped,
        "summary": summary,
        "findings": findings,
        "evidence": evidence,
        "suggested_commands": suggested,
    }
