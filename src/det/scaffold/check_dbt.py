"""dbt scaffold checks layered on kernel ``check_project`` (analytics adapter)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from det.runtime.check import Finding, check_pipeline_config, check_project
from det.runtime.config import load_pipeline_config
from det.runtime.ids import dbt_model_slug
from det.runtime.pipelines import (
    discover_pipeline_files,
    resolve_pipeline_ref,
    resolve_project_root,
)
from det.scaffold.dbt import expected_silver_sql


def _normalize_scaffold_text(text: str) -> str:
    text = text.replace("\r\n", "\n")
    if text and not text.endswith("\n"):
        text += "\n"
    return text


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def scaffold_sql_stale_findings(
    config: Any,
    *,
    project_root: Path,
    pipeline_id: str,
) -> list[Finding]:
    """Warn when on-disk silver SQL no longer matches scaffold templates + knobs."""
    findings: list[Finding] = []
    try:
        expected = expected_silver_sql(config, project_root=project_root)
    except Exception as exc:
        findings.append(
            Finding(
                severity="warning",
                code="scaffold_sql_stale",
                pipeline=pipeline_id,
                path=_rel(project_root / "dbt" / "models" / "silver", project_root),
                detail=f"could not re-render expected silver SQL: {exc}",
            )
        )
        return findings

    for rel, content in expected.items():
        path = project_root / rel
        if not path.is_file():
            findings.append(
                Finding(
                    severity="warning",
                    code="scaffold_sql_stale",
                    pipeline=pipeline_id,
                    path=rel,
                    detail=(
                        "expected scaffolded silver SQL is missing; "
                        f"regenerate with `det scaffold-dbt -p {pipeline_id}`"
                    ),
                )
            )
            continue
        try:
            on_disk = _normalize_scaffold_text(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError) as exc:
            findings.append(
                Finding(
                    severity="warning",
                    code="scaffold_sql_stale",
                    pipeline=pipeline_id,
                    path=rel,
                    detail=f"could not read on-disk silver SQL: {exc}",
                )
            )
            continue
        if on_disk != _normalize_scaffold_text(content):
            findings.append(
                Finding(
                    severity="warning",
                    code="scaffold_sql_stale",
                    pipeline=pipeline_id,
                    path=rel,
                    detail=(
                        "on-disk silver SQL does not match current pipeline "
                        "dbt.silver knobs / scaffold templates. Re-scaffold with "
                        f"`det scaffold-dbt -p {pipeline_id} --force` "
                        "(or keep hand-tuned SQL and accept this warning)."
                    ),
                )
            )
    return findings


def check_pipeline_config_with_dbt(
    config_path: Path,
    *,
    project_root: Path,
) -> list[Finding]:
    """Kernel pipeline check plus scaffold SQL drift when silver models exist."""
    root = project_root.resolve()
    findings = check_pipeline_config(config_path, project_root=root)
    if any(f.code == "invalid_pipeline" for f in findings):
        return findings
    dbt_root = root / "dbt"
    if not dbt_root.is_dir():
        return findings
    try:
        config = load_pipeline_config(config_path)
    except Exception:  # noqa: S112
        return findings
    silver = dbt_root / "models" / "silver" / f"silver_{dbt_model_slug(config.name)}.sql"
    if not silver.is_file():
        return findings
    findings.extend(
        scaffold_sql_stale_findings(
            config,
            project_root=root,
            pipeline_id=config.name,
        )
    )
    return findings


def check_project_with_dbt(
    project_root: Path | str | None = None,
    *,
    pipeline: str | None = None,
) -> list[Finding]:
    """Operator/agent check: ``check_project`` plus ``scaffold_sql_stale``."""
    root = resolve_project_root(project_root)
    if pipeline is not None:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        return check_pipeline_config_with_dbt(resolved.path, project_root=root)

    findings = check_project(root)
    for path in discover_pipeline_files(root):
        try:
            config = load_pipeline_config(path)
        except Exception:  # noqa: S112  # skip unloadable pipelines; kernel check already reported
            continue
        silver = root / "dbt" / "models" / "silver" / f"silver_{dbt_model_slug(config.name)}.sql"
        if not silver.is_file():
            continue
        findings.extend(
            scaffold_sql_stale_findings(
                config,
                project_root=root,
                pipeline_id=config.name,
            )
        )
    return findings
