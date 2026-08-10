"""Deterministic pipeline structure checks (CLI / CI / Cursor hooks)."""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config, resolve_path
from det.runtime.ids import dbt_model_slug
from det.runtime.pipelines import (
    discover_pipeline_files,
    resolve_pipeline_ref,
    resolve_project_root,
)
from det.runtime.registry import list_sources
from det.validation.jsonschema_validator import load_json_schema

Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class Finding:
    severity: Severity
    code: str
    pipeline: str
    detail: str
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _rel(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def check_pipeline_config(
    config_path: Path,
    *,
    project_root: Path,
) -> list[Finding]:
    """Check one pipeline YAML path. Always returns findings (possibly empty)."""
    root = project_root.resolve()
    findings: list[Finding] = []
    try:
        from det.runtime.pipelines import canonical_id_from_path

        pipeline_id = canonical_id_from_path(config_path, root)
    except Exception:
        pipeline_id = config_path.stem

    try:
        config = load_pipeline_config(config_path)
    except Exception as exc:
        findings.append(
            Finding(
                severity="error",
                code="invalid_pipeline",
                pipeline=pipeline_id,
                path=_rel(config_path, root),
                detail=f"failed to load pipeline YAML: {exc}",
            )
        )
        return findings

    pipeline_id = config.name
    schema_path = resolve_path(root, config.schema_path)
    if not schema_path.is_file():
        findings.append(
            Finding(
                severity="error",
                code="missing_schema",
                pipeline=pipeline_id,
                path=config.schema_path,
                detail=f"schema file not found: {config.schema_path}",
            )
        )
    else:
        try:
            schema = load_json_schema(schema_path)
        except Exception as exc:
            findings.append(
                Finding(
                    severity="error",
                    code="invalid_schema",
                    pipeline=pipeline_id,
                    path=_rel(schema_path, root),
                    detail=f"schema failed to load: {exc}",
                )
            )
            schema = None
        if schema is not None:
            if schema.get("type") != "object":
                findings.append(
                    Finding(
                        severity="error",
                        code="invalid_schema",
                        pipeline=pipeline_id,
                        path=_rel(schema_path, root),
                        detail="schema type must be 'object'",
                    )
                )
            props = schema.get("properties")
            if props is not None and not isinstance(props, dict):
                findings.append(
                    Finding(
                        severity="error",
                        code="invalid_schema",
                        pipeline=pipeline_id,
                        path=_rel(schema_path, root),
                        detail="schema properties must be a mapping when present",
                    )
                )

    load_plugins()
    sources = set(list_sources())
    if config.source.type not in sources:
        findings.append(
            Finding(
                severity="error",
                code="unknown_source",
                pipeline=pipeline_id,
                path=_rel(config_path, root),
                detail=(
                    f"source.type {config.source.type!r} is not registered; "
                    f"known: {', '.join(sorted(sources)) or '(none)'}"
                ),
            )
        )

    dbt_root = root / "dbt"
    if dbt_root.is_dir():
        slug = dbt_model_slug(config.bronze_dataset())
        silver_dir = dbt_root / "models" / "silver"
        missing: list[str] = []
        for name in (f"stg_{slug}.sql", f"silver_{slug}.sql"):
            if not (silver_dir / name).is_file():
                missing.append(str(Path("dbt/models/silver") / name))
        if missing:
            findings.append(
                Finding(
                    severity="warning",
                    code="missing_dbt_models",
                    pipeline=pipeline_id,
                    path=_rel(silver_dir, root),
                    detail=(
                        "dbt silver models missing (scaffold with "
                        f"`det scaffold-dbt -p {pipeline_id}`): {', '.join(missing)}"
                    ),
                )
            )

    return findings


def check_project(
    project_root: Path | str | None = None,
    *,
    pipeline: str | None = None,
) -> list[Finding]:
    """
    Check all pipelines (or one) under the project.

    Errors: load / schema / registered source.
    Warnings: missing dbt stg/silver when ``dbt/`` exists.
    """
    root = resolve_project_root(project_root)
    load_plugins()
    findings: list[Finding] = []

    if pipeline is not None:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        return check_pipeline_config(resolved.path, project_root=root)

    paths = discover_pipeline_files(root)
    if not paths:
        findings.append(
            Finding(
                severity="error",
                code="no_pipelines",
                pipeline="*",
                path="configs/pipelines",
                detail="no pipeline YAML files under configs/pipelines/",
            )
        )
        return findings

    for path in paths:
        findings.extend(check_pipeline_config(path, project_root=root))
    return findings


def has_errors(findings: Sequence[Finding]) -> bool:
    return any(f.severity == "error" for f in findings)


def has_warnings(findings: Sequence[Finding]) -> bool:
    return any(f.severity == "warning" for f in findings)


def format_findings(findings: Sequence[Finding]) -> str:
    if not findings:
        return "OK: no structure findings"
    lines: list[str] = []
    for f in findings:
        loc = f" path={f.path}" if f.path else ""
        lines.append(f"{f.severity.upper()} [{f.code}] {f.pipeline}{loc}: {f.detail}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """``python -m det.runtime.check`` entry (used by Cursor hook)."""
    import argparse

    parser = argparse.ArgumentParser(description="DET pipeline structure check")
    parser.add_argument("--project-root", type=Path, default=None)
    parser.add_argument("--pipeline", default=None)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    findings = check_project(args.project_root, pipeline=args.pipeline)
    if args.json:
        # Avoid listing all pipelines from cwd when project-root set
        payload = {
            "ok": not has_errors(findings),
            "error_count": sum(1 for f in findings if f.severity == "error"),
            "warning_count": sum(1 for f in findings if f.severity == "warning"),
            "findings": [f.to_dict() for f in findings],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(format_findings(findings))

    if has_errors(findings):
        return 1
    if args.strict and has_warnings(findings):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
