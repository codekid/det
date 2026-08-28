"""Deterministic pipeline structure checks (CLI / CI / Cursor hooks).

See docs/contract-triangle.md for how schema YAML, dbt sources, and pipeline
dbt.stg knobs stay aligned (``det check --strict`` catches scaffold drift).
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from det.plugins import load_plugins
from det.runtime.config import load_pipeline_config, resolve_path
from det.runtime.discovery import PluginLoadError
from det.runtime.ids import dbt_model_slug
from det.runtime.pipelines import (
    discover_pipeline_files,
    resolve_pipeline_ref,
    resolve_project_root,
)
from det.runtime.registry import get_source, list_sources
from det.runtime.secrets import looks_like_passwordful_uri, uri_has_userinfo
from det.validation.jsonschema_validator import load_json_schema

Severity = Literal["error", "warning"]

# Keys whose value in YAML is a credential, not a name (auth_env stays legal).
_CREDENTIAL_KEY_NAMES = frozenset(
    {"token", "password", "secret", "api_key", "apikey", "client_secret", "dsn"}
)


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
    sources = set(list_sources(project_root=root))
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
    else:
        try:
            get_source(config.source.type, project_root=root)
        except PluginLoadError as exc:
            findings.append(
                Finding(
                    severity="error",
                    code="plugin_load_error",
                    pipeline=pipeline_id,
                    path=_rel(config_path, root),
                    detail=str(exc),
                )
            )

    findings.extend(
        _secret_findings(
            config,
            pipeline_id=pipeline_id,
            config_rel=_rel(config_path, root),
        )
    )
    if config.ingestion.library == "dlt":
        findings.append(
            Finding(
                severity="warning",
                code="ingestion_library_dlt_deprecated",
                pipeline=pipeline_id,
                path=_rel(config_path, root),
                detail=(
                    "ingestion.library: dlt is deprecated; use ingestion.library: det instead "
                    "(the dlt alias will be removed in a future release)"
                ),
            )
        )
    findings.extend(_dlt_lake_findings(config, project_root=root, pipeline_id=pipeline_id))

    dbt_root = root / "dbt"
    if dbt_root.is_dir():
        slug = dbt_model_slug(config.name)
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
        else:
            findings.extend(
                _scaffold_sql_stale_findings(
                    config,
                    project_root=root,
                    pipeline_id=pipeline_id,
                )
            )

    return findings


def _normalize_scaffold_text(text: str) -> str:
    text = text.replace("\r\n", "\n")
    if text and not text.endswith("\n"):
        text += "\n"
    return text


def _scaffold_sql_stale_findings(
    config: Any,
    *,
    project_root: Path,
    pipeline_id: str,
) -> list[Finding]:
    """Warn when on-disk silver SQL no longer matches scaffold templates + knobs."""
    from det.scaffold.dbt import expected_silver_sql

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
            # Existence is covered by missing_dbt_models for the root silver;
            # relation silver gaps are still useful to surface here.
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
        on_disk = _normalize_scaffold_text(path.read_text(encoding="utf-8"))
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


def _credential_literals(value: Any, *, prefix: str = "") -> list[str]:
    """Dotted paths of credential-named keys holding a literal string value."""
    hits: list[str] = []
    if isinstance(value, dict):
        for key, val in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            lowered = str(key).lower()
            is_credential = lowered in _CREDENTIAL_KEY_NAMES or lowered.endswith(
                ("_token", "_password", "_secret", "_api_key")
            )
            if is_credential and isinstance(val, str) and val.strip():
                hits.append(path)
                continue
            hits.extend(_credential_literals(val, prefix=path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            hits.extend(_credential_literals(item, prefix=f"{prefix}[{index}]"))
    return hits


def _dlt_lake_findings(
    config: Any,
    *,
    project_root: Path,
    pipeline_id: str,
) -> list[Finding]:
    """Flag leftover dlt state tables / paths under this pipeline's lake prefixes."""
    from det.destinations.models import bronze_dataset_dir, raw_dataset_dir
    from det.runtime.dlt_hygiene import dlt_hygiene_message, lake_dlt_path_hits

    findings: list[Finding] = []
    try:
        raw_ds = raw_dataset_dir(config, project_root)
        bronze_ds = bronze_dataset_dir(config, project_root)
    except Exception:
        return findings

    hits = lake_dlt_path_hits(raw_ds, bronze_ds)
    for path_str in hits:
        findings.append(
            Finding(
                severity="error",
                code="dlt_state_on_lake",
                pipeline=pipeline_id,
                path=path_str,
                detail=dlt_hygiene_message(f"found path {path_str}", surface="Lake"),
            )
        )
    return findings


def _secret_findings(
    config: Any,
    *,
    pipeline_id: str,
    config_rel: str,
) -> list[Finding]:
    """Credentials must be names in YAML, never values. Details never echo a value."""
    findings: list[Finding] = []
    dest = config.destination
    connection = (dest.connection or "").strip()

    if dest.type == "postgres" and connection:
        if looks_like_passwordful_uri(connection):
            findings.append(
                Finding(
                    severity="error",
                    code="secret_in_config",
                    pipeline=pipeline_id,
                    path=config_rel,
                    detail=(
                        "destination.connection embeds a password. Export the DSN "
                        "and set destination.connection_env: DET_POSTGRES_DSN"
                    ),
                )
            )
        else:
            findings.append(
                Finding(
                    severity="warning",
                    code="secret_in_config",
                    pipeline=pipeline_id,
                    path=config_rel,
                    detail=(
                        "destination.connection holds a DSN in git. Prefer "
                        "destination.connection_env: DET_POSTGRES_DSN"
                    ),
                )
            )

    if uri_has_userinfo(dest.path):
        findings.append(
            Finding(
                severity="error",
                code="secret_in_config",
                pipeline=pipeline_id,
                path=config_rel,
                detail=(
                    "destination.path embeds object-store credentials. Use the "
                    "provider credential chain (env / IAM role) instead"
                ),
            )
        )

    for dotted in _credential_literals(config.source.overrides):
        findings.append(
            Finding(
                severity="error",
                code="secret_in_config",
                pipeline=pipeline_id,
                path=config_rel,
                detail=(
                    f"source.overrides.{dotted} looks like a credential value. "
                    "Declare a name instead (auth_env) and export the secret"
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

    Errors: load / schema / discovered source; ``slo_seed_stale`` when the ops SLO
    seed does not match pipeline YAML (full-project check only);
    ``lake_mode_mismatch`` when ``DET_LAKE_MODE`` disagrees with the lake URI.
    Warnings: missing dbt stg/silver when ``dbt/`` exists;
    ``scaffold_sql_stale`` when silver SQL drifts from scaffold templates;
    ``lake_cloud_experimental`` when ``DET_LAKE_MODE=cloud``.
    """
    root = resolve_project_root(project_root)
    load_plugins()
    findings: list[Finding] = []

    if pipeline is not None:
        resolved = resolve_pipeline_ref(pipeline, project_root=root)
        return check_pipeline_config(resolved.path, project_root=root)

    findings.extend(_lake_mode_findings())

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
    findings.extend(_slo_seed_findings(root))
    return findings


def _lake_mode_findings() -> list[Finding]:
    """Validate DET_LAKE_MODE against the resolved lake URI (no object I/O)."""
    from det.runtime.lake import (
        lake_mode_from_env,
        pick_lake_spec,
        validate_lake_mode,
    )

    findings: list[Finding] = []
    try:
        mode = lake_mode_from_env()
    except ValueError as exc:
        findings.append(
            Finding(
                severity="error",
                code="lake_mode_invalid",
                pipeline="*",
                path=None,
                detail=str(exc),
            )
        )
        return findings

    spec = pick_lake_spec()
    try:
        validate_lake_mode(spec, mode)
    except ValueError as exc:
        findings.append(
            Finding(
                severity="error",
                code="lake_mode_mismatch",
                pipeline="*",
                path=spec,
                detail=str(exc),
            )
        )
        return findings

    if mode == "cloud":
        findings.append(
            Finding(
                severity="warning",
                code="lake_cloud_experimental",
                pipeline="*",
                path=spec,
                detail=(
                    "DET_LAKE_MODE=cloud: CI covers MinIO extract→Iceberg→"
                    "iceberg_scan; multi-writer object lakes and Glue/REST "
                    "catalogs are still out of scope."
                ),
            )
        )
    return findings


def _slo_seed_findings(root: Path) -> list[Finding]:
    from det.runtime.slo import SLO_SEED_RELPATH, slo_seed_is_stale

    if not slo_seed_is_stale(root):
        return []
    return [
        Finding(
            severity="error",
            code="slo_seed_stale",
            pipeline="*",
            path=str(SLO_SEED_RELPATH),
            detail=(
                "dbt/seeds/ops_slo_expected.csv does not match pipeline YAML slo: "
                "rows. Regenerate with `det scaffold-dbt` (any pipeline; seed is "
                "always rewritten from all pipelines)."
            ),
        )
    ]


def has_errors(findings: Sequence[Finding]) -> bool:
    return any(f.severity == "error" for f in findings)


def has_warnings(findings: Sequence[Finding]) -> bool:
    return any(f.severity == "warning" for f in findings)


def findings_payload(findings: Sequence[Finding]) -> dict[str, Any]:
    """JSON shape for ``det check --json`` and the MCP ``check`` tool."""
    return {
        "ok": not has_errors(findings),
        "error_count": sum(1 for f in findings if f.severity == "error"),
        "warning_count": sum(1 for f in findings if f.severity == "warning"),
        "findings": [f.to_dict() for f in findings],
    }


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
        print(json.dumps(findings_payload(findings), indent=2))
    else:
        print(format_findings(findings))

    if has_errors(findings):
        return 1
    if args.strict and has_warnings(findings):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
