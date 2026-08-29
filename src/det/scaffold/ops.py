"""Scaffold DET ops dbt models/tests/macros into an embedder project."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from det.logging import get_logger
from det.scaffold.dbt import (
    _bootstrap_generate_schema_name,
    _write_or_skip,
    _write_slo_seed,
)
from det.scaffold.dbt_sql import ScaffoldAction, ScaffoldResult

logger = get_logger(__name__)

_TEMPLATES_OPS = Path(__file__).resolve().parent / "templates" / "ops"

# Relative destinations under {project_root}/dbt/ → template resource names.
_OPS_MODEL_FILES: tuple[tuple[str, str], ...] = (
    ("models/ops/sources.yml", "models/sources.yml"),
    ("models/ops/stg_det__run_receipts.sql", "models/stg_det__run_receipts.sql"),
    ("models/ops/det__ops_run_daily.sql", "models/det__ops_run_daily.sql"),
    ("models/ops/_ops__models.yml", "models/_ops__models.yml"),
)

_OPS_TEST_FILES: tuple[tuple[str, str], ...] = (
    ("tests/ops/assert_ops_slo_error_rate.sql", "tests/assert_ops_slo_error_rate.sql"),
    ("tests/ops/assert_ops_slo_fail_closed.sql", "tests/assert_ops_slo_fail_closed.sql"),
    ("tests/ops/assert_ops_slo_p95.sql", "tests/assert_ops_slo_p95.sql"),
    ("tests/ops/assert_ops_slo_recency.sql", "tests/assert_ops_slo_recency.sql"),
)

# DET-owned; --force may refresh. generate_schema_name is bootstrapped separately
# (create-if-missing only — never overwrite embedder customizations).
_OPS_MACRO_FILES: tuple[tuple[str, str], ...] = (
    ("macros/det_sql_compat.sql", "macros/det_sql_compat.sql"),
)

_GENERATE_SCHEMA_NAME_PAIR: tuple[str, str] = (
    "macros/generate_schema_name.sql",
    "macros/generate_schema_name.sql",
)

_MINIMAL_DBT_PROJECT: dict[str, Any] = {
    "name": "analytics",
    "version": "1.0.0",
    "config-version": 2,
    "profile": "analytics",
    "model-paths": ["models"],
    "analysis-paths": ["analyses"],
    "test-paths": ["tests"],
    "seed-paths": ["seeds"],
    "macro-paths": ["macros"],
    "snapshot-paths": ["snapshots"],
    "clean-targets": ["target", "dbt_packages"],
}

_OPS_MODEL_CONFIG: dict[str, Any] = {
    "+tags": ["ops"],
    "+schema": "ops",
    # Table (not view): DuckDB iceberg_scan is cwd-sensitive; BQ ops uses BigLake.
    "+materialized": "table",
}

_OPS_SEED_CONFIG: dict[str, Any] = {
    "+schema": "ops",
    "+tags": ["ops"],
    "+column_types": {
        "pipeline": "{% if target.name == 'bigquery' %}string{% else %}varchar{% endif %}",
        "command": "{% if target.name == 'bigquery' %}string{% else %}varchar{% endif %}",
        "cadence": "{% if target.name == 'bigquery' %}string{% else %}varchar{% endif %}",
        "recency_hours": (
            "{% if target.name == 'bigquery' %}int64{% else %}integer{% endif %}"
        ),
        "score_hours": (
            "{% if target.name == 'bigquery' %}int64{% else %}integer{% endif %}"
        ),
        "max_error_rate": (
            "{% if target.name == 'bigquery' %}float64{% else %}double{% endif %}"
        ),
        "p95_ms": "{% if target.name == 'bigquery' %}int64{% else %}integer{% endif %}",
    },
}

_OPS_PROFILE_OUTPUT: dict[str, Any] = {
    "type": "duckdb",
    # File stem must not be `ops` — DuckDB names the catalog after the file,
    # which collides with +schema: ops (`ops.stg_…` becomes ambiguous).
    "path": "{{ env_var('DET_OPS_DUCKDB', '../data/det_ops.duckdb') }}",
    "schema": "main",
    "threads": 1,
    "extensions": ["httpfs", "iceberg"],
}

_MINIMAL_PROFILES: dict[str, Any] = {
    "analytics": {
        "target": "duckdb",
        "outputs": {
            "duckdb": {
                "type": "duckdb",
                "path": "{{ env_var('DET_ANALYTICS_DUCKDB', '../data/analytics.duckdb') }}",
                "schema": "main",
                "threads": 1,
                "extensions": ["httpfs", "iceberg"],
            },
            "ops": dict(_OPS_PROFILE_OUTPUT),
        },
    }
}


def _ensure_under_root(path: Path, *, root: Path) -> Path:
    """Resolve ``path`` and reject destinations that escape ``root``."""
    resolved = path.expanduser().resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"scaffold path escapes project root {root}: {resolved}")
    return resolved


def _dbt_profile_name(project_root: Path) -> str:
    """Profile key from dbt_project.yml ``profile:`` (default ``analytics``)."""
    root = project_root.resolve()
    path = _ensure_under_root(root / "dbt" / "dbt_project.yml", root=root)
    if path.is_file():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(data, dict):
            raw = data.get("profile")
            if raw is not None and str(raw).strip():
                return str(raw).strip()
    return str(_MINIMAL_DBT_PROJECT["profile"])


def _minimal_profiles_for(profile_name: str) -> dict[str, Any]:
    if profile_name == "analytics":
        return dict(_MINIMAL_PROFILES)
    return {
        profile_name: {
            "target": "duckdb",
            "outputs": {
                "duckdb": {
                    "type": "duckdb",
                    "path": "{{ env_var('DET_ANALYTICS_DUCKDB', '../data/analytics.duckdb') }}",
                    "schema": "main",
                    "threads": 1,
                    "extensions": ["httpfs", "iceberg"],
                },
                "ops": dict(_OPS_PROFILE_OUTPUT),
            },
        }
    }


def ops_template_root() -> Path:
    """Return the on-disk root for packaged ops templates."""
    return _TEMPLATES_OPS


def load_ops_template(rel: str) -> str:
    """Read a packaged ops template file (POSIX path under templates/ops/)."""
    path = _TEMPLATES_OPS.joinpath(*rel.split("/"))
    return path.read_text(encoding="utf-8")


def iter_ops_template_pairs() -> list[tuple[str, Path, Path]]:
    """Return (label, canonical_repo_path, template_path) for drift tests."""
    repo = Path(__file__).resolve().parents[3]
    pairs: list[tuple[str, Path, Path]] = []
    for dest, tmpl in (
        *_OPS_MODEL_FILES,
        *_OPS_TEST_FILES,
        *_OPS_MACRO_FILES,
        _GENERATE_SCHEMA_NAME_PAIR,
    ):
        pairs.append((f"dbt/{dest}", repo / f"dbt/{dest}", _TEMPLATES_OPS / tmpl))
    return pairs


def _dump_yaml(data: dict[str, Any]) -> str:
    return yaml.safe_dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _ensure_dbt_project(
    project_root: Path,
    *,
    force: bool,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    """Create or merge dbt_project.yml so ops models/seeds are configured."""
    root = project_root.resolve()
    path = _ensure_under_root(root / "dbt" / "dbt_project.yml", root=root)
    if path.exists():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            data = {}
    else:
        data = dict(_MINIMAL_DBT_PROJECT)

    models = data.setdefault("models", {})
    if not isinstance(models, dict):
        models = {}
        data["models"] = models
    project_name = str(data.get("name") or "analytics")
    proj_models = models.setdefault(project_name, {})
    if not isinstance(proj_models, dict):
        proj_models = {}
        models[project_name] = proj_models
    # Always refresh ops block keys (idempotent merge).
    ops_block = proj_models.setdefault("ops", {})
    if not isinstance(ops_block, dict):
        ops_block = {}
        proj_models["ops"] = ops_block
    ops_block.update(_OPS_MODEL_CONFIG)

    seeds = data.setdefault("seeds", {})
    if not isinstance(seeds, dict):
        seeds = {}
        data["seeds"] = seeds
    proj_seeds = seeds.setdefault(project_name, {})
    if not isinstance(proj_seeds, dict):
        proj_seeds = {}
        seeds[project_name] = proj_seeds
    seed_block = proj_seeds.setdefault("ops_slo_expected", {})
    if not isinstance(seed_block, dict):
        seed_block = {}
        proj_seeds["ops_slo_expected"] = seed_block
    seed_block.update(_OPS_SEED_CONFIG)

    content = _dump_yaml(data)
    # dbt_project is always merged/updated when missing keys; treat like force for
    # the ops sections — write whenever content would change or file is new.
    exists = path.exists()
    if exists and path.read_text(encoding="utf-8") == content:
        actions.append(ScaffoldAction(path=path, action="skip", detail="unchanged"))
        return
    if dry_run:
        actions.append(
            ScaffoldAction(
                path=path,
                action="would_write",
                detail="overwrite" if exists else "create",
            )
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    actions.append(
        ScaffoldAction(
            path=path,
            action="write",
            detail="overwrite" if exists else "create",
        )
    )
    logger.info("scaffolded file", path=str(path), detail=actions[-1].detail)
    _ = force  # force applies to template copies; project YAML always merges


def _ensure_profiles(
    project_root: Path,
    *,
    dry_run: bool,
    actions: list[ScaffoldAction],
) -> None:
    """Create or patch profiles.yml so ``{profile}.outputs.ops`` exists."""
    root = project_root.resolve()
    path = _ensure_under_root(root / "dbt" / "profiles.yml", root=root)
    profile_name = _dbt_profile_name(root)
    if path.exists():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            data = {}
    else:
        data = _minimal_profiles_for(profile_name)

    profile = data.setdefault(profile_name, {})
    if not isinstance(profile, dict):
        profile = {}
        data[profile_name] = profile
    profile.setdefault("target", "duckdb")
    outputs = profile.setdefault("outputs", {})
    if not isinstance(outputs, dict):
        outputs = {}
        profile["outputs"] = outputs
    if "ops" not in outputs or not isinstance(outputs.get("ops"), dict):
        outputs["ops"] = dict(_OPS_PROFILE_OUTPUT)
    else:
        # Fill missing keys only — do not clobber custom ops output paths.
        for key, value in _OPS_PROFILE_OUTPUT.items():
            outputs["ops"].setdefault(key, value)

    content = _dump_yaml(data)
    exists = path.exists()
    if exists and path.read_text(encoding="utf-8") == content:
        actions.append(ScaffoldAction(path=path, action="skip", detail="unchanged"))
        return
    if dry_run:
        actions.append(
            ScaffoldAction(
                path=path,
                action="would_write",
                detail="overwrite" if exists else "create",
            )
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    actions.append(
        ScaffoldAction(
            path=path,
            action="write",
            detail="overwrite" if exists else "create",
        )
    )
    logger.info("scaffolded file", path=str(path), detail=actions[-1].detail)


def scaffold_ops(
    *,
    project_root: Path,
    force: bool = False,
    dry_run: bool = False,
) -> ScaffoldResult:
    """
    Emit ops dbt models, tests, macros, project/profile wiring, and SLO seed.

    Template copies are create-if-missing unless ``force``. ``generate_schema_name``
    is create-if-missing only (never overwritten). The SLO seed is always
    regenerated. ``dbt_project.yml`` / ``profiles.yml`` are merged so ops config
    exists without wiping unrelated keys.
    """
    from det.runtime.slo import SLO_SEED_RELPATH

    root = project_root.resolve()
    dbt_root = root / "dbt"
    actions: list[ScaffoldAction] = []

    for dest_rel, tmpl_rel in (
        *_OPS_MODEL_FILES,
        *_OPS_TEST_FILES,
        *_OPS_MACRO_FILES,
    ):
        dest = _ensure_under_root(dbt_root / dest_rel, root=root)
        content = load_ops_template(tmpl_rel)
        _write_or_skip(
            dest,
            content,
            force=force,
            dry_run=dry_run,
            actions=actions,
        )

    _bootstrap_generate_schema_name(root, dry_run=dry_run, actions=actions)
    _ensure_dbt_project(root, force=force, dry_run=dry_run, actions=actions)
    _ensure_profiles(root, dry_run=dry_run, actions=actions)
    _ensure_under_root(root / SLO_SEED_RELPATH, root=root)
    _write_slo_seed(root, dry_run=dry_run, actions=actions)

    return ScaffoldResult(dataset="ops", actions=actions)
