"""Lake root resolution (unified layout 1 vs split layout 2)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from det.runtime.lake.mode import (
    _OBJECT_SCHEMES,
    ENV_LAKE_PATH_BRONZE,
    ENV_LAKE_PATH_OPS,
    ENV_LAKE_PATH_RAW,
    LakeMode,
    _strip_spec,
    lake_mode_from_env,
    pick_lake_spec,
    validate_lake_mode,
)
from det.runtime.lake.open import open_lake
from det.runtime.lake.ref import LakeRef

if TYPE_CHECKING:
    from det.runtime.settings import DetSettings


@dataclass(frozen=True)
class LakeRoots:
    """Resolved raw / bronze / ops lake roots for one process.

    Layout 1: all three refs share one unified root; dataset dirs use medallion
    prefixes under that root. Layout 2: each layer is an independent URI
    (flattened ``{provider}/{source}_vN`` under the layer root).
    """

    raw: LakeRef
    bronze: LakeRef
    ops: LakeRef
    layout: int
    unified_spec: str | None = None

    @property
    def is_split(self) -> bool:
        return self.layout >= 2


def _layer_spec(
    *,
    cli_override: str | None,
    settings_value: str | None,
    env_key: str,
    env: Mapping[str, str],
) -> str | None:
    for candidate in (cli_override, settings_value, env.get(env_key)):
        text = _strip_spec(candidate if isinstance(candidate, str) else None)
        if text is not None:
            return text
        if candidate is not None and not isinstance(candidate, str):
            text = _strip_spec(str(candidate))
            if text is not None:
                return text
    return None


def split_lake_specs_from_settings(
    settings: DetSettings | None = None,
    *,
    cli_lake_path_raw: str | None = None,
    cli_lake_path_bronze: str | None = None,
    cli_lake_path_ops: str | None = None,
    env: Mapping[str, str] | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Return (raw, bronze, ops) specs from CLI / settings / env (no defaults)."""
    environ = os.environ if env is None else env
    raw_s = settings.lake_path_raw if settings is not None else None
    bronze_s = settings.lake_path_bronze if settings is not None else None
    ops_s = settings.lake_path_ops if settings is not None else None
    raw_o = cli_lake_path_raw
    bronze_o = cli_lake_path_bronze
    ops_o = cli_lake_path_ops
    if settings is not None:
        if not _strip_spec(raw_o):
            raw_o = settings.lake_override_raw
        if not _strip_spec(bronze_o):
            bronze_o = settings.lake_override_bronze
        if not _strip_spec(ops_o):
            ops_o = settings.lake_override_ops
    return (
        _layer_spec(
            cli_override=raw_o,
            settings_value=raw_s,
            env_key=ENV_LAKE_PATH_RAW,
            env=environ,
        ),
        _layer_spec(
            cli_override=bronze_o,
            settings_value=bronze_s,
            env_key=ENV_LAKE_PATH_BRONZE,
            env=environ,
        ),
        _layer_spec(
            cli_override=ops_o,
            settings_value=ops_s,
            env_key=ENV_LAKE_PATH_OPS,
            env=environ,
        ),
    )


def is_split_lake_configured(
    settings: DetSettings | None = None,
    *,
    cli_lake_path_raw: str | None = None,
    cli_lake_path_bronze: str | None = None,
    cli_lake_path_ops: str | None = None,
    env: Mapping[str, str] | None = None,
) -> bool:
    """True when any layout-2 layer root is set (CLI / settings / env)."""
    raw, bronze, ops = split_lake_specs_from_settings(
        settings,
        cli_lake_path_raw=cli_lake_path_raw,
        cli_lake_path_bronze=cli_lake_path_bronze,
        cli_lake_path_ops=cli_lake_path_ops,
        env=env,
    )
    return any(v is not None for v in (raw, bronze, ops))


def _lake_uri_kind(spec: str) -> str:
    """Classify a lake URI for split-root consistency (scheme-specific)."""
    text = (spec or "").strip()
    if text.startswith("memory://"):
        return "memory"
    for scheme in _OBJECT_SCHEMES:
        if text.startswith(scheme):
            return scheme.removesuffix("://")
    return "local"


def validate_lake_roots(
    roots: LakeRoots,
    *,
    mode: LakeMode | None = None,
) -> None:
    """Raise ``ValueError`` when split roots are incomplete or scheme-mismatched."""
    if roots.layout < 2:
        if mode is not None and roots.unified_spec is not None:
            validate_lake_mode(roots.unified_spec, mode)
        return
    specs = {
        "raw": str(roots.raw),
        "bronze": str(roots.bronze),
        "ops": str(roots.ops),
    }
    kinds: dict[str, str] = {}
    for name, spec in specs.items():
        if mode is not None:
            validate_lake_mode(spec, mode)
        kinds[name] = _lake_uri_kind(spec)
    if len(set(kinds.values())) > 1:
        raise ValueError(
            "split lake roots must share the same URI kind "
            f"(local / s3 / gs / gcs / memory); got {kinds}"
        )


def resolve_lake_roots(
    settings: DetSettings | None = None,
    *,
    project_root: Path | None = None,
    cli_lake_path: str | None = None,
    cli_lake_path_raw: str | None = None,
    cli_lake_path_bronze: str | None = None,
    cli_lake_path_ops: str | None = None,
    destination_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> LakeRoots:
    """
    Resolve process-wide lake roots from settings / env / CLI.

    Split (layout 2) when any of ``DET_LAKE_PATH_{RAW,BRONZE,OPS}`` (or
    settings/CLI equivalents) is set — all three required. Unified (layout 1)
    otherwise; ``destination_path`` applies only in unified mode.
    """
    from det.runtime.settings import get_active_settings

    active = settings if settings is not None else get_active_settings()
    environ = os.environ if env is None else env
    root = project_root
    mode: LakeMode | None = None
    if active is not None:
        root = active.project_root if root is None else root
        mode = active.lake_mode
    if root is None:
        root = Path.cwd()
    mode = mode if mode is not None else lake_mode_from_env(environ)

    raw_spec, bronze_spec, ops_spec = split_lake_specs_from_settings(
        active,
        cli_lake_path_raw=cli_lake_path_raw,
        cli_lake_path_bronze=cli_lake_path_bronze,
        cli_lake_path_ops=cli_lake_path_ops,
        env=environ,
    )
    if any(v is not None for v in (raw_spec, bronze_spec, ops_spec)):
        missing = [
            name
            for name, spec in (
                ("DET_LAKE_PATH_RAW", raw_spec),
                ("DET_LAKE_PATH_BRONZE", bronze_spec),
                ("DET_LAKE_PATH_OPS", ops_spec),
            )
            if spec is None
        ]
        if missing:
            raise ValueError(
                "split lake mode requires all three layer roots "
                f"(raw, bronze, ops); missing {', '.join(missing)}"
            )
        # Narrowed by missing check above.
        roots = LakeRoots(
            raw=open_lake(raw_spec, root, lake_mode=mode, env=environ),  # type: ignore[arg-type]
            bronze=open_lake(bronze_spec, root, lake_mode=mode, env=environ),  # type: ignore[arg-type]
            ops=open_lake(ops_spec, root, lake_mode=mode, env=environ),  # type: ignore[arg-type]
            layout=2,
            unified_spec=None,
        )
        validate_lake_roots(roots, mode=mode)
        return roots

    override = cli_lake_path
    settings_lake: str | None = None
    if active is not None:
        if not _strip_spec(override):
            override = active.lake_override
        settings_lake = active.lake_path
    # destination.path is layout-1 only (ignored when split roots are set).
    unified = pick_lake_spec(
        cli_lake_path=override,
        destination_path=destination_path,
        settings_lake_path=settings_lake,
        env=environ,
    )
    lake = open_lake(unified, root, lake_mode=mode, env=environ)
    roots = LakeRoots(
        raw=lake,
        bronze=lake,
        ops=lake,
        layout=1,
        unified_spec=unified,
    )
    validate_lake_roots(roots, mode=mode)
    return roots
