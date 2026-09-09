"""Lake root resolution (layout 2 only: derived or explicit split)."""

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

ENV_LAKE_LAYOUT = "DET_LAKE_LAYOUT"

_LAYOUT1_REMOVED = (
    "lake layout 1 was removed in det-elt 0.9.0; only layout 2 is supported "
    "(derived from DET_LAKE_PATH or explicit DET_LAKE_PATH_RAW/_BRONZE/_OPS)"
)


@dataclass(frozen=True)
class LakeRoots:
    """Resolved raw / bronze / ops lake roots for one process.

    Layout 2: each layer is an independent URI (flattened
    ``{provider}/{source}_vN`` under the layer root). Derived mode sets
    raw/bronze under the parent and ops = parent.
    """

    raw: LakeRef
    bronze: LakeRef
    ops: LakeRef
    layout: int
    unified_spec: str | None = None

    @property
    def is_split(self) -> bool:
        return self.layout >= 2


def _join_lake_child(parent_spec: str, child: str) -> str:
    """Append a path segment to a lake URI or filesystem path."""
    text = (parent_spec or "").strip().rstrip("/")
    name = (child or "").strip().strip("/")
    if not name:
        raise ValueError("lake child segment must be non-empty")
    if not text:
        return name
    if text.startswith("memory://"):
        rest = text[len("memory://") :].rstrip("/")
        return f"memory://{rest}/{name}" if rest else f"memory://{name}"
    for scheme in _OBJECT_SCHEMES:
        if text.startswith(scheme):
            return f"{text}/{name}"
    return str(Path(text) / name)


def lake_layout_preference(
    settings: DetSettings | None = None,
    *,
    cli_lake_layout: int | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    """Return preferred lake layout (always 2). Rejects layout 1."""
    del settings  # layout is env/CLI only; settings no longer carry lake_layout
    if cli_lake_layout is not None:
        return _validate_layout_value(cli_lake_layout, where="--lake-layout")
    return lake_layout_from_env(env)


def lake_layout_from_env(env: Mapping[str, str] | None = None) -> int:
    """Parse ``DET_LAKE_LAYOUT``. Unset, empty, or ``2`` → ``2``; ``1`` errors."""
    environ = os.environ if env is None else env
    raw = (environ.get(ENV_LAKE_LAYOUT) or "").strip()
    if not raw:
        return 2
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{ENV_LAKE_LAYOUT} must be 2 (layout 1 removed), got {raw!r}"
        ) from exc
    return _validate_layout_value(value, where=ENV_LAKE_LAYOUT)


def _validate_layout_value(value: object, *, where: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{where} must be an int, got {value!r}")
    if value == 1:
        raise ValueError(f"{where}: {_LAYOUT1_REMOVED}")
    if value != 2:
        raise ValueError(f"{where} must be 2, got {value!r}")
    return value


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
    """True when any explicit layout-2 layer root is set (CLI / settings / env)."""
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
        raise ValueError(_LAYOUT1_REMOVED)
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


@dataclass(frozen=True)
class LakeRootSpecs:
    """URI/path strings for lake layers without opening backends."""

    layout: int
    raw: str
    bronze: str
    ops: str
    unified_spec: str | None = None

    @property
    def is_split(self) -> bool:
        return self.layout >= 2


def validate_lake_root_specs(
    specs: LakeRootSpecs,
    *,
    mode: LakeMode | None = None,
) -> None:
    """Raise ``ValueError`` when specs disagree with mode or split URI kinds."""
    if specs.layout < 2:
        raise ValueError(_LAYOUT1_REMOVED)
    kinds: dict[str, str] = {}
    for name, spec in (
        ("raw", specs.raw),
        ("bronze", specs.bronze),
        ("ops", specs.ops),
    ):
        if mode is not None:
            validate_lake_mode(spec, mode)
        kinds[name] = _lake_uri_kind(spec)
    if len(set(kinds.values())) > 1:
        raise ValueError(
            "split lake roots must share the same URI kind "
            f"(local / s3 / gs / gcs / memory); got {kinds}"
        )


def resolve_lake_root_specs(
    settings: DetSettings | None = None,
    *,
    project_root: Path | None = None,
    cli_lake_path: str | None = None,
    cli_lake_path_raw: str | None = None,
    cli_lake_path_bronze: str | None = None,
    cli_lake_path_ops: str | None = None,
    cli_lake_layout: int | None = None,
    destination_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> LakeRootSpecs:
    """
    Resolve lake layer URI strings (no ``open_lake``).

    Layout 2 only: explicit three roots, or derive from ``DET_LAKE_PATH``.
    ``destination_path`` is ignored (kept for call-site compatibility).
    ``cli_lake_layout`` is validated (must be unset or 2).
    """
    from det.runtime.settings import get_active_settings

    del destination_path  # never selects the lake root (layout 2 only)
    active = settings if settings is not None else get_active_settings()
    environ = os.environ if env is None else env
    mode: LakeMode | None = None
    if active is not None:
        mode = active.lake_mode
    mode = mode if mode is not None else lake_mode_from_env(environ)
    # Reject DET_LAKE_LAYOUT=1 / --lake-layout 1 early.
    lake_layout_preference(active, cli_lake_layout=cli_lake_layout, env=environ)

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
        specs = LakeRootSpecs(
            layout=2,
            raw=raw_spec,  # type: ignore[arg-type]
            bronze=bronze_spec,  # type: ignore[arg-type]
            ops=ops_spec,  # type: ignore[arg-type]
            unified_spec=None,
        )
        validate_lake_root_specs(specs, mode=mode)
        return specs

    override = cli_lake_path
    settings_lake: str | None = None
    if active is not None:
        if not _strip_spec(override):
            override = active.lake_override
        settings_lake = active.lake_path
    parent = pick_lake_spec(
        cli_lake_path=override,
        destination_path=None,
        settings_lake_path=settings_lake,
        env=environ,
    )

    specs = LakeRootSpecs(
        layout=2,
        raw=_join_lake_child(parent, "raw"),
        bronze=_join_lake_child(parent, "bronze"),
        ops=parent,
        unified_spec=None,
    )
    validate_lake_root_specs(specs, mode=mode)
    return specs


def resolve_lake_roots(
    settings: DetSettings | None = None,
    *,
    project_root: Path | None = None,
    cli_lake_path: str | None = None,
    cli_lake_path_raw: str | None = None,
    cli_lake_path_bronze: str | None = None,
    cli_lake_path_ops: str | None = None,
    cli_lake_layout: int | None = None,
    destination_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> LakeRoots:
    """
    Resolve process-wide lake roots from settings / env / CLI.

    Layout **2** only:
    - Explicit ``DET_LAKE_PATH_{RAW,BRONZE,OPS}`` (all three) → split.
    - Else derive ``{DET_LAKE_PATH}/raw``, ``…/bronze``, ops = parent.
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

    specs = resolve_lake_root_specs(
        active,
        project_root=root,
        cli_lake_path=cli_lake_path,
        cli_lake_path_raw=cli_lake_path_raw,
        cli_lake_path_bronze=cli_lake_path_bronze,
        cli_lake_path_ops=cli_lake_path_ops,
        cli_lake_layout=cli_lake_layout,
        destination_path=destination_path,
        env=environ,
    )
    roots = LakeRoots(
        raw=open_lake(specs.raw, root, lake_mode=mode, env=environ),
        bronze=open_lake(specs.bronze, root, lake_mode=mode, env=environ),
        ops=open_lake(specs.ops, root, lake_mode=mode, env=environ),
        layout=specs.layout,
        unified_spec=None,
    )
    validate_lake_roots(roots, mode=mode)
    return roots
