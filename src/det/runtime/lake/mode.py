"""Lake mode policy and URI/path helpers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Literal

DEFAULT_LAKE_REL = "./data/lake"
ENV_LAKE_MODE = "DET_LAKE_MODE"
ENV_LAKE_PATH_RAW = "DET_LAKE_PATH_RAW"
ENV_LAKE_PATH_BRONZE = "DET_LAKE_PATH_BRONZE"
ENV_LAKE_PATH_OPS = "DET_LAKE_PATH_OPS"
LakeMode = Literal["local", "cloud"]
_OBJECT_SCHEMES = ("s3://", "gs://", "gcs://")


def is_lake_uri(spec: str) -> bool:
    text = (spec or "").strip()
    return text.startswith((*_OBJECT_SCHEMES, "memory://"))


def is_object_lake_spec(spec: str) -> bool:
    text = (spec or "").strip()
    return text.startswith(_OBJECT_SCHEMES)


def lake_mode_from_env(env: Mapping[str, str] | None = None) -> LakeMode:
    """Parse ``DET_LAKE_MODE``. Unset or empty → ``local``."""
    environ = os.environ if env is None else env
    raw = (environ.get(ENV_LAKE_MODE) or "").strip().lower()
    if not raw:
        return "local"
    if raw in {"local", "cloud"}:
        return raw  # type: ignore[return-value]
    raise ValueError(
        f"{ENV_LAKE_MODE} must be 'local' or 'cloud', got {raw!r}"
    )


def validate_lake_mode(spec: str, mode: LakeMode) -> None:
    """Raise ``ValueError`` when lake URI shape disagrees with ``DET_LAKE_MODE``."""
    text = (spec or "").strip() or DEFAULT_LAKE_REL
    if mode == "local":
        if is_object_lake_spec(text):
            raise ValueError(
                f"DET_LAKE_MODE=local forbids object-store lakes "
                f"(got {text!r}); use a filesystem path or set "
                f"{ENV_LAKE_MODE}=cloud"
            )
        return
    # cloud
    if text.startswith("memory://"):
        raise ValueError(
            f"DET_LAKE_MODE=cloud forbids memory:// lakes (got {text!r}); "
            f"use s3://, gs://, or gcs://, or set {ENV_LAKE_MODE}=local"
        )
    if not is_object_lake_spec(text):
        raise ValueError(
            f"DET_LAKE_MODE=cloud requires an s3://, gs://, or gcs:// lake "
            f"(got {text!r}); set {ENV_LAKE_MODE}=local for filesystem lakes"
        )


def pick_lake_spec(
    *,
    cli_lake_path: str | None = None,
    destination_path: str | None = None,
    settings_lake_path: str | None = None,
    env: Mapping[str, str] | None = None,
) -> str:
    """
    Resolve the lake root spec (URI or local path). First hit wins:

    1. CLI ``--lake-path`` / ``DetSettings.lake_override``
    2. ``DetSettings.lake_path`` (usually from ``DET_LAKE_PATH`` via ``from_env``)
    3. ``DET_LAKE_PATH``
    4. ``./data/lake``

    ``destination_path`` is ignored (layout 2 only; kept for call-site compat).
    """
    del destination_path
    if cli_lake_path is not None and str(cli_lake_path).strip():
        return str(cli_lake_path).strip()
    if settings_lake_path is not None and str(settings_lake_path).strip():
        return str(settings_lake_path).strip()
    environ = os.environ if env is None else env
    env_val = environ.get("DET_LAKE_PATH")
    if env_val is not None and str(env_val).strip():
        return str(env_val).strip()
    return DEFAULT_LAKE_REL


def _strip_spec(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
