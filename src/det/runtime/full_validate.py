"""Opt-in gate for MCP full-partition migrate dry-run (validate_limit=0)."""

from __future__ import annotations

import os
from collections.abc import Mapping

ENV_ALLOW_FULL_VALIDATE = "DET_ALLOW_FULL_VALIDATE"


def full_validate_allowed(*, env: Mapping[str, str] | None = None) -> bool:
    """True when DET_ALLOW_FULL_VALIDATE is set to a truthy value."""
    source = env if env is not None else os.environ
    raw = str(source.get(ENV_ALLOW_FULL_VALIDATE, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def assert_full_validate_allowed(
    *,
    confirm: bool,
    env: Mapping[str, str] | None = None,
) -> None:
    """Raise unless confirm_full_validate and DET_ALLOW_FULL_VALIDATE=1."""
    if not confirm:
        raise ValueError(
            "validate_limit=0 requires confirm_full_validate=true after running "
            "validate_sample and/or migrate_dry_run with validate_limit=50; "
            "get user confirmation first"
        )
    if not full_validate_allowed(env=env):
        raise ValueError(
            f"validate_limit=0 requires {ENV_ALLOW_FULL_VALIDATE}=1 in the agent "
            "environment (see .envrc.example); det check reports full_validate_gated "
            "when unset"
        )
