from __future__ import annotations

import pytest

from det.mcp.inspect._common import resolve_migrate_validate_limit
from det.runtime.full_validate import (
    ENV_ALLOW_FULL_VALIDATE,
    assert_full_validate_allowed,
    full_validate_allowed,
)


def test_resolve_migrate_validate_limit_zero_is_full():
    assert resolve_migrate_validate_limit(0) is None


def test_resolve_migrate_validate_limit_clamps_sample():
    assert resolve_migrate_validate_limit(50) == 50
    assert resolve_migrate_validate_limit(10) == 10


def test_resolve_migrate_validate_limit_rejects_out_of_range():
    with pytest.raises(ValueError, match="validate_limit must be"):
        resolve_migrate_validate_limit(51)


def test_full_validate_allowed_truthy_values():
    assert full_validate_allowed(env={ENV_ALLOW_FULL_VALIDATE: "1"})
    assert full_validate_allowed(env={ENV_ALLOW_FULL_VALIDATE: "true"})
    assert not full_validate_allowed(env={ENV_ALLOW_FULL_VALIDATE: ""})
    assert not full_validate_allowed(env={})


def test_assert_full_validate_allowed_requires_confirm():
    env = {ENV_ALLOW_FULL_VALIDATE: "1"}
    with pytest.raises(ValueError, match="confirm_full_validate"):
        assert_full_validate_allowed(confirm=False, env=env)


def test_assert_full_validate_allowed_requires_env():
    with pytest.raises(ValueError, match=ENV_ALLOW_FULL_VALIDATE):
        assert_full_validate_allowed(confirm=True, env={})


def test_assert_full_validate_allowed_ok():
    assert_full_validate_allowed(
        confirm=True,
        env={ENV_ALLOW_FULL_VALIDATE: "1"},
    )
