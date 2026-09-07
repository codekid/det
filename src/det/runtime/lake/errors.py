"""Lake object-store CAS errors."""

from __future__ import annotations


class ObjectVersionConflict(Exception):
    """Conditional put/delete failed: object version no longer matches."""


class ObjectCasUnsupported(RuntimeError):
    """Object store cannot apply strong preconditions (fail closed for leases)."""
