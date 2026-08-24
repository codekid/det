"""Secret lookup helpers for tests (no process-env mutation)."""

from __future__ import annotations

from collections.abc import Mapping

from det.runtime.settings import SecretLookup


def secrets_map(values: Mapping[str, str | None]) -> SecretLookup:
    """
    Build a ``DetSettings``-compatible secret lookup from a fixed map.

    Names not present in *values* resolve to ``None`` (unset). Does not read
    or write ``os.environ``.
    """
    frozen = dict(values)

    def lookup(name: str) -> str | None:
        return frozen.get(name)

    return lookup
