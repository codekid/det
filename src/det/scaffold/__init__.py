from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["ScaffoldResult", "scaffold_dbt"]

if TYPE_CHECKING:
    from det.scaffold.dbt import ScaffoldResult, scaffold_dbt


def __getattr__(name: str) -> Any:
    if name in __all__:
        from det.scaffold.dbt import ScaffoldResult, scaffold_dbt

        return {"ScaffoldResult": ScaffoldResult, "scaffold_dbt": scaffold_dbt}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
