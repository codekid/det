"""DET CLI package. Entry point: ``det.cli:app``."""

from __future__ import annotations

# Register Typer commands (side-effect imports).
from det.cli import approvals as approvals  # noqa: F401
from det.cli import dbt_cmd as dbt_cmd  # noqa: F401
from det.cli import extract_load as extract_load  # noqa: F401
from det.cli import inspect_cmd as inspect_cmd  # noqa: F401
from det.cli import migrate_prune as migrate_prune  # noqa: F401
from det.cli import ops as ops  # noqa: F401
from det.cli import scaffold as scaffold  # noqa: F401
from det.cli import silver_catchup_cmd as silver_catchup_cmd  # noqa: F401
from det.cli.app import app
from det.cli.ops import lock_release
from det.cli.render_runs import _print_run_list

__all__ = ["app", "lock_release", "_print_run_list"]


if __name__ == "__main__":
    app()
