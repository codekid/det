"""Import-DAG: det.runtime (landing) must not depend on MCP or scaffold codegen.

Allowed: schema_shapes; temporary dbt_runner → scaffold.flatten (adapter code
still under runtime/ — do not expand this exception).
"""

from __future__ import annotations

import ast
from pathlib import Path

RUNTIME_ROOT = Path(__file__).resolve().parents[2] / "src" / "det" / "runtime"

_FORBIDDEN_PREFIXES = (
    "det.mcp",
    "det.scaffold.dbt",
    "det.scaffold.dbt_sql",
    "det.scaffold.dbt_yaml",
)

_FLATTEN_ALLOWLIST = frozenset({"dbt_runner.py"})


def _iter_runtime_py() -> list[Path]:
    return sorted(p for p in RUNTIME_ROOT.rglob("*.py") if p.is_file())


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            if node.level:
                continue  # relative within runtime — ignore
            names.append(node.module)
            for alias in node.names:
                names.append(f"{node.module}.{alias.name}")
    return names


def test_runtime_import_dag_forbids_mcp_and_scaffold_dbt() -> None:
    violations: list[str] = []
    for path in _iter_runtime_py():
        rel = path.relative_to(RUNTIME_ROOT)
        for name in _imported_modules(path):
            for forbidden in _FORBIDDEN_PREFIXES:
                if name == forbidden or name.startswith(forbidden + "."):
                    violations.append(f"{rel}: imports {name}")
            if name == "expected_silver_sql" or name.endswith(".expected_silver_sql"):
                violations.append(f"{rel}: imports expected_silver_sql")
            if not (name == "det.scaffold" or name.startswith("det.scaffold.")):
                continue
            if name == "det.scaffold.flatten" or name.startswith("det.scaffold.flatten."):
                if path.name not in _FLATTEN_ALLOWLIST:
                    violations.append(f"{rel}: imports {name}")
                continue
            violations.append(f"{rel}: imports {name}")
    assert not violations, "runtime import-DAG violations:\n" + "\n".join(violations)


def test_dbt_runner_may_import_scaffold_flatten_only() -> None:
    path = RUNTIME_ROOT / "dbt_runner.py"
    assert path.is_file()
    names = _imported_modules(path)
    scaffold_hits = [n for n in names if n.startswith("det.scaffold")]
    assert scaffold_hits, "dbt_runner should import scaffold.flatten"
    for n in scaffold_hits:
        assert n == "det.scaffold.flatten" or n.startswith("det.scaffold.flatten."), n
