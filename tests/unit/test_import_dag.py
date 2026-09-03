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

# Paths relative to RUNTIME_ROOT (not basenames — nested dbt_runner.py must not match).
_FLATTEN_ALLOWLIST = frozenset({Path("dbt_runner.py")})

_UNRESOLVED_RELATIVE = "__unresolved_relative_import__"


def _iter_runtime_py() -> list[Path]:
    return sorted(p for p in RUNTIME_ROOT.rglob("*.py") if p.is_file())


def _package_for(path: Path) -> str:
    """Return ``__package__`` for a runtime module path."""
    rel = path.resolve().relative_to(RUNTIME_ROOT.resolve())
    if rel.name == "__init__.py":
        parts = rel.parent.parts
        return "det.runtime" if not parts else "det.runtime." + ".".join(parts)
    parent = rel.with_suffix("").parts[:-1]
    return "det.runtime" if not parent else "det.runtime." + ".".join(parent)


def _resolve_from_module(package: str, module: str | None, level: int) -> str | None:
    """Resolve a relative ImportFrom to an absolute module name (None if beyond top)."""
    if level <= 0:
        return module
    parts = package.split(".")
    if level > len(parts):
        return None
    base = ".".join(parts[: len(parts) - (level - 1)])
    if not base:
        return None
    return f"{base}.{module}" if module else base


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: list[str] = []
    package = _package_for(path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                resolved = _resolve_from_module(package, node.module, node.level)
                if resolved is None:
                    names.append(_UNRESOLVED_RELATIVE)
                    continue
                if node.module is None:
                    for alias in node.names:
                        if alias.name == "*":
                            names.append(resolved)
                        else:
                            names.append(f"{resolved}.{alias.name}")
                else:
                    names.append(resolved)
                    for alias in node.names:
                        if alias.name != "*":
                            names.append(f"{resolved}.{alias.name}")
                continue
            if node.module is None:
                continue
            names.append(node.module)
            for alias in node.names:
                names.append(f"{node.module}.{alias.name}")
    return names


def test_relative_import_resolution_helpers() -> None:
    assert _package_for(RUNTIME_ROOT / "dbt_runner.py") == "det.runtime"
    assert _package_for(RUNTIME_ROOT / "lease" / "store.py") == "det.runtime.lease"
    assert _package_for(RUNTIME_ROOT / "lease" / "__init__.py") == "det.runtime.lease"
    assert _resolve_from_module("det.runtime.lease", "mcp", 3) == "det.mcp"
    assert _resolve_from_module("det.runtime", "scaffold.dbt", 2) == "det.scaffold.dbt"
    assert _resolve_from_module("det.runtime", None, 2) == "det"
    assert _resolve_from_module("det.runtime", "mcp", 3) is None
    assert _resolve_from_module("det", "mcp", 2) is None


def test_runtime_import_dag_forbids_mcp_and_scaffold_dbt() -> None:
    violations: list[str] = []
    for path in _iter_runtime_py():
        rel = path.relative_to(RUNTIME_ROOT)
        for name in _imported_modules(path):
            if name == _UNRESOLVED_RELATIVE:
                violations.append(f"{rel}: unresolved relative import")
                continue
            for forbidden in _FORBIDDEN_PREFIXES:
                if name == forbidden or name.startswith(forbidden + "."):
                    violations.append(f"{rel}: imports {name}")
            if name == "expected_silver_sql" or name.endswith(".expected_silver_sql"):
                violations.append(f"{rel}: imports expected_silver_sql")
            if not (name == "det.scaffold" or name.startswith("det.scaffold.")):
                continue
            if name == "det.scaffold.flatten" or name.startswith("det.scaffold.flatten."):
                if rel not in _FLATTEN_ALLOWLIST:
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
    assert Path("dbt_runner.py") in _FLATTEN_ALLOWLIST
    assert Path("lease") / "dbt_runner.py" not in _FLATTEN_ALLOWLIST
