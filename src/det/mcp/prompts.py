"""MCP prompts that load Cursor skill markdown (skills stay the source of truth)."""

from __future__ import annotations

from pathlib import Path

import yaml

from det.mcp.context import project_root

# prompt name → .cursor/skills/<dir>/SKILL.md
SKILL_PROMPTS: dict[str, str] = {
    "det_ops": "det-ops",
    "det_new_source": "det-new-source",
    "det_migrate": "det-migrate",
    "det_dbt": "det-dbt",
    "det_airflow": "det-airflow",
}


def _skills_candidates(*, root: Path | None = None) -> list[Path]:
    """Prefer DET_PROJECT_ROOT, then this checkout (src/det/mcp → repo)."""
    seen: list[Path] = []
    for base in (
        root,
        project_root(),
        Path(__file__).resolve().parents[3],
    ):
        if base is None:
            continue
        resolved = Path(base).resolve()
        if resolved not in seen:
            seen.append(resolved)
    return seen


def skill_markdown_path(skill_dir: str, *, root: Path | None = None) -> Path:
    for base in _skills_candidates(root=root):
        path = base / ".cursor" / "skills" / skill_dir / "SKILL.md"
        if path.is_file():
            return path
    raise FileNotFoundError(
        f"skill not found: .cursor/skills/{skill_dir}/SKILL.md "
        f"(set DET_PROJECT_ROOT to the DET checkout)"
    )


def load_skill_markdown(skill_dir: str, *, root: Path | None = None) -> str:
    return skill_markdown_path(skill_dir, root=root).read_text(encoding="utf-8")


def skill_description(skill_dir: str, *, root: Path | None = None) -> str:
    """YAML frontmatter ``description`` from SKILL.md (folded to one paragraph)."""
    text = load_skill_markdown(skill_dir, root=root)
    if not text.startswith("---"):
        return ""
    rest = text[3:]
    end = rest.find("\n---")
    if end < 0:
        return ""
    meta = yaml.safe_load(rest[:end]) or {}
    raw = meta.get("description") or ""
    if isinstance(raw, str):
        return " ".join(raw.split())
    return str(raw)


def register_skill_prompts(mcp: object) -> None:
    """Register one FastMCP prompt per Cursor skill."""
    prompt = getattr(mcp, "prompt")
    for prompt_name, skill_dir in SKILL_PROMPTS.items():
        description = skill_description(skill_dir)
        _bind_prompt(prompt, prompt_name, skill_dir, description)


def _bind_prompt(
    prompt_decorator: object,
    prompt_name: str,
    skill_dir: str,
    description: str,
) -> None:
    def skill_prompt() -> str:
        return load_skill_markdown(skill_dir)

    skill_prompt.__name__ = prompt_name
    skill_prompt.__doc__ = description
    prompt_decorator(name=prompt_name, description=description)(skill_prompt)
