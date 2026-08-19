from __future__ import annotations

from pathlib import Path

from det.mcp.prompts import SKILL_PROMPTS, load_skill_markdown, skill_description
from det.mcp.server import create_server

REPO = Path(__file__).resolve().parents[2]


def test_prompts_registered_with_skill_descriptions():
    server = create_server()
    registered = server._prompt_manager._prompts
    for prompt_name, skill_dir in SKILL_PROMPTS.items():
        assert prompt_name in registered
        prompt = registered[prompt_name]
        assert prompt.description.strip() == skill_description(skill_dir).strip()


def test_prompt_bodies_match_skill_files():
    server = create_server()
    registered = server._prompt_manager._prompts
    for prompt_name, skill_dir in SKILL_PROMPTS.items():
        on_disk = (REPO / ".cursor" / "skills" / skill_dir / "SKILL.md").read_text(
            encoding="utf-8"
        )
        loaded = load_skill_markdown(skill_dir, root=REPO)
        assert loaded == on_disk
        rendered = registered[prompt_name].fn()
        assert rendered == on_disk
