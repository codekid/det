from __future__ import annotations

import json
from pathlib import Path

import pytest

from det.mcp.policy import (
    ALLOWED_MCP_TOOLS,
    WRITING_CLI_COMMANDS,
    is_dry_run_cli,
    is_writing_cli,
    score_trace,
    trace_from_dict,
    violation_codes,
)
from det.mcp.server import create_server

TRAJ_DIR = Path(__file__).resolve().parent / "trajectories"
FIXTURES = sorted(TRAJ_DIR.glob("*.json"))


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_trajectory_fixture(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["id"] == path.stem
    trace = trace_from_dict(data)
    assert violation_codes(score_trace(trace)) == set(trace.expect)


def test_trajectory_fixtures_cover_plan_ids():
    names = {p.stem for p in FIXTURES}
    for expected in (
        "ops_gap_diagnose_stop",
        "ops_gap_no_diagnose",
        "ops_gap_chain_extract",
        "ops_gap_extract_after_yes",
        "prune_dry_run_stop",
        "prune_chain_apply",
        "migrate_dry_run_stop",
        "migrate_chain_write",
        "scaffold_ops_dry_run_stop",
        "scaffold_ops_chain_write",
        "fleet_cube_load",
        "fleet_sql_without_cube",
        "gold_cube_load",
        "gold_invented_sql",
        "new_source_list_then_init_dry_run",
        "new_source_init_without_list",
        "dlt_pipeline_text",
    ):
        assert expected in names


def test_writing_argv_classifier():
    assert is_writing_cli(["det", "extract", "-p", "noaa.storm_events"])
    assert is_writing_cli(["uv", "run", "det", "load", "-p", "noaa.storm_events"])
    assert is_writing_cli(["det", "run", "-p", "noaa.storm_events", "-s", "2026-08-06"])
    assert is_writing_cli(["det", "migrate", "-p", "x", "--mapper", "identity"])
    assert not is_writing_cli(["det", "migrate", "-p", "x", "--dry-run"])
    assert is_writing_cli(["det", "prune", "-p", "x", "--apply"])
    assert not is_writing_cli(["det", "prune", "-p", "x", "--dry-run"])
    assert is_writing_cli(["det", "init-pipeline", "--name", "a.b", "--source-type", "a.b"])
    assert not is_writing_cli(
        ["det", "init-pipeline", "--name", "a.b", "--source-type", "a.b", "--dry-run"]
    )
    assert is_writing_cli(["det", "scaffold-ops"])
    assert not is_writing_cli(["det", "scaffold-ops", "--dry-run"])
    assert is_writing_cli(["det", "dbt"])
    assert is_writing_cli(["det", "lock-release", "-p", "x", "-s", "2026-08-06", "--force"])
    assert is_writing_cli(["airflow", "dags", "trigger", "det_extract"])
    assert is_dry_run_cli(["det", "migrate", "--dry-run", "-p", "x"])
    assert not is_writing_cli(["det", "check"])
    assert not is_writing_cli(["det", "list-sources"])


def test_unknown_mcp_tool_is_a_violation():
    trace = trace_from_dict(
        {
            "id": "unknown_tool",
            "scenario": "ops_lake_gap",
            "expect": ["unknown_mcp_tool", "missing_inspect"],
            "turns": [{"events": [{"type": "mcp", "name": "extract", "arguments": {}}]}],
        }
    )
    assert "unknown_mcp_tool" in violation_codes(score_trace(trace))


def test_mcp_server_tools_are_inspect_only():
    server = create_server()
    names = frozenset(server._tool_manager._tools)
    assert names == ALLOWED_MCP_TOOLS
    overlap = names & WRITING_CLI_COMMANDS
    assert not overlap, f"MCP tools collide with writing CLI verbs: {sorted(overlap)}"
