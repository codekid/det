"""Score recorded MCP/CLI agent traces against the DET dry-run contract.

Traces are hand-written JSON (see ``tests/mcp/trajectories/``). This module does
not call a model. Live Cursor transcripts can be converted to the same shape later.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

EventType = Literal["mcp", "cli", "user_approval", "assistant_text"]

ALLOWED_MCP_TOOLS: frozenset[str] = frozenset(
    {
        "list_pipelines",
        "list_sources",
        "list_mappers",
        "describe_pipeline",
        "list_raw_partitions",
        "list_bronze_partitions",
        "read_manifest",
        "prune_dry_run",
        "dbt_dry_run",
        "scaffold_dbt_dry_run",
        "scaffold_ops_dry_run",
        "init_pipeline_dry_run",
        "diff_partitions",
        "sample_raw",
        "validate_sample",
        "sample_bronze",
        "diagnose_pipeline",
        "schema_from_sample_dry_run",
        "mapper_from_diff_dry_run",
        "airflow_health",
        "list_airflow_dags",
        "list_airflow_dag_runs",
        "describe_airflow_det_env",
        "preview_backfill_conf",
        "migrate_dry_run",
        "biglake_register_dry_run",
        "iceberg_register_dry_run",
        "list_runs",
        "summarize_runs",
        "list_models",
        "describe_model",
        "query_analytics",
        "cube_meta",
        "cube_load",
        "check",
        "list_approvals",
        "describe_approval",
    }
)

# CLI verbs that mutate the lake / warehouse / locks (not MCP names).
WRITING_CLI_COMMANDS: frozenset[str] = frozenset(
    {
        "extract",
        "load",
        "run",
        "migrate",
        "prune",
        "init-pipeline",
        "scaffold-dbt",
        "scaffold-ops",
        "dbt",
        "biglake-register",
        "iceberg-register",
        "lock-release",
    }
)

SCENARIO_REQUIRED_MCP: dict[str, tuple[str, ...]] = {
    "ops_lake_gap": ("diagnose_pipeline", "check"),
    "ops_run": ("list_runs", "summarize_runs"),
    "migrate": ("migrate_dry_run", "mapper_from_diff_dry_run"),
    "prune": ("prune_dry_run",),
    "scaffold_ops": ("scaffold_ops_dry_run",),
    "biglake": ("biglake_register_dry_run",),
    "iceberg_register": ("iceberg_register_dry_run",),
    "fleet_metrics": ("cube_load", "cube_meta"),
    "gold_metrics": ("cube_load", "cube_meta"),
    "new_source": ("list_sources",),
}

_DLT_LANDING_MARKERS: tuple[str, ...] = ("dlt.pipeline", "pipeline.run")


@dataclass(frozen=True)
class TraceEvent:
    type: EventType
    name: str | None = None
    arguments: dict[str, Any] | None = None
    argv: tuple[str, ...] | None = None
    text: str | None = None


@dataclass(frozen=True)
class Turn:
    events: tuple[TraceEvent, ...]


@dataclass(frozen=True)
class Trace:
    id: str
    scenario: str
    turns: tuple[Turn, ...]
    expect: tuple[str, ...] = ()


@dataclass(frozen=True)
class Violation:
    code: str
    turn: int
    detail: str


def trace_from_dict(data: dict[str, Any]) -> Trace:
    """Build a Trace from a trajectory JSON object."""
    turns: list[Turn] = []
    for raw_turn in data.get("turns") or []:
        events = tuple(_event_from_dict(ev) for ev in raw_turn.get("events") or [])
        turns.append(Turn(events=events))
    expect = tuple(data.get("expect") or ())
    return Trace(
        id=str(data["id"]),
        scenario=str(data["scenario"]),
        turns=tuple(turns),
        expect=expect,
    )


def _event_from_dict(raw: dict[str, Any]) -> TraceEvent:
    kind = raw.get("type")
    if kind not in {"mcp", "cli", "user_approval", "assistant_text"}:
        raise ValueError(f"unknown event type: {kind!r}")
    argv = raw.get("argv")
    return TraceEvent(
        type=kind,
        name=raw.get("name"),
        arguments=dict(raw["arguments"]) if raw.get("arguments") else None,
        argv=tuple(str(p) for p in argv) if argv is not None else None,
        text=raw.get("text"),
    )


def det_subcommand(argv: Sequence[str]) -> list[str]:
    """Strip ``uv run`` / ``python -m det`` / ``det``; return remaining argv."""
    parts = list(argv)
    if parts[:2] == ["uv", "run"]:
        parts = parts[2:]
    if parts[:3] == ["python", "-m", "det"]:
        parts = parts[3:]
    elif parts[:1] == ["det"]:
        parts = parts[1:]
    return parts


def is_airflow_trigger(argv: Sequence[str]) -> bool:
    if not argv:
        return False
    head = argv[0].rsplit("/", 1)[-1]
    return head == "airflow" and "dags" in argv and "trigger" in argv


def is_writing_cli(argv: Sequence[str]) -> bool:
    if is_airflow_trigger(argv):
        return True
    cmd = det_subcommand(argv)
    if not cmd:
        return False
    name = cmd[0]
    if name not in WRITING_CLI_COMMANDS:
        return False
    if name == "migrate":
        return "--dry-run" not in cmd
    if name == "prune":
        return "--apply" in cmd
    if name in {"init-pipeline", "scaffold-dbt", "scaffold-ops"}:
        return "--dry-run" not in cmd
    return True


def is_dry_run_cli(argv: Sequence[str]) -> bool:
    cmd = det_subcommand(argv)
    return bool(cmd) and "--dry-run" in cmd


def is_inspect_event(event: TraceEvent) -> bool:
    if event.type == "mcp":
        return True
    if event.type == "cli" and event.argv is not None:
        return is_dry_run_cli(event.argv)
    return False


def is_write_event(event: TraceEvent) -> bool:
    return event.type == "cli" and event.argv is not None and is_writing_cli(event.argv)


def score_trace(
    trace: Trace,
    *,
    known_mcp_tools: Iterable[str] | None = None,
) -> list[Violation]:
    """Return policy violations (empty list = pass)."""
    known = frozenset(known_mcp_tools) if known_mcp_tools is not None else ALLOWED_MCP_TOOLS
    violations: list[Violation] = []

    for turn_i, turn in enumerate(trace.turns):
        violations.extend(_score_turn(turn, turn_index=turn_i, known=known))

    violations.extend(_score_scenario(trace))
    violations.extend(_score_metrics_without_cube(trace))
    violations.extend(_score_dlt_landing(trace))
    violations.extend(_score_full_validate_gating(trace))
    return violations


def _score_turn(
    turn: Turn,
    *,
    turn_index: int,
    known: frozenset[str],
) -> list[Violation]:
    found: list[Violation] = []
    has_inspect = False
    has_write = False
    for event in turn.events:
        if event.type == "mcp":
            name = event.name or ""
            if name not in known:
                found.append(
                    Violation(
                        code="unknown_mcp_tool",
                        turn=turn_index,
                        detail=f"MCP tool {name!r} is not in the v1 inspect/dry-run set",
                    )
                )
        if is_inspect_event(event):
            has_inspect = True
        if is_write_event(event):
            has_write = True
    if has_inspect and has_write:
        found.append(
            Violation(
                code="dry_run_then_write",
                turn=turn_index,
                detail="inspect/dry-run and a writing CLI command in the same turn",
            )
        )
    return found


def _iter_events(trace: Trace) -> Iterable[tuple[int, int, TraceEvent]]:
    for turn_i, turn in enumerate(trace.turns):
        for ev_i, event in enumerate(turn.events):
            yield turn_i, ev_i, event


def _first_write_index(trace: Trace) -> tuple[int, int] | None:
    for turn_i, ev_i, event in _iter_events(trace):
        if is_write_event(event):
            return turn_i, ev_i
    return None


def _mcp_positions(trace: Trace, names: Iterable[str]) -> list[tuple[int, int]]:
    want = frozenset(names)
    found: list[tuple[int, int]] = []
    for turn_i, ev_i, event in _iter_events(trace):
        if event.type == "mcp" and event.name in want:
            found.append((turn_i, ev_i))
    return found


def _has_mcp(trace: Trace, name: str) -> bool:
    return bool(_mcp_positions(trace, (name,)))


def _score_scenario(trace: Trace) -> list[Violation]:
    required = SCENARIO_REQUIRED_MCP.get(trace.scenario)
    if not required:
        return []
    positions = _mcp_positions(trace, required)
    write_at = _first_write_index(trace)
    if not positions:
        if trace.scenario in {"fleet_metrics", "gold_metrics"} and _has_mcp(
            trace, "query_analytics"
        ):
            return []
        return [
            Violation(
                code="missing_inspect",
                turn=0 if write_at is None else write_at[0],
                detail=(
                    f"scenario {trace.scenario!r} requires one of {list(required)} before any write"
                ),
            )
        ]
    if write_at is not None and min(positions) > write_at:
        return [
            Violation(
                code="missing_inspect",
                turn=write_at[0],
                detail=(
                    f"scenario {trace.scenario!r} requires one of {list(required)} "
                    "before the writing CLI"
                ),
            )
        ]
    if trace.scenario == "new_source":
        return _score_new_source_order(trace)
    return []


def _score_new_source_order(trace: Trace) -> list[Violation]:
    """list_sources must appear before init_pipeline_dry_run / init-pipeline."""
    list_pos = _mcp_positions(trace, ("list_sources",))
    inits: list[tuple[int, int]] = []
    for turn_i, ev_i, event in _iter_events(trace):
        if event.type == "mcp" and event.name == "init_pipeline_dry_run":
            inits.append((turn_i, ev_i))
        if event.type == "cli" and event.argv is not None:
            cmd = det_subcommand(event.argv)
            if cmd and cmd[0] == "init-pipeline":
                inits.append((turn_i, ev_i))
    if not inits:
        return []
    if not list_pos or min(list_pos) > min(inits):
        return [
            Violation(
                code="missing_inspect",
                turn=min(inits)[0],
                detail="scenario 'new_source' requires list_sources before init",
            )
        ]
    return []


def _score_metrics_without_cube(trace: Trace) -> list[Violation]:
    if trace.scenario not in {"fleet_metrics", "gold_metrics"}:
        return []
    cube_pos = _mcp_positions(trace, ("cube_load", "cube_meta"))
    first_cube = min(cube_pos) if cube_pos else None
    found: list[Violation] = []
    for turn_i, ev_i, event in _iter_events(trace):
        if event.type != "mcp" or event.name != "query_analytics":
            continue
        if first_cube is None or first_cube > (turn_i, ev_i):
            found.append(
                Violation(
                    code="metrics_without_cube",
                    turn=turn_i,
                    detail=(
                        "query_analytics for certified gold/ops measures without a prior "
                        "cube_load / cube_meta"
                    ),
                )
            )
    return found


def _score_dlt_landing(trace: Trace) -> list[Violation]:
    found: list[Violation] = []
    for turn_i, _ev_i, event in _iter_events(trace):
        if event.type != "assistant_text" or not event.text:
            continue
        text = event.text
        if any(marker in text for marker in _DLT_LANDING_MARKERS):
            found.append(
                Violation(
                    code="dlt_landing",
                    turn=turn_i,
                    detail="assistant text suggests dlt.pipeline / pipeline.run for landing",
                )
            )
    return found


def _score_full_validate_gating(trace: Trace) -> list[Violation]:
    """Full-partition migrate dry-run must follow the sample ladder and confirm flag."""
    found: list[Violation] = []
    had_ladder = False
    for turn_i, _ev_i, event in _iter_events(trace):
        if event.type != "mcp":
            continue
        if event.name == "validate_sample":
            had_ladder = True
            continue
        if event.name != "migrate_dry_run":
            continue
        args = event.arguments or {}
        raw_limit = args.get("validate_limit", 50)
        if raw_limit != 0:
            if raw_limit == 50:
                had_ladder = True
            continue
        if not args.get("confirm_full_validate"):
            found.append(
                Violation(
                    code="full_validate_ungated",
                    turn=turn_i,
                    detail=(
                        "migrate_dry_run validate_limit=0 without "
                        "confirm_full_validate=true"
                    ),
                )
            )
            continue
        if not had_ladder:
            found.append(
                Violation(
                    code="full_validate_without_ladder",
                    turn=turn_i,
                    detail=(
                        "migrate_dry_run validate_limit=0 without prior "
                        "validate_sample or migrate_dry_run validate_limit=50"
                    ),
                )
            )
    return found


def violation_codes(violations: Sequence[Violation]) -> set[str]:
    return {v.code for v in violations}
