"""Pipeline SLO policy: YAML overlays → dbt seed rows.

Extract/load stay dumb (receipts only). Cadence hours live here so scaffold and
``det check`` share one helper. Not inferred from ``interval_*`` / ``dbt.silver.lookback``.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

Cadence = Literal["daily", "weekly", "hourly"]

# recency_hours / score_hours when omitted on slo: (and not overridden).
CADENCE_DEFAULTS: dict[str, tuple[int, int]] = {
    "daily": (26, 24),
    "weekly": (192, 168),
    "hourly": (2, 24),
}

SLO_SEED_RELPATH = Path("dbt") / "seeds" / "ops_slo_expected.csv"
SLO_SEED_COLUMNS = (
    "pipeline",
    "command",
    "cadence",
    "recency_hours",
    "score_hours",
    "max_error_rate",
    "p95_ms",
)
SLO_COMMANDS = ("extract", "load")


class SloOverlay(BaseModel):
    """Sparse threshold fields. Nested extract/load overlays merge onto parent ``slo:``."""

    model_config = ConfigDict(extra="forbid")

    cadence: Cadence | None = None
    recency_hours: int | None = None
    score_hours: int | None = None
    max_error_rate: float | None = None
    p95_ms: int | None = None

    @field_validator("recency_hours", "score_hours")
    @classmethod
    def _positive_hours(cls, v: int | None) -> int | None:
        if v is not None and v <= 0:
            raise ValueError("hours must be a positive integer")
        return v

    @field_validator("max_error_rate")
    @classmethod
    def _rate_bounds(cls, v: float | None) -> float | None:
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError("max_error_rate must be between 0 and 1 inclusive")
        return v

    @field_validator("p95_ms")
    @classmethod
    def _positive_p95(cls, v: int | None) -> int | None:
        if v is not None and v <= 0:
            raise ValueError("p95_ms must be a positive integer")
        return v


class SloConfig(SloOverlay):
    """Opt-in fleet SLO on a pipeline. ``extract: false`` / ``load: false`` skips a command."""

    extract: SloOverlay | Literal[False] | None = None
    load: SloOverlay | Literal[False] | None = None

    @field_validator("extract", "load", mode="before")
    @classmethod
    def _false_or_mapping(cls, v: Any) -> Any:
        if v is False:
            return False
        if v is True:
            raise ValueError("extract/load must be a mapping or false")
        return v

    @model_validator(mode="after")
    def _require_cadence_or_recency(self) -> Self:
        if self.cadence is None and self.recency_hours is None:
            raise ValueError("slo requires cadence or recency_hours")
        return self


@dataclass(frozen=True)
class SloExpectedRow:
    pipeline: str
    command: str
    cadence: str | None
    recency_hours: int
    score_hours: int
    max_error_rate: float | None
    p95_ms: int | None


def resolve_slo_hours(
    *,
    cadence: str | None,
    recency_hours: int | None,
    score_hours: int | None,
) -> tuple[int, int]:
    """Fill omitted hours from cadence defaults. Explicit hours win."""
    defaults = CADENCE_DEFAULTS.get(cadence) if cadence else None
    recency = recency_hours
    score = score_hours
    if recency is None:
        if defaults is None:
            raise ValueError("slo requires cadence or recency_hours")
        recency = defaults[0]
    if score is None:
        score = defaults[1] if defaults is not None else recency
    return recency, score


def _merge_command(base: SloConfig, overlay: SloOverlay | None) -> dict[str, Any]:
    """Overlay replaces only set fields; siblings stay on the parent ``slo:``."""
    cadence = base.cadence
    recency_hours = base.recency_hours
    score_hours = base.score_hours
    max_error_rate = base.max_error_rate
    p95_ms = base.p95_ms
    if overlay is not None:
        if overlay.cadence is not None:
            cadence = overlay.cadence
        if overlay.recency_hours is not None:
            recency_hours = overlay.recency_hours
        if overlay.score_hours is not None:
            score_hours = overlay.score_hours
        if overlay.max_error_rate is not None:
            max_error_rate = overlay.max_error_rate
        if overlay.p95_ms is not None:
            p95_ms = overlay.p95_ms
    return {
        "cadence": cadence,
        "recency_hours": recency_hours,
        "score_hours": score_hours,
        "max_error_rate": max_error_rate,
        "p95_ms": p95_ms,
    }


def flatten_slo_rows(pipeline: str, slo: SloConfig) -> list[SloExpectedRow]:
    """One seed row per command unless that command is ``false``."""
    rows: list[SloExpectedRow] = []
    for command in SLO_COMMANDS:
        overlay = getattr(slo, command)
        if overlay is False:
            continue
        merged = _merge_command(slo, overlay if isinstance(overlay, SloOverlay) else None)
        recency, score = resolve_slo_hours(
            cadence=merged["cadence"],
            recency_hours=merged["recency_hours"],
            score_hours=merged["score_hours"],
        )
        rows.append(
            SloExpectedRow(
                pipeline=pipeline,
                command=command,
                cadence=merged["cadence"],
                recency_hours=recency,
                score_hours=score,
                max_error_rate=merged["max_error_rate"],
                p95_ms=merged["p95_ms"],
            )
        )
    return rows


def _fmt_rate(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return f"{value:g}"


def render_slo_seed_csv(rows: Sequence[SloExpectedRow]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(SLO_SEED_COLUMNS), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "pipeline": row.pipeline,
                "command": row.command,
                "cadence": row.cadence or "",
                "recency_hours": row.recency_hours,
                "score_hours": row.score_hours,
                "max_error_rate": (
                    "" if row.max_error_rate is None else _fmt_rate(row.max_error_rate)
                ),
                "p95_ms": "" if row.p95_ms is None else row.p95_ms,
            }
        )
    return buf.getvalue()


def collect_slo_rows(project_root: Path) -> list[SloExpectedRow]:
    """Flatten ``slo:`` from every pipeline that loads. Invalid YAML is skipped."""
    from det.runtime.config import load_pipeline_config
    from det.runtime.pipelines import discover_pipeline_files

    rows: list[SloExpectedRow] = []
    for path in discover_pipeline_files(project_root):
        try:
            config = load_pipeline_config(path)
        except Exception:  # noqa: S112  # skip unloadable YAML when collecting SLOs
            continue
        if config.slo is None:
            continue
        rows.extend(flatten_slo_rows(config.name, config.slo))
    rows.sort(key=lambda r: (r.pipeline, r.command))
    return rows


def render_slo_seed_for_project(project_root: Path) -> str:
    return render_slo_seed_csv(collect_slo_rows(project_root))


def slo_seed_is_stale(project_root: Path) -> bool:
    expected = render_slo_seed_for_project(project_root)
    path = Path(project_root) / SLO_SEED_RELPATH
    expected_has_rows = len(expected.splitlines()) > 1
    if not path.is_file():
        return expected_has_rows
    on_disk = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    if not on_disk.endswith("\n") and on_disk:
        on_disk += "\n"
    return on_disk != expected
