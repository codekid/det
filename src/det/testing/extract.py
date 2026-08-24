"""Unit-tier helpers: call extract_to_raw / records_from_raw without a full run."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from det.runtime.meta import resolve_interval
from det.sources.base import Interval, SourcePlugin, SourceRow, merge_source_config


@dataclass
class ExtractFixture:
    """Result of :func:`extract_fixture`."""

    artifacts: list[dict[str, Any]]
    data_dir: Path
    raw_dir: Path
    interval: Interval
    config: dict[str, Any]
    _tmpdir: TemporaryDirectory[str] | None = None

    def cleanup(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None


def extract_fixture(
    source: SourcePlugin,
    *,
    rows: Sequence[Mapping[str, Any]] | None = None,
    config: Mapping[str, Any] | None = None,
    interval_start: str = "2026-08-06",
    interval_end: str | None = None,
    data_dir: Path | None = None,
) -> ExtractFixture:
    """
    Run ``source.extract_to_raw`` into a temp (or given) ``data/`` directory.

    When *rows* is set, merges ``fixture_records`` into the effective config
    (shallow; caller *config* wins on other keys).
    """
    overrides: dict[str, Any] = dict(config or {})
    if rows is not None:
        overrides.setdefault("fixture_records", [dict(r) for r in rows])
    effective = merge_source_config(source.defaults(), overrides)
    start_iso, end_iso = resolve_interval(interval_start, interval_end)
    interval = Interval(start=start_iso, end=end_iso)

    tmp: TemporaryDirectory[str] | None = None
    if data_dir is None:
        tmp = TemporaryDirectory(prefix="det-extract-")
        raw_dir = Path(tmp.name) / "raw_run"
        data_path = raw_dir / "data"
    else:
        data_path = Path(data_dir)
        raw_dir = data_path.parent
    data_path.mkdir(parents=True, exist_ok=True)

    artifacts = source.extract_to_raw(
        config=effective, interval=interval, data_dir=data_path
    )
    return ExtractFixture(
        artifacts=list(artifacts),
        data_dir=data_path,
        raw_dir=raw_dir,
        interval=interval,
        config=effective,
        _tmpdir=tmp,
    )


def records_from_fixture(
    source: SourcePlugin,
    *,
    artifacts: Sequence[Mapping[str, Any]] | None = None,
    raw_dir: Path | None = None,
    config: Mapping[str, Any] | None = None,
    fixture: ExtractFixture | None = None,
) -> list[SourceRow]:
    """Parse rows via ``records_from_raw`` using a prior :func:`extract_fixture`."""
    if fixture is not None:
        arts = fixture.artifacts
        root = fixture.raw_dir
        effective = merge_source_config(source.defaults(), fixture.config)
    else:
        if artifacts is None or raw_dir is None:
            raise ValueError("pass fixture= or both artifacts= and raw_dir=")
        arts = list(artifacts)
        root = Path(raw_dir)
        effective = merge_source_config(source.defaults(), dict(config or {}))
    manifest = {"artifacts": arts}
    return list(
        source.records_from_raw(config=effective, raw_dir=root, manifest=manifest)
    )


def iter_records_from_fixture(
    source: SourcePlugin,
    *,
    fixture: ExtractFixture,
) -> Iterator[SourceRow]:
    """Streaming variant of :func:`records_from_fixture`."""
    effective = merge_source_config(source.defaults(), fixture.config)
    yield from source.records_from_raw(
        config=effective,
        raw_dir=fixture.raw_dir,
        manifest={"artifacts": fixture.artifacts},
    )
