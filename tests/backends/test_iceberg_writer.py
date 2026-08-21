from __future__ import annotations

from pathlib import Path

import pytest

from det.ingestion.det_backend import DetBackend
from det.ingestion.iceberg_writer import (
    list_iceberg_extract_runs,
    load_iceberg_table,
    scan_iceberg_rows,
    write_iceberg_table,
)
from det.runtime.config import (
    DestinationConfig,
    IngestionConfig,
    MedallionConfig,
    PipelineConfig,
    SourceConfig,
)
from det.runtime.lake import open_lake

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")


def _json_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "event_id": {"type": ["integer", "null"]},
            "payload": {
                "type": "object",
                "properties": {"k": {"type": "string"}},
            },
        },
    }


def _config(tmp_path: Path) -> PipelineConfig:
    schema_rel = "event.schema.yaml"
    (tmp_path / schema_rel).write_text(
        "type: object\n"
        "properties:\n"
        "  event_id: {type: [integer, 'null']}\n"
        "  payload:\n"
        "    type: object\n"
        "    properties:\n"
        "      k: {type: string}\n",
        encoding="utf-8",
    )
    return PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path=schema_rel,
        ingestion=IngestionConfig(library="det"),
        destination=DestinationConfig(type="iceberg", path=str(tmp_path / "lake")),
        medallion=MedallionConfig(bronze_prefix="bronze", raw_prefix="raw"),
    )


def _meta(**overrides):
    row = {
        "__row_hash": "abc",
        "__filename": "x.csv",
        "__extract_run_datetime": "2026-08-06T15:04:05+00:00",
        "__bronze_loaded_at": "2026-08-06T15:04:06+00:00",
        "__interval_start_datetime": "2026-08-06T00:00:00+00:00",
        "__interval_end_datetime": "2026-08-07T00:00:00+00:00",
        "__data_interval_date": "2026-08-06",
        **overrides,
    }
    return row


def _records(**overrides):
    return [{**_meta(), "event_id": 1, "payload": {"k": "v"}, **overrides}]


def test_det_backend_writes_iceberg_and_keeps_sibling(tmp_path: Path):
    dest = DestinationConfig(type="iceberg", path=str(tmp_path / "lake"))
    config = _config(tmp_path)
    backend = DetBackend()
    written = backend.write(
        _records(),
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "unused_hive",
        destination=dest,
    )
    assert "storm_events_v1" in str(written)
    assert not (tmp_path / "unused_hive").exists()

    backend.write(
        _records(
            event_id=2,
            __row_hash="sib",
            __extract_run_datetime="2026-08-06T16:00:00+00:00",
        ),
        config=config,
        project_root=tmp_path,
        partition_dir=tmp_path / "unused_hive2",
        destination=dest,
    )
    ice = load_iceberg_table(
        lake=open_lake(str(tmp_path / "lake"), tmp_path),
        namespace="bronze_noaa",
        table="storm_events_v1",
        table_location=written,
    )
    assert ice is not None
    rows = scan_iceberg_rows(ice, limit=10)
    hashes = {r["__row_hash"] for r in rows}
    assert hashes == {"abc", "sib"}
    by_hash = {r["__row_hash"]: r for r in rows}
    assert by_hash["abc"]["event_id"] == 1
    assert by_hash["abc"]["payload"] == '{"k": "v"}'
    assert (tmp_path / "lake" / "bronze" / "noaa" / "storm_events_v1" / "metadata").is_dir()
    assert not any(p.name == "data.jsonl" for p in written.rglob("*"))


def test_iceberg_replaces_same_extract_run(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "noaa" / "storm_events_v1"
    schema = _json_schema()
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=schema,
    )
    write_iceberg_table(
        _records(event_id=99, __row_hash="zzz"),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=schema,
    )
    write_iceberg_table(
        _records(
            event_id=2,
            __row_hash="sib",
            __extract_run_datetime="2026-08-06T16:00:00+00:00",
        ),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=schema,
    )
    ice = load_iceberg_table(
        lake=lake, namespace="bronze_noaa", table="storm_events_v1", table_location=loc
    )
    rows = scan_iceberg_rows(ice, limit=10)
    by_hash = {r["__row_hash"]: r for r in rows}
    assert set(by_hash) == {"zzz", "sib"}
    assert by_hash["zzz"]["event_id"] == 99
    runs = list_iceberg_extract_runs(ice)
    assert len(runs) == 2


def test_iceberg_refuses_incompatible_type(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "noaa" / "storm_events_v1"
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
    )
    with pytest.raises(ValueError, match="has type INTEGER, expected STRING"):
        write_iceberg_table(
            [{**_meta(), "event_id": "x"}],
            lake=lake,
            table_location=loc,
            namespace="bronze_noaa",
            table="storm_events_v1",
            json_schema={
                "type": "object",
                "properties": {"event_id": {"type": "string"}},
            },
        )


def test_iceberg_alter_adds_missing_column(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "noaa" / "storm_events_v1"
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema={
            "type": "object",
            "properties": {"event_id": {"type": "integer"}},
        },
    )
    write_iceberg_table(
        [{**_meta(), "event_id": 2, "state": "TX", "__row_hash": "two",
          "__extract_run_datetime": "2026-08-06T16:00:00+00:00"}],
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema={
            "type": "object",
            "properties": {
                "event_id": {"type": "integer"},
                "state": {"type": ["string", "null"]},
            },
        },
    )
    ice = load_iceberg_table(
        lake=lake, namespace="bronze_noaa", table="storm_events_v1", table_location=loc
    )
    names = {f.name for f in ice.schema().fields}
    assert "state" in names
    rows = scan_iceberg_rows(ice, limit=10)
    states = {r.get("state") for r in rows}
    assert states == {None, "TX"}


def test_version_hint_is_duckdb_stem_not_file_uri(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "noaa" / "storm_events_v1"
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
    )
    hint_path = (
        tmp_path / "lake" / "bronze" / "noaa" / "storm_events_v1" / "metadata" / "version-hint.text"
    )
    hint = hint_path.read_text(encoding="utf-8").strip()
    assert "://" not in hint
    assert not hint.endswith(".metadata.json")
    ice = load_iceberg_table(
        lake=lake, namespace="bronze_noaa", table="storm_events_v1", table_location=loc
    )
    assert ice is not None
    assert scan_iceberg_rows(ice, limit=1)

    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
    except Exception as exc:  # pragma: no cover - optional extension
        pytest.skip(f"duckdb iceberg extension unavailable: {exc}")
    path = str((tmp_path / "lake" / "bronze" / "noaa" / "storm_events_v1").resolve())
    n = con.execute(f"SELECT count(*) FROM iceberg_scan('{path}')").fetchone()[0]
    assert n >= 1


def test_iceberg_partition_extract_run_is_single_identity(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "noaa" / "storm_events_v1"
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=_json_schema(),
        partition="extract_run",
    )
    ice = load_iceberg_table(
        lake=lake, namespace="bronze_noaa", table="storm_events_v1", table_location=loc
    )
    assert ice is not None
    fields = list(ice.spec().fields)
    assert len(fields) == 1
    src = ice.schema().find_field(fields[0].source_id)
    assert src.name == "__extract_run_datetime"
    assert str(fields[0].transform) == "identity"


def test_iceberg_partition_none_is_unpartitioned(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "example_api" / "events_v1"
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_example_api",
        table="events_v1",
        json_schema=_json_schema(),
        partition="none",
    )
    ice = load_iceberg_table(
        lake=lake,
        namespace="bronze_example_api",
        table="events_v1",
        table_location=loc,
    )
    assert ice is not None
    assert list(ice.spec().fields) == []


def test_iceberg_replace_works_when_unpartitioned(tmp_path: Path):
    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "example_api" / "events_v1"
    schema = _json_schema()
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_example_api",
        table="events_v1",
        json_schema=schema,
        partition="none",
    )
    write_iceberg_table(
        _records(event_id=99, __row_hash="zzz"),
        lake=lake,
        table_location=loc,
        namespace="bronze_example_api",
        table="events_v1",
        json_schema=schema,
        partition="none",
    )
    ice = load_iceberg_table(
        lake=lake,
        namespace="bronze_example_api",
        table="events_v1",
        table_location=loc,
    )
    rows = scan_iceberg_rows(ice, limit=10)
    assert {r["__row_hash"]: r["event_id"] for r in rows} == {"zzz": 99}


def test_iceberg_keeps_live_spec_when_yaml_mismatches(tmp_path: Path):
    from structlog.testing import capture_logs

    lake = open_lake(str(tmp_path / "lake"), tmp_path)
    loc = lake / "bronze" / "noaa" / "storm_events_v1"
    schema = _json_schema()
    write_iceberg_table(
        _records(),
        lake=lake,
        table_location=loc,
        namespace="bronze_noaa",
        table="storm_events_v1",
        json_schema=schema,
        partition="extract_run",
    )
    with capture_logs() as logs:
        write_iceberg_table(
            _records(
                event_id=2,
                __row_hash="sib",
                __extract_run_datetime="2026-08-06T16:00:00+00:00",
            ),
            lake=lake,
            table_location=loc,
            namespace="bronze_noaa",
            table="storm_events_v1",
            json_schema=schema,
            partition="none",
        )
    ice = load_iceberg_table(
        lake=lake, namespace="bronze_noaa", table="storm_events_v1", table_location=loc
    )
    assert len(list(ice.spec().fields)) == 1
    warnings = [e for e in logs if e.get("log_level") == "warning"]
    assert any(
        "partition YAML does not match" in str(e.get("event", "")) for e in warnings
    )