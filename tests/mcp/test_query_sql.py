from __future__ import annotations

from pathlib import Path

import duckdb

from det.mcp.query_sql import query_analytics


def _analytics_db(tmp_path: Path) -> Path:
    path = tmp_path / "data" / "analytics.duckdb"
    path.parent.mkdir(parents=True)
    con = duckdb.connect(str(path))
    con.execute("create schema gold")
    con.execute("create schema silver_noaa")
    con.execute(
        "create table gold.gold_yearly_damage as "
        "select '2026' as event_year, 'TX' as state, 10.0 as total_property_damage"
    )
    con.execute("create table silver_noaa.storm as select 1 as event_id")
    con.close()
    return path


def _ops_db(tmp_path: Path) -> Path:
    path = tmp_path / "data" / "det_ops.duckdb"
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("create schema ops")
    con.execute(
        "create table ops.det__ops_run_daily as select date '2026-08-06' as attempt_date, "
        "'noaa.storm_events' as pipeline, 'extract' as command, 2 as attempts"
    )
    con.close()
    return path


def test_query_analytics_gold_ok(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_ANALYTICS_DUCKDB", raising=False)
    _analytics_db(tmp_path)
    out = query_analytics(
        "select event_year, state from gold.gold_yearly_damage",
        warehouse="analytics",
        limit=5,
        root=tmp_path,
    )
    assert out["ok"] is True
    assert out["rows"][0]["data"]["state"] == "TX"
    assert "hunter" not in out["connection"]


def test_query_analytics_rejects_insert_and_cross_schema(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_ANALYTICS_DUCKDB", raising=False)
    _analytics_db(tmp_path)
    inserted = query_analytics(
        "insert into gold.gold_yearly_damage values ('x','y',1)",
        warehouse="analytics",
        root=tmp_path,
    )
    assert inserted["ok"] is False
    assert inserted["error"] == "sql_rejected"
    cross = query_analytics(
        "select * from ops.det__ops_run_daily",
        warehouse="analytics",
        root=tmp_path,
    )
    assert cross["ok"] is False
    assert "ops" in cross["detail"]


def test_query_ops_allowlist(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_OPS_DUCKDB", raising=False)
    _ops_db(tmp_path)
    ok = query_analytics(
        "select pipeline from ops.det__ops_run_daily",
        warehouse="ops",
        root=tmp_path,
    )
    assert ok["ok"] is True
    gold_on_ops = query_analytics(
        "select * from gold.gold_yearly_damage",
        warehouse="ops",
        root=tmp_path,
    )
    assert gold_on_ops["ok"] is False
    assert gold_on_ops["error"] == "sql_rejected"


def test_query_analytics_caps_limit(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("DET_ANALYTICS_DUCKDB", raising=False)
    path = tmp_path / "data" / "analytics.duckdb"
    path.parent.mkdir(parents=True)
    con = duckdb.connect(str(path))
    con.execute("create schema gold")
    con.execute("create table gold.t as select unnest(range(20)) as n")
    con.close()
    out = query_analytics(
        "select n from gold.t order by n",
        warehouse="analytics",
        limit=3,
        root=tmp_path,
    )
    assert out["ok"] is True
    assert len(out["rows"]) == 3
    assert out["truncated"] is True
