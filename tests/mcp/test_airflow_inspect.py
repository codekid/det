from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from det.mcp.airflow_inspect import (
    DET_DAG_IDS,
    airflow_health,
    airflow_settings,
    describe_airflow_det_env,
    list_airflow_dag_runs,
    list_airflow_dags,
    preview_backfill_conf,
)
from det.mcp.server import create_server


def test_unsupported_auth(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DET_AIRFLOW_AUTH", "bearer")
    out = airflow_settings(root=tmp_path)
    assert isinstance(out, dict)
    assert out["ok"] is False
    assert out["error"] == "unsupported_auth"


def test_custom_base_url_used(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DET_AIRFLOW_BASE_URL", "http://airflow.example:8080")
    monkeypatch.setenv("DET_AIRFLOW_USER", "u")
    monkeypatch.setenv("DET_AIRFLOW_PASSWORD", "p")
    monkeypatch.setenv("DET_AIRFLOW_AUTH", "basic")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {"metadatabase": {"status": "healthy"}}

    with patch("det.mcp.airflow_inspect.requests.request", return_value=mock_resp) as req:
        out = airflow_health(root=tmp_path)
    assert out["ok"] is True
    assert out["base_url"] == "http://airflow.example:8080"
    assert req.call_args.kwargs["auth"] == ("u", "p")
    assert req.call_args.args[1].startswith("http://airflow.example:8080/")


def test_unreachable_returns_ok_false(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import requests

    monkeypatch.setenv("DET_AIRFLOW_BASE_URL", "http://127.0.0.1:9")
    with patch(
        "det.mcp.airflow_inspect.requests.request",
        side_effect=requests.ConnectionError("down"),
    ):
        out = airflow_health(root=tmp_path)
    assert out["ok"] is False
    assert out["error"] == "unreachable"
    assert "make airflow-up" in out["note"]


def test_list_dags_filters_det(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DET_AIRFLOW_BASE_URL", "http://localhost:8080")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {
        "dags": [
            {
                "dag_id": "det_extract_bronze",
                "is_paused": True,
                "has_import_errors": False,
            },
            {"dag_id": "other_dag", "is_paused": False, "has_import_errors": False},
        ]
    }
    with patch("det.mcp.airflow_inspect.requests.request", return_value=mock_resp):
        out = list_airflow_dags(root=tmp_path)
    assert out["ok"] is True
    assert len(out["dags"]) == 1
    assert out["dags"][0]["dag_id"] == "det_extract_bronze"
    assert "det_dbt_silver_gold" in out["missing_det_dags"]


def test_list_dag_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DET_AIRFLOW_BASE_URL", "http://localhost:8080")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "application/json"}
    mock_resp.json.return_value = {
        "dag_runs": [
            {
                "dag_run_id": "manual__1",
                "state": "success",
                "logical_date": "2026-08-02T00:00:00+00:00",
                "data_interval_start": "2026-08-01T00:00:00+00:00",
                "data_interval_end": "2026-08-02T00:00:00+00:00",
            }
        ]
    }
    with patch("det.mcp.airflow_inspect.requests.request", return_value=mock_resp):
        out = list_airflow_dag_runs("det_extract_bronze", limit=5, root=tmp_path)
    assert out["ok"] is True
    assert out["runs"][0]["state"] == "success"


def test_describe_env_redacts_password(tmp_path: Path):
    env_dir = tmp_path / "airflow"
    env_dir.mkdir()
    (env_dir / ".env").write_text(
        "\n".join(
            [
                "_AIRFLOW_WWW_USER_USERNAME=airflow",
                "_AIRFLOW_WWW_USER_PASSWORD=secret",
                "DET_PIPELINE_CONFIG=noaa.storm_events",
                "DET_ANALYTICS_DUCKDB=/opt/det/data/analytics.duckdb",
                "",
            ]
        ),
        encoding="utf-8",
    )
    out = describe_airflow_det_env(root=tmp_path)
    assert out["ok"] is True
    assert out["password_set"] is True
    assert out["det"]["DET_PIPELINE_CONFIG"] == "noaa.storm_events"
    assert out["web_user"] == "airflow"
    # Password value must not appear in structured fields
    blob = json.dumps(
        {k: v for k, v in out.items() if k != "note"}, default=str
    )
    assert "secret" not in blob
    assert "_AIRFLOW_WWW_USER_PASSWORD" not in out.get("det", {})


def test_preview_backfill_conf():
    out = preview_backfill_conf("2026-08-01", "2026-08-03")
    assert out["ok"] is True
    assert out["day_count"] == 2
    assert out["conf"] == {
        "interval_start": "2026-08-01",
        "interval_end": "2026-08-03",
    }
    assert len(out["logical_dates"]) == 2
    assert "docker compose exec" in out["suggested_commands"]["compose"]
    assert out["suggested_commands"]["generic"].startswith(
        "airflow dags trigger det_backfill_extract_bronze"
    )


def test_det_dag_ids_include_clear_lock():
    assert "det_clear_lock" in DET_DAG_IDS
    assert "det_extract_bronze" in DET_DAG_IDS


def test_create_server_registers_airflow_tools():
    server = create_server()
    names = sorted(server._tool_manager._tools)
    for expected in (
        "airflow_health",
        "list_airflow_dags",
        "list_airflow_dag_runs",
        "describe_airflow_det_env",
        "preview_backfill_conf",
    ):
        assert expected in names
