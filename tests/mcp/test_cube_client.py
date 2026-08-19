from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests

from det.mcp.cube_client import cube_load, cube_meta, cube_settings


def test_cube_meta_unavailable_without_secret(monkeypatch, tmp_path):
    monkeypatch.delenv("DET_CUBE_API_SECRET", raising=False)
    monkeypatch.delenv("DET_CUBE_BASE_URL", raising=False)
    out = cube_meta(root=tmp_path)
    assert out["ok"] is False
    assert out["error"] == "cube_unavailable"
    assert out["suggested"] == "make cube-up"


def test_cube_meta_connection_error(monkeypatch, tmp_path):
    monkeypatch.setenv("DET_CUBE_API_SECRET", "test-secret")
    monkeypatch.setenv("DET_CUBE_BASE_URL", "http://localhost:4000")
    with patch(
        "det.mcp.cube_client.requests.request",
        side_effect=requests.ConnectionError("connection refused"),
    ):
        out = cube_meta(root=tmp_path)
    assert out["ok"] is False
    assert out["error"] == "cube_unavailable"
    assert "make cube-up" in out["suggested"]


def test_cube_meta_and_load_mocked(monkeypatch, tmp_path):
    monkeypatch.setenv("DET_CUBE_API_SECRET", "test-secret")
    monkeypatch.setenv("DET_CUBE_BASE_URL", "http://cube.test:4000")

    meta_resp = MagicMock()
    meta_resp.status_code = 200
    meta_resp.json.return_value = {
        "cubes": [{"name": "yearly_damage", "measures": [{"name": "total_property_damage"}]}]
    }

    load_resp = MagicMock()
    load_resp.status_code = 200
    load_resp.json.return_value = {
        "data": [{"yearly_damage.total_property_damage": 10, "yearly_damage.state": "TX"}],
        "annotation": {},
    }

    with patch("det.mcp.cube_client.requests.request", return_value=meta_resp) as req:
        meta = cube_meta(root=tmp_path)
        assert meta["ok"] is True
        assert meta["cubes"][0]["name"] == "yearly_damage"
        assert req.call_args.kwargs["headers"]["Authorization"].count(".") == 2

    with patch("det.mcp.cube_client.requests.request", return_value=load_resp) as req:
        loaded = cube_load(
            measures=["yearly_damage.total_property_damage"],
            dimensions=["yearly_damage.state"],
            limit=5,
            root=tmp_path,
        )
        assert loaded["ok"] is True
        assert loaded["data"][0]["yearly_damage.state"] == "TX"
        body = req.call_args.kwargs["json"]
        assert body["query"]["limit"] == 5
        assert body["query"]["measures"] == ["yearly_damage.total_property_damage"]


def test_cube_load_requires_measures(monkeypatch, tmp_path):
    monkeypatch.setenv("DET_CUBE_API_SECRET", "test-secret")
    out = cube_load(measures=[], root=tmp_path)
    assert out["ok"] is False
    assert out["error"] == "invalid_query"


def test_cube_settings_reads_env_example(tmp_path, monkeypatch):
    monkeypatch.delenv("DET_CUBE_API_SECRET", raising=False)
    cube_dir = tmp_path / "cube"
    cube_dir.mkdir()
    (cube_dir / ".env.example").write_text(
        "CUBEJS_API_SECRET=from-example\n", encoding="utf-8"
    )
    settings = cube_settings(root=tmp_path)
    assert settings["api_secret_set"] is True
    assert settings["_secret"] == "from-example"
