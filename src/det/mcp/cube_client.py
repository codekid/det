"""Read-only Cube Core REST client for DET MCP (local Compose, not Cube Cloud)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

from det.mcp.context import project_root
from det.mcp.inspect import clamp_sample_limit

DEFAULT_BASE_URL = "http://localhost:4000"
DEFAULT_TIMEOUT_SEC = 10.0
CUBE_DOWN_HINT = (
    "Cube Core is not reachable. Start it with `make cube-up` "
    "(http://localhost:4000) or set DET_CUBE_BASE_URL."
)


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def cube_jwt(secret: str, *, ttl_sec: int = 3600) -> str:
    """HS256 JWT Cube REST expects (payload is iat/exp only)."""
    header_obj = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    payload_obj = {"iat": now, "exp": now + ttl_sec}
    header = _b64url(json.dumps(header_obj, separators=(",", ":")).encode())
    payload = _b64url(json.dumps(payload_obj, separators=(",", ":")).encode())
    sig = hmac.new(
        secret.encode("utf-8"),
        f"{header}.{payload}".encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{header}.{payload}.{_b64url(sig)}"


def cube_settings(*, root: Path | None = None) -> dict[str, Any]:
    """Resolve DET_CUBE_* from env, then cube/.env / cube/.env.example."""
    base = os.environ.get("DET_CUBE_BASE_URL", "").strip() or DEFAULT_BASE_URL
    secret = os.environ.get("DET_CUBE_API_SECRET", "").strip()
    if not secret:
        env_root = _root(root)
        for name in (".env", ".env.example"):
            path = env_root / "cube" / name
            if not path.is_file():
                continue
            for line in path.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if stripped.startswith("CUBEJS_API_SECRET="):
                    secret = stripped.split("=", 1)[1].strip().strip("'\"")
                    break
            if secret:
                break
    return {
        "base_url": base.rstrip("/"),
        "api_secret_set": bool(secret),
        "timeout_sec": DEFAULT_TIMEOUT_SEC,
        "_secret": secret,
    }


def _unavailable(settings: dict[str, Any], detail: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": "cube_unavailable",
        "detail": detail,
        "suggested": "make cube-up",
        "hint": CUBE_DOWN_HINT,
        "base_url": settings["base_url"],
    }


def _request(
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    settings = cube_settings(root=root)
    secret = settings.get("_secret") or ""
    if not secret:
        return _unavailable(
            settings,
            "DET_CUBE_API_SECRET / CUBEJS_API_SECRET is not set "
            "(copy cube/.env.example → cube/.env)",
        )
    url = urljoin(settings["base_url"] + "/", path.lstrip("/"))
    headers = {
        "Authorization": cube_jwt(secret),
        "Content-Type": "application/json",
    }
    try:
        resp = requests.request(
            method,
            url,
            headers=headers,
            json=json_body,
            params=params,
            timeout=settings["timeout_sec"],
        )
    except requests.RequestException as exc:
        from det.mcp.errors import sanitize_detail

        return _unavailable(settings, sanitize_detail(exc))
    if resp.status_code >= 500 or resp.status_code in {401, 403, 404}:
        if resp.status_code in {401, 403}:
            return {
                "ok": False,
                "error": "cube_auth_failed",
                "detail": f"HTTP {resp.status_code}",
                "base_url": settings["base_url"],
            }
        if resp.status_code == 404:
            return _unavailable(settings, f"HTTP {resp.status_code} for {path}")
        return _unavailable(settings, f"HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        payload = resp.json()
    except ValueError:
        return _unavailable(settings, f"non-JSON response HTTP {resp.status_code}")
    if resp.status_code >= 400:
        err = payload.get("error") if isinstance(payload, dict) else None
        return {
            "ok": False,
            "error": "cube_query_failed",
            "detail": err or f"HTTP {resp.status_code}",
            "base_url": settings["base_url"],
        }
    return {"ok": True, "base_url": settings["base_url"], "payload": payload}


def cube_meta(*, root: Path | None = None) -> dict[str, Any]:
    """GET /cubejs-api/v1/meta — cubes, measures, dimensions."""
    result = _request("GET", "/cubejs-api/v1/meta", root=root)
    if not result.get("ok"):
        return result
    payload = result["payload"]
    cubes = payload.get("cubes") if isinstance(payload, dict) else None
    return {
        "ok": True,
        "base_url": result["base_url"],
        "cubes": cubes if isinstance(cubes, list) else payload,
        "note": (
            "Certified metrics. Prefer cube_load over inventing SQL. "
            "yearly_damage is gold; run_daily is ops (already daily grain — "
            "do not re-sum p95_ms)."
        ),
    }


def cube_load(
    *,
    measures: list[str],
    dimensions: list[str] | None = None,
    filters: list[dict[str, Any]] | None = None,
    limit: int | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """POST /cubejs-api/v1/load — Cube query (measures/dimensions/filters)."""
    capped = clamp_sample_limit(limit)
    if not measures:
        return {
            "ok": False,
            "error": "invalid_query",
            "detail": "measures is required (e.g. yearly_damage.total_property_damage)",
        }
    query: dict[str, Any] = {"measures": measures, "limit": capped}
    if dimensions:
        query["dimensions"] = dimensions
    if filters:
        query["filters"] = filters
    result = _request(
        "POST",
        "/cubejs-api/v1/load",
        json_body={"query": query},
        root=root,
    )
    if not result.get("ok"):
        return result
    payload = result["payload"]
    data = payload.get("data") if isinstance(payload, dict) else None
    return {
        "ok": True,
        "base_url": result["base_url"],
        "query": query,
        "data": data if isinstance(data, list) else payload,
        "annotation": payload.get("annotation") if isinstance(payload, dict) else None,
        "note": "Do not invent gold/ops metric SQL; this is the certified path.",
    }
