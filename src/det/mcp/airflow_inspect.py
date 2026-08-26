"""Read-only Airflow REST inspect helpers for DET MCP (Compose today, cloud-ready)."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

from det.logging import redact_uri_credentials
from det.mcp.context import project_root
from det.mcp.inspect import clamp_sample_limit

DET_DAG_IDS = (
    "det_extract_bronze",
    "det_backfill_extract_bronze",
    "det_dbt_silver_gold",
    "det_clear_lock",
)

DEFAULT_BASE_URL = "http://localhost:8080"
DEFAULT_USER = "airflow"
DEFAULT_PASSWORD = "airflow"
DEFAULT_TIMEOUT_SEC = 10.0
SUPPORTED_AUTH = frozenset({"basic"})

_ENV_LINE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")


@dataclass(frozen=True)
class AirflowSettings:
    base_url: str
    user: str
    password: str
    timeout_sec: float
    auth: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "user": self.user,
            "auth": self.auth,
            "timeout_sec": self.timeout_sec,
            "password_set": bool(self.password),
        }


def _root(root: Path | None = None) -> Path:
    return root.resolve() if root is not None else project_root()


def _parse_dotenv(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        m = _ENV_LINE.match(line)
        if not m:
            continue
        key, raw_val = m.group(1), m.group(2)
        val = raw_val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in {'"', "'"}:
            val = val[1:-1]
        out[key] = val
    return out


def _load_airflow_dotenv(root: Path) -> dict[str, str]:
    env_path = root / "airflow" / ".env"
    example = root / "airflow" / ".env.example"
    if env_path.is_file():
        return _parse_dotenv(env_path)
    return _parse_dotenv(example)


def airflow_settings(*, root: Path | None = None) -> AirflowSettings | dict[str, Any]:
    """
    Resolve connection settings: process env → airflow/.env → Compose defaults.

    Returns AirflowSettings, or an error dict when DET_AIRFLOW_AUTH is unsupported.
    """
    base = _root(root)
    file_env = _load_airflow_dotenv(base)

    auth = (os.environ.get("DET_AIRFLOW_AUTH") or "basic").strip().lower()
    if auth not in SUPPORTED_AUTH:
        return {
            "ok": False,
            "error": "unsupported_auth",
            "detail": (
                f"DET_AIRFLOW_AUTH={auth!r} is not implemented; "
                f"supported: {', '.join(sorted(SUPPORTED_AUTH))}. "
                "Bearer/OIDC/IAM are reserved for a future cloud setup."
            ),
            "auth": auth,
        }

    base_url = (
        os.environ.get("DET_AIRFLOW_BASE_URL")
        or f"http://localhost:{file_env.get('AIRFLOW_WEBSERVER_PORT', '8080')}"
        or DEFAULT_BASE_URL
    ).rstrip("/")
    if not base_url.startswith("http"):
        base_url = DEFAULT_BASE_URL

    user = (
        os.environ.get("DET_AIRFLOW_USER")
        or file_env.get("_AIRFLOW_WWW_USER_USERNAME")
        or DEFAULT_USER
    )
    password = (
        os.environ.get("DET_AIRFLOW_PASSWORD")
        or file_env.get("_AIRFLOW_WWW_USER_PASSWORD")
        or DEFAULT_PASSWORD
    )
    try:
        timeout = float(
            os.environ.get("DET_AIRFLOW_TIMEOUT_SEC") or DEFAULT_TIMEOUT_SEC
        )
    except ValueError:
        timeout = DEFAULT_TIMEOUT_SEC

    return AirflowSettings(
        base_url=base_url,
        user=user,
        password=password,
        timeout_sec=max(1.0, timeout),
        auth=auth,
    )


def _is_local_url(base_url: str) -> bool:
    host = base_url.lower()
    return "localhost" in host or "127.0.0.1" in host


def _unreachable_note(base_url: str, exc: BaseException) -> str:
    from det.mcp.errors import sanitize_detail

    detail = sanitize_detail(exc) if isinstance(exc, Exception) else str(exc)
    if _is_local_url(base_url):
        return (
            f"Airflow unreachable at {base_url} ({detail}). "
            "Start local Compose with `make airflow-up`, then retry."
        )
    return (
        f"Airflow unreachable at {base_url} ({detail}). "
        "Check DET_AIRFLOW_BASE_URL, credentials, and network."
    )


def _resolve_settings(
    root: Path | None = None,
) -> tuple[AirflowSettings | None, dict[str, Any] | None]:
    settings = airflow_settings(root=root)
    if isinstance(settings, dict):
        return None, settings
    return settings, None


def _request(
    settings: AirflowSettings,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> tuple[Any | None, dict[str, Any] | None]:
    """Return (json_or_text, error_dict)."""
    url = urljoin(settings.base_url.rstrip("/") + "/", path.lstrip("/"))
    try:
        resp = requests.request(
            method,
            url,
            auth=(settings.user, settings.password),
            params=params,
            timeout=settings.timeout_sec,
        )
    except requests.RequestException as exc:
        return None, {
            "ok": False,
            "error": "unreachable",
            "base_url": settings.base_url,
            "note": _unreachable_note(settings.base_url, exc),
        }

    if resp.status_code == 401:
        return None, {
            "ok": False,
            "error": "unauthorized",
            "base_url": settings.base_url,
            "note": (
                f"Airflow returned 401 for {settings.base_url}. "
                "Check DET_AIRFLOW_USER / DET_AIRFLOW_PASSWORD "
                "(or airflow/.env _AIRFLOW_WWW_USER_*)."
            ),
        }
    if resp.status_code >= 400:
        return None, {
            "ok": False,
            "error": "http_error",
            "base_url": settings.base_url,
            "status_code": resp.status_code,
            "note": f"Airflow HTTP {resp.status_code} for {path}: {resp.text[:300]}",
        }
    ctype = resp.headers.get("content-type", "")
    if "application/json" in ctype:
        try:
            return resp.json(), None
        except ValueError:
            return None, {
                "ok": False,
                "error": "invalid_json",
                "base_url": settings.base_url,
                "note": f"Non-JSON body from {path}",
            }
    return resp.text, None


def daily_logical_dates_for_interval(
    interval_start: str, interval_end: str
) -> list[str]:
    """Map DET ``[interval_start, interval_end)`` to Airflow @daily logical_date ISOs."""
    start = date.fromisoformat(interval_start.strip()[:10])
    end = date.fromisoformat(interval_end.strip()[:10])
    if end <= start:
        raise ValueError(
            f"interval_end ({end.isoformat()}) must be after "
            f"interval_start ({start.isoformat()})"
        )
    out: list[str] = []
    day = start
    while day < end:
        logical = datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)
        out.append(logical.isoformat())
        day += timedelta(days=1)
    return out


def airflow_health(*, root: Path | None = None) -> dict[str, Any]:
    settings, err = _resolve_settings(root)
    if err is not None:
        return err
    assert settings is not None
    data, req_err = _request(settings, "GET", "/health")
    if req_err is not None:
        return req_err
    return {
        "ok": True,
        "base_url": settings.base_url,
        "settings": settings.public_dict(),
        "health": data,
    }


def list_airflow_dags(*, root: Path | None = None) -> dict[str, Any]:
    settings, err = _resolve_settings(root)
    if err is not None:
        return err
    assert settings is not None
    data, req_err = _request(
        settings,
        "GET",
        "/api/v1/dags",
        params={"limit": 100},
    )
    if req_err is not None:
        return req_err
    dags_raw = []
    if isinstance(data, dict):
        dags_raw = data.get("dags") or []
    wanted = set(DET_DAG_IDS)
    dags = []
    for d in dags_raw:
        if not isinstance(d, dict):
            continue
        dag_id = d.get("dag_id")
        if dag_id not in wanted:
            continue
        dags.append(
            {
                "dag_id": dag_id,
                "is_paused": d.get("is_paused"),
                "has_import_errors": d.get("has_import_errors"),
                "schedule_interval": d.get("schedule_interval"),
                "tags": d.get("tags") or [],
            }
        )
    missing = sorted(wanted - {d["dag_id"] for d in dags})
    return {
        "ok": True,
        "base_url": settings.base_url,
        "dags": dags,
        "missing_det_dags": missing,
        "note": (
            "Filtered to DET DAG ids. Unpause in the UI if is_paused is true."
            if dags
            else "No DET DAGs found — check import errors and that dags/ is mounted."
        ),
    }


def list_airflow_dag_runs(
    dag_id: str,
    *,
    limit: int = 10,
    root: Path | None = None,
) -> dict[str, Any]:
    settings, err = _resolve_settings(root)
    if err is not None:
        return err
    assert settings is not None
    capped = clamp_sample_limit(limit)
    data, req_err = _request(
        settings,
        "GET",
        f"/api/v1/dags/{dag_id}/dagRuns",
        params={"limit": capped, "order_by": "-execution_date"},
    )
    if req_err is not None:
        return req_err
    runs_raw = []
    if isinstance(data, dict):
        runs_raw = data.get("dag_runs") or []
    runs = []
    for r in runs_raw[:capped]:
        if not isinstance(r, dict):
            continue
        runs.append(
            {
                "dag_run_id": r.get("dag_run_id"),
                "state": r.get("state"),
                "logical_date": r.get("logical_date") or r.get("execution_date"),
                "data_interval_start": r.get("data_interval_start"),
                "data_interval_end": r.get("data_interval_end"),
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
            }
        )
    return {
        "ok": True,
        "base_url": settings.base_url,
        "dag_id": dag_id,
        "limit": capped,
        "runs": runs,
    }


def describe_airflow_det_env(*, root: Path | None = None) -> dict[str, Any]:
    """Local Compose helper: read airflow/.env (or .env.example); redact secrets."""
    base = _root(root)
    env_path = base / "airflow" / ".env"
    example_path = base / "airflow" / ".env.example"
    source = env_path if env_path.is_file() else example_path
    raw = _parse_dotenv(source) if source.is_file() else {}

    redact_keys = {
        "_AIRFLOW_WWW_USER_PASSWORD",
        "DET_AIRFLOW_PASSWORD",
        "POSTGRES_PASSWORD",
    }
    public: dict[str, str] = {}
    for key, val in sorted(raw.items()):
        if key in redact_keys or key.endswith("_PASSWORD") or "SECRET" in key:
            public[key] = "***"
        else:
            # DET_PIPELINE_OVERRIDES can carry --set destination.connection=<DSN>.
            public[key] = redact_uri_credentials(val)

    det_keys = {k: v for k, v in public.items() if k.startswith("DET_")}
    path_notes: list[str] = []
    for key in ("DET_ANALYTICS_DUCKDB", "DET_OPS_DUCKDB"):
        value = raw.get(key, "")
        if value and not (value.startswith("/") or Path(value).is_absolute()):
            path_notes.append(
                f"{key} should be absolute in Compose/Airflow (got {value!r})"
            )

    client = airflow_settings(root=base)
    client_public: dict[str, Any]
    if isinstance(client, AirflowSettings):
        client_public = client.public_dict()
    else:
        client_public = {"auth_error": True, "detail": client.get("detail")}

    rel_source = "airflow/.env.example"
    if source.is_file():
        try:
            rel_source = str(source.resolve().relative_to(base))
        except ValueError:
            rel_source = str(source)

    return {
        "ok": True,
        "source_path": rel_source,
        "webserver_port": raw.get("AIRFLOW_WEBSERVER_PORT", "8080"),
        "web_user": raw.get("_AIRFLOW_WWW_USER_USERNAME", DEFAULT_USER),
        "password_set": bool(raw.get("_AIRFLOW_WWW_USER_PASSWORD")),
        "det": det_keys,
        "analytics_duckdb_notes": path_notes,
        "airflow_client": client_public,
        "note": (
            "Local Compose helper reading airflow/.env. "
            "Cloud deployments set the same DET_* keys on workers/secrets "
            "(not this file). Override agent→API connection with DET_AIRFLOW_*."
        ),
    }


def preview_backfill_conf(
    interval_start: str,
    interval_end: str,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Preview backfill trigger conf + logical dates. Never triggers a DagRun."""
    _ = root
    from det.runtime.approval import backfill_write_argv, make_plan

    try:
        logical_dates = daily_logical_dates_for_interval(interval_start, interval_end)
    except ValueError as exc:
        return {"ok": False, "error": "invalid_interval", "detail": str(exc)}

    start = interval_start.strip()[:10]
    end = interval_end.strip()[:10]
    conf = {
        "interval_start": start,
        "interval_end": end,
    }
    argv = backfill_write_argv(start, end)
    plan = make_plan("backfill", argv)
    conf_with_approval = {**conf, "approval": "apr_…"}
    conf_json = json.dumps(conf_with_approval)
    compose_hint = (
        "cd airflow && docker compose exec airflow-scheduler "
        f"airflow dags trigger det_backfill_extract_bronze --conf '{conf_json}'"
    )
    generic_hint = (
        f"airflow dags trigger det_backfill_extract_bronze --conf '{conf_json}'"
    )
    return {
        "ok": True,
        "dag_id": "det_backfill_extract_bronze",
        "conf": conf,
        "conf_json": json.dumps(conf),
        "logical_dates": logical_dates,
        "day_count": len(logical_dates),
        "approval_plan": plan.to_dict(),
        "suggested_commands": {
            "compose": compose_hint,
            "generic": generic_hint,
        },
        "note": (
            "Dry-run only — MCP never triggers DagRuns. "
            "Operator: det approve --plan <approval_plan> --approved-by <id>, "
            "then trigger with conf.approval set to that apr_…. "
            "Child extract DagRuns stay approval-free."
        ),
    }
