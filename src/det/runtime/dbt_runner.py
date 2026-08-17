from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from det.logging import bound_run_context, get_logger, sanitize_lake_uri
from det.runtime.config import PipelineConfig, load_pipeline_config, resolve_path
from det.runtime.ids import dbt_model_slug, sql_names_for_config
from det.runtime.lake import open_lake, pick_lake_spec

logger = get_logger(__name__)

DbtCommand = Literal["build", "run", "test"]


def find_dbt_executable() -> str | None:
    """Prefer the dbt in the active venv, then PATH.

    Do not Path.resolve() sys.executable: uv venvs symlink python to a base
    interpreter outside .venv, which would miss .venv/bin/dbt.
    """
    candidates = [
        Path(sys.executable).parent / "dbt",
        Path(sys.prefix) / "bin" / "dbt",
    ]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return shutil.which("dbt")


@dataclass(frozen=True)
class DbtRunResult:
    command: list[str]
    returncode: int
    project_dir: Path
    select: tuple[str, ...]
    lake_path: str | None = None
    bronze_source: str | None = None
    output: str = ""


def _run_dbt_subprocess(
    argv: list[str],
    *,
    cwd: str,
    env: dict[str, str],
) -> tuple[int, str]:
    """Run dbt, stream combined stdout/stderr live, and return (code, full text).

    Streaming via sys.stdout is what Airflow task logs capture; a bare
    subprocess.run(..., inherit) often does not show up in the UI.
    """
    # dbt is Python; unbuffered helps lines appear before process exit.
    run_env = dict(env)
    run_env.setdefault("PYTHONUNBUFFERED", "1")

    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        env=run_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    chunks: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        chunks.append(line)
        sys.stdout.write(line)
        sys.stdout.flush()
    returncode = proc.wait()
    return returncode, "".join(chunks)


class DbtNotInstalledError(RuntimeError):
    """Raised when the dbt CLI is not available on PATH."""


def resolve_dbt_project_dir(project_root: Path, project_dir: Path | str | None = None) -> Path:
    if project_dir is None:
        return (project_root / "dbt").resolve()
    path = Path(project_dir)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def default_select_for_pipeline(config: PipelineConfig) -> list[str]:
    """
    Parent stg + downstream, plus each ``dbt.stg.relations`` child stg+
    (including nested relations).

    Relation models read bronze directly (not ``ref`` parent), so they are not
    pulled in by ``stg_<parent>+`` alone.
    """
    from det.scaffold.flatten import iter_relation_paths

    slug = dbt_model_slug(config.name)
    selects = [f"stg_{slug}+"]
    for name_parts, _chain, _rel in iter_relation_paths(config.dbt.stg.relations):
        selects.append(f"stg_{slug}__{'__'.join(name_parts)}+")
    return selects


def build_dbt_argv(
    *,
    command: DbtCommand,
    project_dir: Path,
    profiles_dir: Path | None = None,
    select: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    target: str | None = None,
    full_refresh: bool = False,
    extra_args: Sequence[str] | None = None,
    dbt_executable: str = "dbt",
) -> list[str]:
    profiles = profiles_dir or project_dir
    argv = [
        dbt_executable,
        command,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles),
    ]
    if select:
        argv.extend(["--select", *select])
    if exclude:
        argv.extend(["--exclude", *exclude])
    if target:
        argv.extend(["--target", target])
    if full_refresh:
        argv.append("--full-refresh")
    if extra_args:
        argv.extend(extra_args)
    return argv


OPS_TAG_EXCLUDE = "tag:ops"


def is_ops_selector(selector: str) -> bool:
    """True when a ``--select`` token intentionally targets ops models."""
    text = selector.strip().lower()
    if not text:
        return False
    if "tag:ops" in text:
        return True
    if text == "ops" or text.startswith("ops.") or text.startswith("path:models/ops"):
        return True
    if text.startswith("stg_det__"):
        return True
    return False


def analytics_exclude(select: Sequence[str] | None) -> list[str] | None:
    """Exclude ops from analytics builds unless select explicitly targets ops."""
    if select and any(is_ops_selector(s) for s in select):
        return None
    return [OPS_TAG_EXCLUDE]


def ops_dbt_target(select: Sequence[str] | None) -> str | None:
    """Use profile target ``ops`` when select is ops-only."""
    if select and all(is_ops_selector(s) for s in select):
        return "ops"
    return None


def run_dbt(
    *,
    project_root: Path,
    command: DbtCommand = "build",
    project_dir: Path | str | None = None,
    profiles_dir: Path | str | None = None,
    select: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    target: str | None = None,
    full_refresh: bool = False,
    lake_path: str | Path | None = None,
    pipeline: PipelineConfig | Path | str | None = None,
    pipeline_overrides: Sequence[str] | None = None,
    extra_args: Sequence[str] | None = None,
    dry_run: bool = False,
) -> DbtRunResult:
    """
    Invoke the dbt CLI for local/testing use.

    Sets DET_LAKE_PATH from --lake-path, destination.path, existing env, or
    ``./data/lake``. Requires the optional `[dbt]` extra.
    """
    root = project_root.resolve()
    dbt_dir = resolve_dbt_project_dir(root, project_dir)
    if not (dbt_dir / "dbt_project.yml").exists():
        raise FileNotFoundError(f"No dbt_project.yml under {dbt_dir}")

    config: PipelineConfig | None = None
    if pipeline is not None:
        if isinstance(pipeline, PipelineConfig):
            config = pipeline
        else:
            config = load_pipeline_config(
                resolve_path(root, str(pipeline)), overrides=pipeline_overrides
            )

    resolved_select = list(select) if select else None
    if resolved_select is None and config is not None:
        resolved_select = default_select_for_pipeline(config)

    resolved_exclude = list(exclude) if exclude is not None else None
    resolved_target = target if target is not None else ops_dbt_target(resolved_select)

    profiles = (
        resolve_dbt_project_dir(root, profiles_dir)
        if profiles_dir is not None
        else dbt_dir
    )

    env = os.environ.copy()
    spec = pick_lake_spec(
        cli_lake_path=str(lake_path).strip() if lake_path is not None else None,
        destination_path=config.destination.path if config is not None else None,
        env=env,
    )
    lake = open_lake(spec, root)
    env["DET_LAKE_PATH"] = str(lake)
    if resolved_target == "ops":
        env.setdefault(
            "DET_OPS_DUCKDB",
            str((root / "data" / "det_ops.duckdb").resolve()),
        )

    # Native DuckDB bronze vs JSONL lake — DET_BRONZE_SCHEMA is bronze_{provider}.
    if config is not None:
        sql_schema, _ = sql_names_for_config(config)
        if config.destination.type == "duckdb":
            env["DET_BRONZE_SOURCE"] = "duckdb"
            env["DET_BRONZE_SCHEMA"] = sql_schema
        elif config.destination.type == "iceberg":
            env["DET_BRONZE_SOURCE"] = "iceberg"
            env["DET_BRONZE_SCHEMA"] = sql_schema
        else:
            env.setdefault("DET_BRONZE_SOURCE", "filesystem")
            env["DET_BRONZE_SCHEMA"] = sql_schema
    else:
        env.setdefault("DET_BRONZE_SOURCE", "filesystem")
        env.setdefault("DET_BRONZE_SCHEMA", "bronze")

    dbt_bin = find_dbt_executable()
    argv = build_dbt_argv(
        command=command,
        project_dir=dbt_dir,
        profiles_dir=profiles,
        select=resolved_select,
        exclude=resolved_exclude,
        target=resolved_target,
        full_refresh=full_refresh,
        extra_args=extra_args,
        dbt_executable=dbt_bin or "dbt",
    )
    resolved_lake = env.get("DET_LAKE_PATH")
    bronze_source = env.get("DET_BRONZE_SOURCE")
    lake_for_logs = sanitize_lake_uri(resolved_lake) if resolved_lake else None

    with bound_run_context(
        command="dbt",
        pipeline=config.name if config is not None else None,
        destination=config.destination.type if config is not None else None,
        lake=lake_for_logs,
    ):
        if dry_run:
            logger.info(
                "dbt dry-run",
                argv=argv,
                DET_LAKE_PATH=lake_for_logs,
                DET_BRONZE_SOURCE=bronze_source,
            )
            return DbtRunResult(
                command=argv,
                returncode=0,
                project_dir=dbt_dir,
                select=tuple(resolved_select or ()),
                lake_path=resolved_lake,
                bronze_source=bronze_source,
                output="",
            )

        if dbt_bin is None:
            raise DbtNotInstalledError(
                "dbt CLI not found next to the current Python or on PATH. "
                'Install the optional extra: pip install -e ".[dbt]" '
                '(or uv pip install -e ".[dbt]")'
            )

        logger.info(
            "running dbt",
            argv=argv,
            DET_LAKE_PATH=lake_for_logs,
            DET_BRONZE_SOURCE=bronze_source,
            cwd=str(dbt_dir),
        )
        returncode, output = _run_dbt_subprocess(argv, cwd=str(dbt_dir), env=env)
        return DbtRunResult(
            command=argv,
            returncode=returncode,
            project_dir=dbt_dir,
            select=tuple(resolved_select or ()),
            lake_path=resolved_lake,
            bronze_source=bronze_source,
            output=output,
        )
