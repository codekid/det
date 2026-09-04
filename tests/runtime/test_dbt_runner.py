from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from det.runtime.config import PipelineConfig, SourceConfig
from det.runtime.dbt_runner import (
    DbtNotInstalledError,
    _run_dbt_subprocess,
    analytics_exclude,
    build_dbt_argv,
    default_select_for_pipeline,
    is_ops_selector,
    ops_dbt_target,
    run_dbt,
)


def test_default_select_for_pipeline():
    config = PipelineConfig(
        name="noaa.storm_events",
        source=SourceConfig(type="noaa.storm_events"),
        schema_path="schemas/noaa/storm_events/storm_events.schema.yaml",
    )
    assert default_select_for_pipeline(config) == ["stg_noaa__storm_events+"]


def test_default_select_includes_stg_relations():
    config = PipelineConfig(
        name="example_api.orders",
        source=SourceConfig(type="example_api.orders"),
        schema_path="schemas/example_api/orders/orders.schema.yaml",
        dbt={
            "stg": {
                "relations": {
                    "discount_codes": {"materialized": "view"},
                    "line_items": {
                        "materialized": "view",
                        "relations": {"tax_lines": {"materialized": "view"}},
                    },
                },
            }
        },
    )
    assert default_select_for_pipeline(config) == [
        "stg_example_api__orders+",
        "stg_example_api__orders__discount_codes+",
        "stg_example_api__orders__line_items+",
        "stg_example_api__orders__line_items__tax_lines+",
    ]


def test_build_dbt_argv():
    argv = build_dbt_argv(
        command="build",
        project_dir=Path("/proj/dbt"),
        select=["stg_mini+"],
        full_refresh=True,
    )
    assert argv[:2] == ["dbt", "build"]
    assert "--project-dir" in argv and "/proj/dbt" in argv
    assert argv[argv.index("--select") + 1] == "stg_mini+"
    assert "--full-refresh" in argv


def test_build_dbt_argv_exclude():
    argv = build_dbt_argv(
        command="build",
        project_dir=Path("/proj/dbt"),
        exclude=["tag:ops"],
    )
    assert argv[argv.index("--exclude") + 1] == "tag:ops"


def test_analytics_exclude_skips_when_selecting_ops():
    assert analytics_exclude(None) == ["tag:ops"]
    assert analytics_exclude(["stg_noaa__storm_events+"]) == ["tag:ops"]
    assert analytics_exclude(["tag:ops"]) is None
    assert analytics_exclude(["stg_det__run_receipts"]) is None
    assert is_ops_selector("path:models/ops")


def test_ops_dbt_target():
    assert ops_dbt_target(None) is None
    assert ops_dbt_target(["stg_noaa__storm_events+"]) is None
    assert ops_dbt_target(["tag:ops"]) == "ops"
    assert ops_dbt_target(["stg_det__run_receipts"]) == "ops"
    assert ops_dbt_target(["tag:ops"], "bigquery") == "bigquery"
    assert ops_dbt_target(["tag:ops"], "duckdb") == "ops"


def test_run_dbt_ops_select_uses_ops_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_OPS_DUCKDB", raising=False)
    monkeypatch.delenv("DET_DBT_TARGET", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    result = run_dbt(
        project_root=tmp_path,
        select=["tag:ops"],
        dry_run=True,
    )
    assert result.command[result.command.index("--target") + 1] == "ops"
    assert "--exclude" not in result.command


def test_run_dbt_ops_select_honors_det_dbt_target_bigquery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.setenv("DET_DBT_TARGET", "bigquery")
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    result = run_dbt(
        project_root=tmp_path,
        select=["tag:ops"],
        dry_run=True,
    )
    assert result.command[result.command.index("--target") + 1] == "bigquery"


def test_run_dbt_dry_run_sets_lake_and_select(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )

    result = run_dbt(
        project_root=tmp_path,
        pipeline=pipeline,
        dry_run=True,
    )
    assert result.returncode == 0
    assert result.select == ("stg_noaa__storm_events+",)
    assert result.lake_path == str((tmp_path / "data" / "lake").resolve())
    assert result.bronze_source == "filesystem"
    assert "stg_noaa__storm_events+" in result.command


def test_run_dbt_sets_duckdb_bronze_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_BRONZE_SOURCE", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        f"""
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: duckdb
  path: ./data/lake
  connection: {tmp_path / "analytics.duckdb"}
  dataset: bronze
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(project_root=tmp_path, pipeline=pipeline, dry_run=True)
    assert result.bronze_source == "duckdb"


def test_run_dbt_sets_iceberg_bronze_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_BRONZE_SOURCE", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: iceberg
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(project_root=tmp_path, pipeline=pipeline, dry_run=True)
    assert result.bronze_source == "iceberg"


def _mini_iceberg_pipeline(tmp_path: Path) -> Path:
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: example_api.events
source:
  type: example_api.events
schema: schemas/mini.schema.yaml
destination:
  type: iceberg
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    return pipeline


def test_run_dbt_s3_lake_uses_duckdb_s3_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.delenv("DET_DBT_TARGET", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9000")
    pipeline = _mini_iceberg_pipeline(tmp_path)
    result = run_dbt(
        project_root=tmp_path,
        pipeline=pipeline,
        lake_path="s3://det-ci/det-lake",
        dry_run=True,
    )
    assert result.command[result.command.index("--target") + 1] == "duckdb_s3"
    assert result.lake_path == "s3://det-ci/det-lake"


def test_run_dbt_gs_lake_does_not_force_duckdb_s3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("gcsfs")
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.delenv("DET_DBT_TARGET", raising=False)
    pipeline = _mini_iceberg_pipeline(tmp_path)
    result = run_dbt(
        project_root=tmp_path,
        pipeline=pipeline,
        lake_path="gs://det-ci/det-lake",
        dry_run=True,
    )
    assert "--target" not in result.command
    assert result.lake_path == "gs://det-ci/det-lake"


def test_run_dbt_gs_lake_honors_det_dbt_target_bigquery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("gcsfs")
    monkeypatch.setenv("DET_LAKE_MODE", "cloud")
    monkeypatch.setenv("DET_DBT_TARGET", "bigquery")
    pipeline = _mini_iceberg_pipeline(tmp_path)
    result = run_dbt(
        project_root=tmp_path,
        pipeline=pipeline,
        lake_path="gs://det-ci/det-lake",
        dry_run=True,
    )
    assert result.command[result.command.index("--target") + 1] == "bigquery"


def test_run_dbt_local_lake_keeps_duckdb_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DET_LAKE_MODE", "local")
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_DBT_TARGET", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    pipeline = tmp_path / "pipe.yaml"
    pipeline.write_text(
        """
name: noaa.storm_events
source:
  type: noaa.storm_events
schema: schemas/mini.schema.yaml
destination:
  type: filesystem
  path: ./data/lake
""",
        encoding="utf-8",
    )
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "mini.schema.yaml").write_text(
        "type: object\nproperties: {}\n",
        encoding="utf-8",
    )
    result = run_dbt(project_root=tmp_path, pipeline=pipeline, dry_run=True)
    assert "--target" not in result.command


def test_run_dbt_missing_cli(tmp_path: Path):
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    with (
        patch("det.runtime.dbt_runner.find_dbt_executable", return_value=None),
        pytest.raises(DbtNotInstalledError),
    ):
        run_dbt(project_root=tmp_path, select=["silver_noaa__storm_events"])


def test_run_dbt_subprocess_streams_to_stdout(capsys: pytest.CaptureFixture[str]):
    proc = MagicMock()
    proc.stdout = io.StringIO("PASS=1\nDone\n")
    proc.wait.return_value = 0

    with patch("det.runtime.dbt_runner.subprocess.Popen", return_value=proc) as popen:
        code, output = _run_dbt_subprocess(
            ["dbt", "build"],
            cwd="/tmp/dbt",
            env={"PATH": "/bin"},
        )

    assert code == 0
    assert output == "PASS=1\nDone\n"
    assert "PASS=1" in capsys.readouterr().out
    assert popen.call_args.kwargs["stdout"] is not None
    assert popen.call_args.kwargs["env"]["PYTHONUNBUFFERED"] == "1"


def test_run_dbt_stores_streamed_output(tmp_path: Path):
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")

    with (
        patch("det.runtime.dbt_runner.find_dbt_executable", return_value="/usr/bin/dbt"),
        patch(
            "det.runtime.dbt_runner._run_dbt_subprocess",
            return_value=(1, "Compilation Error\n"),
        ),
    ):
        result = run_dbt(project_root=tmp_path, select=["stg_x+"])

    assert result.returncode == 1
    assert result.output == "Compilation Error\n"


def test_run_dbt_catchup_reads_manifest_from_resolved_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """--lake-path must drive catch-up manifest reads, not the default lake."""
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.delenv("DET_LAKE_PATH_RAW", raising=False)
    monkeypatch.delenv("DET_LAKE_PATH_BRONZE", raising=False)
    monkeypatch.delenv("DET_LAKE_PATH_OPS", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    other = tmp_path / "other_lake"
    mid = "scm_" + ("ab" * 8)
    (other / "ops" / "silver_catchup").mkdir(parents=True)
    manifest = {
        "manifest_version": 1,
        "manifest_id": mid,
        "content_digest": "sha256:" + ("0" * 64),
        "runs": [
            {
                "pipeline": "noaa.storm_events",
                "extract_run_datetime": "2026-08-06T12:00:00+00:00",
                "interval_start": "2026-08-06T00:00:00+00:00",
                "interval_end": "2026-08-07T00:00:00+00:00",
            }
        ],
    }
    scm_path = other / "ops" / "silver_catchup" / f"{mid}.json"
    scm_path.write_text(__import__("json").dumps(manifest), encoding="utf-8")

    seen: dict[str, object] = {}

    def _fake_read(*, manifest_id, project_root, settings=None, lake_path=None):
        seen["lake_path"] = lake_path
        seen["manifest_id"] = manifest_id
        return manifest

    captured_env: dict[str, str] = {}

    def _fake_subprocess(argv, *, cwd, env):
        captured_env.update(env)
        return 0, ""

    with (
        patch(
            "det.runtime.silver_catchup.read_catchup_manifest",
            side_effect=_fake_read,
        ),
        patch(
            "det.runtime.silver_catchup.catchup_select_from_manifest",
            return_value=["silver_noaa__storm_events"],
        ),
        patch(
            "det.runtime.dbt_runner.find_dbt_executable",
            return_value="dbt",
        ),
        patch(
            "det.runtime.dbt_runner._run_dbt_subprocess",
            side_effect=_fake_subprocess,
        ),
    ):
        result = run_dbt(
            project_root=tmp_path,
            catchup=True,
            catchup_manifest=mid,
            lake_path=other,
            dry_run=False,
        )

    assert seen["lake_path"] == str(other.resolve())
    assert seen["manifest_id"] == mid
    assert "--vars" in result.command
    vars_idx = result.command.index("--vars")
    vars_json = result.command[vars_idx + 1]
    assert "det_catchup_by_pipeline" not in vars_json
    assert '"det_catchup":true' in vars_json.replace(" ", "")
    assert mid in vars_json
    assert "silver_noaa__storm_events" in result.select
    assert captured_env.get("DET_CATCHUP_MANIFEST_PATH") == str(scm_path.resolve())


def test_run_dbt_catchup_refuses_bigquery_on_local_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    lake = tmp_path / "lake"
    mid = "scm_" + ("cd" * 8)
    (lake / "ops" / "silver_catchup").mkdir(parents=True)
    manifest = {
        "manifest_version": 1,
        "manifest_id": mid,
        "content_digest": "sha256:" + ("1" * 64),
        "runs": [
            {
                "pipeline": "noaa.storm_events",
                "extract_run_datetime": "2026-08-06T12:00:00+00:00",
                "interval_start": "2026-08-06T00:00:00+00:00",
                "interval_end": "2026-08-07T00:00:00+00:00",
            }
        ],
    }
    (lake / "ops" / "silver_catchup" / f"{mid}.json").write_text(
        __import__("json").dumps(manifest), encoding="utf-8"
    )
    with (
        patch(
            "det.runtime.silver_catchup.read_catchup_manifest",
            return_value=manifest,
        ),
        patch(
            "det.runtime.silver_catchup.catchup_select_from_manifest",
            return_value=["silver_noaa__storm_events"],
        ),
        pytest.raises(ValueError, match="GCS ops lake"),
    ):
        run_dbt(
            project_root=tmp_path,
            catchup=True,
            catchup_manifest=mid,
            lake_path=lake,
            target="bigquery",
            dry_run=True,
        )


def test_run_dbt_catchup_bigquery_gcs_sets_bq_relation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.setenv("DET_GCP_PROJECT", "proj-test")
    monkeypatch.setenv("DET_BQ_DATASET", "analytics")
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    mid = "scm_" + ("ef" * 8)
    runs = [
        {
            "pipeline": "noaa.storm_events",
            "extract_run_datetime": "2026-08-06T12:00:00+00:00",
            "interval_start": "2026-08-06T00:00:00+00:00",
            "interval_end": "2026-08-07T00:00:00+00:00",
        }
    ]
    from det.runtime.silver_catchup import _runs_jsonl_bytes, catchup_content_digest

    digest = catchup_content_digest(runs)
    manifest = {
        "manifest_version": 1,
        "manifest_id": mid,
        "content_digest": digest,
        "runs": runs,
    }
    scm_uri = f"gs://bucket/ops/silver_catchup/{mid}.json"
    runs_uri = f"gs://bucket/ops/silver_catchup/{mid}.runs.jsonl"
    runs_body = _runs_jsonl_bytes(runs).decode("utf-8")

    class _FakeRef:
        def __init__(self, uri: str, *, exists: bool = True, text: str = "") -> None:
            self._uri = uri
            self._exists = exists
            self._text = text

        def __str__(self) -> str:
            return self._uri

        def exists(self) -> bool:
            return self._exists

        def read_text(self, encoding: str = "utf-8") -> str:
            return self._text

    captured_env: dict[str, str] = {}
    ensure_seen: dict[str, str] = {}

    def _fake_subprocess(argv, *, cwd, env):
        captured_env.update(env)
        return 0, ""

    def _fake_ensure(*, runs_uri: str, manifest_id: str) -> str:
        ensure_seen["runs_uri"] = runs_uri
        ensure_seen["manifest_id"] = manifest_id
        return f"`proj-test.analytics._det_catchup_runs_{manifest_id}`"

    with (
        patch(
            "det.runtime.silver_catchup.read_catchup_manifest",
            return_value=manifest,
        ),
        patch(
            "det.runtime.silver_catchup.catchup_select_from_manifest",
            return_value=["silver_noaa__storm_events"],
        ),
        patch(
            "det.runtime.silver_catchup.catchup_manifest_file_path",
            return_value=_FakeRef(scm_uri),
        ),
        patch(
            "det.runtime.silver_catchup.catchup_runs_file_path",
            return_value=_FakeRef(runs_uri, text=runs_body),
        ),
        patch(
            "det.runtime.silver_catchup.ensure_bq_catchup_external_table",
            side_effect=_fake_ensure,
        ),
        patch(
            "det.runtime.dbt_runner.find_dbt_executable",
            return_value="dbt",
        ),
        patch(
            "det.runtime.dbt_runner._run_dbt_subprocess",
            side_effect=_fake_subprocess,
        ),
    ):
        result = run_dbt(
            project_root=tmp_path,
            catchup=True,
            catchup_manifest=mid,
            lake_path="gs://bucket",
            target="bigquery",
            dry_run=False,
        )

    vars_json = result.command[result.command.index("--vars") + 1]
    assert "det_catchup_by_pipeline" not in vars_json
    assert ensure_seen["runs_uri"] == runs_uri
    assert ensure_seen["manifest_id"] == mid
    assert captured_env.get("DET_CATCHUP_MANIFEST_PATH") == scm_uri
    assert (
        captured_env.get("DET_CATCHUP_BQ_RELATION")
        == f"`proj-test.analytics._det_catchup_runs_{mid}`"
    )


def test_run_dbt_catchup_bigquery_rejects_sidecar_digest_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("DET_LAKE_PATH", raising=False)
    monkeypatch.setenv("DET_GCP_PROJECT", "proj-test")
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text("name: x\n", encoding="utf-8")
    mid = "scm_" + ("11" * 8)
    from det.runtime.silver_catchup import _runs_jsonl_bytes, catchup_content_digest

    runs = [
        {
            "pipeline": "noaa.storm_events",
            "extract_run_datetime": "2026-08-06T12:00:00+00:00",
            "interval_start": "2026-08-06T00:00:00+00:00",
            "interval_end": "2026-08-07T00:00:00+00:00",
        }
    ]
    manifest = {
        "manifest_version": 1,
        "manifest_id": mid,
        "content_digest": catchup_content_digest(runs),
        "runs": runs,
    }
    other_runs = [
        {
            "pipeline": "noaa.storm_events",
            "extract_run_datetime": "2026-08-08T12:00:00+00:00",
            "interval_start": "2026-08-08T00:00:00+00:00",
            "interval_end": "2026-08-09T00:00:00+00:00",
        }
    ]
    scm_uri = f"gs://bucket/ops/silver_catchup/{mid}.json"
    runs_uri = f"gs://bucket/ops/silver_catchup/{mid}.runs.jsonl"

    class _FakeRef:
        def __init__(self, uri: str, *, text: str = "") -> None:
            self._uri = uri
            self._text = text

        def __str__(self) -> str:
            return self._uri

        def exists(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8") -> str:
            return self._text

    with (
        patch(
            "det.runtime.silver_catchup.read_catchup_manifest",
            return_value=manifest,
        ),
        patch(
            "det.runtime.silver_catchup.catchup_select_from_manifest",
            return_value=["silver_noaa__storm_events"],
        ),
        patch(
            "det.runtime.silver_catchup.catchup_manifest_file_path",
            return_value=_FakeRef(scm_uri),
        ),
        patch(
            "det.runtime.silver_catchup.catchup_runs_file_path",
            return_value=_FakeRef(
                runs_uri, text=_runs_jsonl_bytes(other_runs).decode("utf-8")
            ),
        ),
        pytest.raises(ValueError, match="does not match manifest content_digest"),
    ):
        run_dbt(
            project_root=tmp_path,
            catchup=True,
            catchup_manifest=mid,
            lake_path="gs://bucket",
            target="bigquery",
            dry_run=True,
        )
