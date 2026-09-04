"""Coverage for kernel helpers introduced in P0.1 boundary edges."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
import yaml

from det.runtime.bronze_runs import list_bronze_runs, run_dict, walk_hive_runs
from det.runtime.limits import DEFAULT_LIST_LIMIT, clamp_list_limit
from det.runtime.schema_shapes import (
    allowed_types,
    is_array_prop,
    is_object_prop,
    is_scalar_prop,
)
from det.runtime.warehouse_paths import analytics_duckdb_path, ops_duckdb_path
from det.scaffold.check_dbt import (
    check_pipeline_config_with_dbt,
    check_project_with_dbt,
    scaffold_sql_stale_findings,
)


def test_schema_shapes_classifiers() -> None:
    assert allowed_types({"type": "string"}) == {"string"}
    assert allowed_types({"type": ["string", "null"]}) == {"string", "null"}
    assert allowed_types({"type": ["string", 1]}) == {"string"}
    assert allowed_types({}) == set()
    assert allowed_types({"type": 1}) == set()

    assert is_object_prop({"type": "object", "properties": {"a": {"type": "string"}}})
    assert is_object_prop({"properties": {"a": {"type": "integer"}}})
    assert not is_object_prop({"type": "array", "items": {"type": "string"}})

    assert is_array_prop({"type": "array", "items": {"type": "string"}})
    assert is_array_prop({"items": {"type": "integer"}})
    assert not is_array_prop({"type": "string"})
    assert not is_array_prop({"type": "object", "items": {"type": "string"}})

    assert is_scalar_prop({"type": "string"})
    assert is_scalar_prop({"type": ["number", "null"]})
    assert is_scalar_prop({})
    assert not is_scalar_prop({"type": "object", "properties": {}})
    assert not is_scalar_prop({"type": "array", "items": {}})
    assert not is_scalar_prop({"type": "object"})
    assert not is_scalar_prop({"properties": {}})
    assert not is_scalar_prop({"type": "foo"})


def test_clamp_list_limit_and_warehouse_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert clamp_list_limit(None) == DEFAULT_LIST_LIMIT
    assert clamp_list_limit(0) == 1
    assert clamp_list_limit(9999) == DEFAULT_LIST_LIMIT
    assert clamp_list_limit(50) == 50

    # CI exports DET_*_DUCKDB; clear so default-under-root behavior is testable.
    monkeypatch.delenv("DET_ANALYTICS_DUCKDB", raising=False)
    monkeypatch.delenv("DET_OPS_DUCKDB", raising=False)
    assert analytics_duckdb_path(tmp_path) == (tmp_path / "data" / "analytics.duckdb").resolve()
    assert ops_duckdb_path(tmp_path) == (tmp_path / "data" / "det_ops.duckdb").resolve()

    monkeypatch.setenv("DET_ANALYTICS_DUCKDB", str(tmp_path / "a.duckdb"))
    monkeypatch.setenv("DET_OPS_DUCKDB", str(tmp_path / "o.duckdb"))
    assert analytics_duckdb_path(tmp_path) == (tmp_path / "a.duckdb").resolve()
    assert ops_duckdb_path(tmp_path) == (tmp_path / "o.duckdb").resolve()


def test_walk_hive_runs_and_filesystem_list_bronze(tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config
    from det.runtime.manifest import write_manifest

    root = tmp_path
    ds = (
        root
        / "bronze"
        / "acme"
        / "widgets_v1"
        / "__interval_start_datetime=20260806T000000Z"
        / "__interval_end_datetime=20260807T000000Z"
        / "__extract_run_datetime=20260806T120000Z"
    )
    ds.mkdir(parents=True)
    (ds / "data.jsonl").write_text("{}\n", encoding="utf-8")
    write_manifest(ds, {"extract_run_datetime": "2026-08-06T12:00:00+00:00"})

    runs = walk_hive_runs(
        root / "bronze" / "acme" / "widgets_v1",
        root=root,
        limit=10,
        normalize_iso=True,
    )
    assert len(runs) == 1
    assert runs[0]["extract_run_datetime"].startswith("2026-08-06")
    assert run_dict(
        interval_start="a",
        interval_end="b",
        extract_run_datetime="c",
        path="p",
    ) == {
        "interval_start": "a",
        "interval_end": "b",
        "extract_run_datetime": "c",
        "path": "p",
    }

    pipe = root / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = root / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: string}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source:",
                "  type: acme.widgets",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination:",
                "  type: filesystem",
                "  path: .",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config = load_pipeline_config(pipe)
    listed, note = list_bronze_runs(config, root=root, limit=10)
    assert note is None
    assert len(listed) == 1

    # Incomplete JSONL without commit must not surface for catch-up listing.
    orphan = (
        root
        / "bronze"
        / "acme"
        / "widgets_v1"
        / "__interval_start_datetime=20260807T000000Z"
        / "__interval_end_datetime=20260808T000000Z"
        / "__extract_run_datetime=20260807T120000Z"
    )
    orphan.mkdir(parents=True)
    (orphan / "data.jsonl").write_text("{}\n", encoding="utf-8")
    listed2, _ = list_bronze_runs(config, root=root, limit=10)
    assert len(listed2) == 1


def test_list_bronze_runs_duckdb(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    from det.runtime.config import load_pipeline_config

    root = tmp_path
    pipe = root / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = root / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    db_path = root / "bronze.duckdb"
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source:",
                "  type: acme.widgets",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination:",
                "  type: duckdb",
                f"  connection: {db_path.name}",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    con = duckdb.connect(str(db_path))
    con.execute("create schema bronze_acme")
    con.execute(
        """
        create table bronze_acme.widgets_v1 (
            id integer,
            __interval_start_datetime timestamp,
            __interval_end_datetime timestamp,
            __extract_run_datetime timestamp
        )
        """
    )
    con.execute(
        """
        insert into bronze_acme.widgets_v1 values
          (1, '2026-08-06 00:00:00.123456', '2026-08-07 00:00:00.999000',
           '2026-08-06 12:00:00.654321'),
          (2, '2026-08-06 00:00:00.123456', '2026-08-07 00:00:00.999000',
           '2026-08-06 12:00:00.654321'),
          (3, '2026-08-07', '2026-08-08', '2026-08-07 12:00:00')
        """
    )
    con.close()

    config = load_pipeline_config(pipe)
    listed, note = list_bronze_runs(config, root=root, limit=10)
    assert note is None
    assert len(listed) == 2
    assert listed[0]["interval_start"] == "2026-08-06T00:00:00+00:00"
    assert listed[0]["interval_end"] == "2026-08-07T00:00:00+00:00"
    assert listed[0]["extract_run_datetime"] == "2026-08-06T12:00:00+00:00"
    assert "." not in listed[0]["extract_run_datetime"].split("+")[0]
    windowed, _ = list_bronze_runs(
        config,
        root=root,
        limit=10,
        interval_start="2026-08-07",
        interval_end="2026-08-08",
    )
    assert len(windowed) == 1

    missing_db = list_bronze_runs(
        config.model_copy(
            update={
                "destination": config.destination.model_copy(
                    update={"connection": "nope.duckdb"}
                )
            }
        ),
        root=root,
        limit=5,
    )
    assert missing_db[0] == []
    assert missing_db[1] and "not found" in missing_db[1]


def _write_acme_plugin(root: Path, name: str = "acme.widgets") -> None:
    provider, source = name.split(".", 1)
    path = root / "sources" / provider / f"{source}.py"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "from det.sources.base import SourceRow",
                "",
                "class AcmeWidgetsSource:",
                f"    name = {name!r}",
                "    def defaults(self):",
                "        return {}",
                "    def extract_to_raw(self, raw_dir, interval, config):",
                "        return []",
                "    def records_from_raw(self, raw_dir, config):",
                "        yield SourceRow(payload={'id': 1}, filename='x.json')",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_list_bronze_runs_duckdb_table_missing(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    from det.runtime.config import load_pipeline_config

    root = tmp_path
    pipe = root / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = root / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    db_path = root / "empty.duckdb"
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source: {type: acme.widgets}",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination:",
                "  type: duckdb",
                f"  connection: {db_path.name}",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    duckdb.connect(str(db_path)).close()
    config = load_pipeline_config(pipe)
    runs, note = list_bronze_runs(config, root=root, limit=5)
    assert runs == []
    assert note and "table not found" in note


def test_list_bronze_runs_iceberg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config

    root = tmp_path
    pipe = root / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = root / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source: {type: acme.widgets}",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination:",
                "  type: iceberg",
                "  path: .",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config = load_pipeline_config(pipe)

    monkeypatch.setattr(
        "det.ingestion.iceberg_writer.load_iceberg_table",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "det.ingestion.iceberg_writer.list_iceberg_extract_runs",
        lambda *_a, **_k: [
            ("2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00", "2026-08-06T12:00:00+00:00")
        ],
    )
    runs, note = list_bronze_runs(config, root=root, limit=10)
    assert note is None
    assert len(runs) == 1

    monkeypatch.setattr(
        "det.ingestion.iceberg_writer.load_iceberg_table",
        lambda **_kwargs: None,
    )
    runs2, note2 = list_bronze_runs(config, root=root, limit=10)
    assert runs2 == []
    assert note2 and "not found" in note2

    def _boom(**_kwargs):
        raise ImportError("no pyiceberg")

    monkeypatch.setattr("det.ingestion.iceberg_writer.load_iceberg_table", _boom)
    runs3, note3 = list_bronze_runs(config, root=root, limit=10)
    assert runs3 == []
    assert note3 and "pyiceberg" in note3


def test_walk_hive_runs_filters_and_committed(tmp_path: Path) -> None:
    from det.runtime.meta import to_partition_value

    root = tmp_path
    base = root / "raw" / "acme" / "widgets_v1"
    start = to_partition_value("2026-08-06T00:00:00+00:00")
    end = to_partition_value("2026-08-07T00:00:00+00:00")
    run = to_partition_value("2026-08-06T12:00:00+00:00")
    committed = (
        base
        / f"__interval_start_datetime={start}"
        / f"__interval_end_datetime={end}"
        / f"__extract_run_datetime={run}"
    )
    committed.mkdir(parents=True)
    (committed / "meta").mkdir()
    (committed / "meta" / "manifest.json").write_text("{}", encoding="utf-8")
    orphan = (
        base
        / f"__interval_start_datetime={start}"
        / f"__interval_end_datetime={end}"
        / f"__extract_run_datetime={to_partition_value('2026-08-06T13:00:00+00:00')}"
    )
    orphan.mkdir(parents=True)
    (base / "not-a-hive").mkdir()

    all_runs = walk_hive_runs(base, root=root, limit=10, require_committed=False)
    assert len(all_runs) == 2
    committed_only = walk_hive_runs(base, root=root, limit=10, require_committed=True)
    assert len(committed_only) == 1
    windowed = walk_hive_runs(
        base,
        root=root,
        limit=10,
        interval_start="2026-08-08",
        interval_end="2026-08-09",
    )
    assert windowed == []
    # Window filter must use ISO bounds even when callers ask for compact values.
    in_window = walk_hive_runs(
        base,
        root=root,
        limit=10,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
        normalize_iso=False,
    )
    assert len(in_window) == 2
    assert in_window[0]["interval_start"] == start
    assert walk_hive_runs(root / "missing", root=root, limit=5) == []

    # Skip non-dirs / malformed hive keys; honor limit and raw (non-ISO) values.
    start_hive = base / f"__interval_start_datetime={start}"
    (start_hive / "not-an-end-dir").write_text("x", encoding="utf-8")
    (start_hive / "random-dir").mkdir(exist_ok=True)
    end_hive = start_hive / f"__interval_end_datetime={end}"
    (end_hive / "not-a-run-dir").write_text("x", encoding="utf-8")
    (end_hive / "random-run").mkdir(exist_ok=True)
    (base / "file-at-dataset").write_text("x", encoding="utf-8")
    limited = walk_hive_runs(base, root=root, limit=1, normalize_iso=False)
    assert len(limited) == 1
    assert "T" in limited[0]["interval_start"]  # compact hive form


def test_list_bronze_sql_runs_unsupported_filesystem(tmp_path: Path) -> None:
    from det.runtime.bronze_runs import _list_bronze_sql_runs
    from det.runtime.config import load_pipeline_config

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: string}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source: {type: acme.widgets}",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination: {type: filesystem, path: .}",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config = load_pipeline_config(pipe)
    runs, note = _list_bronze_sql_runs(config, root=tmp_path, limit=5)
    assert runs == []
    assert note and "unsupported" in note


class _FakePgCursor:
    def __init__(self, *, table_exists: bool = True, rows: list[tuple] | None = None):
        self._table_exists = table_exists
        self._rows = rows or []
        self._mode = "exists"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        if "information_schema.tables" in sql:
            self._mode = "exists"
            return
        self._mode = "rows"

    def fetchone(self):
        if self._mode == "exists":
            return (1,) if self._table_exists else (0,)
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakePgConn:
    def __init__(self, cursor: _FakePgCursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self._cursor


def _install_list_runs_psycopg(
    monkeypatch: pytest.MonkeyPatch,
    *,
    table_exists: bool = True,
    rows: list[tuple] | None = None,
    missing_import: bool = False,
) -> None:
    if missing_import:
        monkeypatch.delitem(sys.modules, "psycopg", raising=False)

        class _BlockPsycopg:
            def find_spec(self, fullname, path=None, target=None):  # noqa: ARG002
                if fullname == "psycopg" or fullname.startswith("psycopg."):
                    raise ImportError("no psycopg")
                return None

        monkeypatch.setattr(sys, "meta_path", [_BlockPsycopg(), *sys.meta_path])
        return

    module = types.ModuleType("psycopg")
    cursor = _FakePgCursor(table_exists=table_exists, rows=rows)

    def connect(dsn, **_kwargs):
        return _FakePgConn(cursor)

    module.connect = connect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg", module)


def _write_postgres_pipeline(root: Path) -> Path:
    pipe = root / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True, exist_ok=True)
    schema = root / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True, exist_ok=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source: {type: acme.widgets}",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination:",
                "  type: postgres",
                "  connection_env: DET_POSTGRES_DSN",
                "  dataset: bronze",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return pipe


def test_list_bronze_runs_postgres(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config

    monkeypatch.setenv("DET_POSTGRES_DSN", "postgresql://det:secret@db/det")
    pipe = _write_postgres_pipeline(tmp_path)
    config = load_pipeline_config(pipe)

    rows = [
        ("2026-08-06T00:00:00+00:00", "2026-08-07T00:00:00+00:00", "2026-08-06T12:00:00+00:00"),
        ("2026-08-07T00:00:00+00:00", "2026-08-08T00:00:00+00:00", "2026-08-07T12:00:00+00:00"),
    ]
    _install_list_runs_psycopg(monkeypatch, rows=rows)
    listed, note = list_bronze_runs(config, root=tmp_path, limit=10)
    assert note is None
    assert len(listed) == 2

    windowed, _ = list_bronze_runs(
        config,
        root=tmp_path,
        limit=10,
        interval_start="2026-08-07",
        interval_end="2026-08-08",
    )
    assert len(windowed) == 2  # fake cursor ignores SQL filters; still exercises window branch

    _install_list_runs_psycopg(monkeypatch, table_exists=False)
    empty, missing_note = list_bronze_runs(config, root=tmp_path, limit=5)
    assert empty == []
    assert missing_note and "table not found" in missing_note

    monkeypatch.delenv("DET_POSTGRES_DSN", raising=False)
    from det.runtime.secrets import clear_secret_cache

    clear_secret_cache()
    _install_list_runs_psycopg(monkeypatch, rows=rows)
    unset, secret_note = list_bronze_runs(config, root=tmp_path, limit=5)
    assert unset == []
    assert secret_note

    _install_list_runs_psycopg(monkeypatch, missing_import=True)
    monkeypatch.setenv("DET_POSTGRES_DSN", "postgresql://det:secret@db/det")
    no_pg, import_note = list_bronze_runs(config, root=tmp_path, limit=5)
    assert no_pg == []
    assert import_note and "postgres" in import_note.lower()


def test_list_bronze_runs_iceberg_window(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config

    root = tmp_path
    pipe = root / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = root / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source: {type: acme.widgets}",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination: {type: iceberg, path: .}",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config = load_pipeline_config(pipe)
    monkeypatch.setattr(
        "det.ingestion.iceberg_writer.load_iceberg_table",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        "det.ingestion.iceberg_writer.list_iceberg_extract_runs",
        lambda *_a, **_k: [],
    )
    runs, note = list_bronze_runs(
        config,
        root=root,
        limit=10,
        interval_start="2026-08-06",
        interval_end="2026-08-07",
    )
    assert note is None
    assert runs == []


def test_scaffold_sql_stale_render_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config
    from det.scaffold import check_dbt as check_dbt_mod
    from det.scaffold.dbt import scaffold_dbt

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    doc = {
        "name": "acme.widgets",
        "source": {"type": "acme.widgets"},
        "schema": "schemas/acme/widgets/widgets.schema.yaml",
        "destination": {"type": "filesystem", "path": "."},
        "wire_version": 1,
        "dbt": {
            "silver": {
                "materialized": "table",
                "unique_key": ["id"],
                "order_by": ["id asc"],
            }
        },
    }
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    config = load_pipeline_config(pipe)
    scaffold_dbt(config, project_root=tmp_path, force=True)

    def _boom(*_a, **_k):
        raise RuntimeError("render failed")

    monkeypatch.setattr(check_dbt_mod, "expected_silver_sql", _boom)
    findings = check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    assert any(f.code == "scaffold_sql_stale" and "re-render" in f.detail for f in findings)


def test_check_project_with_dbt_without_scaffold(tmp_path: Path) -> None:
    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: string}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source:",
                "  type: acme.widgets",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination:",
                "  type: filesystem",
                "  path: .",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    findings = check_project_with_dbt(tmp_path, pipeline="acme.widgets")
    assert not any(f.code == "scaffold_sql_stale" for f in findings)


def test_check_project_with_dbt_detects_stale_silver(tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    doc = {
        "name": "acme.widgets",
        "source": {"type": "acme.widgets"},
        "schema": "schemas/acme/widgets/widgets.schema.yaml",
        "destination": {"type": "filesystem", "path": "."},
        "wire_version": 1,
        "dbt": {
            "silver": {
                "materialized": "incremental",
                "unique_key": ["id"],
                "order_by": ["__extract_run_datetime desc"],
                "watermark": "__extract_run_datetime",
                "lookback": "3 days",
            }
        },
    }
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    config = load_pipeline_config(pipe)
    scaffold_dbt(config, project_root=tmp_path, force=True)
    assert not any(
        f.code == "scaffold_sql_stale"
        for f in check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    )
    doc["dbt"]["silver"]["lookback"] = "9 days"
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    findings = check_project_with_dbt(tmp_path, pipeline="acme.widgets")
    assert any(f.code == "scaffold_sql_stale" for f in findings)
    findings_all = check_project_with_dbt(tmp_path)
    assert any(f.code == "scaffold_sql_stale" for f in findings_all)


def test_scaffold_sql_stale_missing_file_and_normalize(tmp_path: Path) -> None:
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    doc = {
        "name": "acme.widgets",
        "source": {"type": "acme.widgets"},
        "schema": "schemas/acme/widgets/widgets.schema.yaml",
        "destination": {"type": "filesystem", "path": "."},
        "wire_version": 1,
        "dbt": {
            "silver": {
                "materialized": "table",
                "unique_key": ["id"],
                "order_by": ["id asc"],
            }
        },
    }
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    config = load_pipeline_config(pipe)
    scaffold_dbt(config, project_root=tmp_path, force=True)
    silver = tmp_path / "dbt" / "models" / "silver" / "silver_acme__widgets.sql"
    assert silver.is_file()
    # CRLF + missing trailing newline normalization should still match.
    body = silver.read_text(encoding="utf-8").rstrip("\n").replace("\n", "\r\n") + "\r\n"
    silver.write_text(body, encoding="utf-8")
    assert not any(
        f.code == "scaffold_sql_stale"
        for f in check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    )
    silver.unlink()
    findings = scaffold_sql_stale_findings(
        config, project_root=tmp_path, pipeline_id="acme.widgets"
    )
    assert any(f.code == "scaffold_sql_stale" and "missing" in f.detail for f in findings)


def test_scaffold_sql_stale_unreadable_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    doc = {
        "name": "acme.widgets",
        "source": {"type": "acme.widgets"},
        "schema": "schemas/acme/widgets/widgets.schema.yaml",
        "destination": {"type": "filesystem", "path": "."},
        "wire_version": 1,
        "dbt": {
            "silver": {
                "materialized": "table",
                "unique_key": ["id"],
                "order_by": ["id asc"],
            }
        },
    }
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    config = load_pipeline_config(pipe)
    scaffold_dbt(config, project_root=tmp_path, force=True)

    real_read_text = Path.read_text

    def _boom(self, *args, **kwargs):
        if self.name.endswith(".sql") and "silver_acme__widgets" in self.name:
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _boom)
    findings = check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    assert any(
        f.code == "scaffold_sql_stale" and "could not read" in f.detail for f in findings
    )


def test_scaffold_sql_stale_despite_missing_dbt_models(tmp_path: Path) -> None:
    """Silver drift still reported when only stg is missing (missing_dbt_models)."""
    from det.runtime.config import load_pipeline_config
    from det.scaffold.dbt import scaffold_dbt

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: integer}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    doc = {
        "name": "acme.widgets",
        "source": {"type": "acme.widgets"},
        "schema": "schemas/acme/widgets/widgets.schema.yaml",
        "destination": {"type": "filesystem", "path": "."},
        "wire_version": 1,
        "dbt": {
            "silver": {
                "materialized": "incremental",
                "unique_key": ["id"],
                "order_by": ["__extract_run_datetime desc"],
                "watermark": "__extract_run_datetime",
                "lookback": "3 days",
            }
        },
    }
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    config = load_pipeline_config(pipe)
    scaffold_dbt(config, project_root=tmp_path, force=True)
    stg = tmp_path / "dbt" / "models" / "silver" / "stg_acme__widgets.sql"
    assert stg.is_file()
    stg.unlink()
    doc["dbt"]["silver"]["lookback"] = "9 days"
    pipe.write_text(yaml.safe_dump(doc), encoding="utf-8")
    findings = check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    assert any(f.code == "missing_dbt_models" for f in findings)
    assert any(f.code == "scaffold_sql_stale" for f in findings)


def test_check_pipeline_config_with_dbt_early_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from det.scaffold import check_dbt as check_dbt_mod

    pipe = tmp_path / "configs" / "pipelines" / "acme" / "widgets.yaml"
    pipe.parent.mkdir(parents=True)
    schema = tmp_path / "schemas" / "acme" / "widgets" / "widgets.schema.yaml"
    schema.parent.mkdir(parents=True)
    schema.write_text(
        "type: object\nproperties:\n  id: {type: string}\nadditionalProperties: false\n",
        encoding="utf-8",
    )
    _write_acme_plugin(tmp_path)
    pipe.write_text(
        "\n".join(
            [
                "name: acme.widgets",
                "source: {type: acme.widgets}",
                "schema: schemas/acme/widgets/widgets.schema.yaml",
                "destination: {type: filesystem, path: .}",
                "wire_version: 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    # No dbt/ dir → skip scaffold drift.
    findings = check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    assert not any(f.code == "scaffold_sql_stale" for f in findings)

    (tmp_path / "dbt").mkdir()
    # dbt exists but silver SQL missing → skip.
    findings2 = check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    assert not any(f.code == "scaffold_sql_stale" for f in findings2)

    # Absolute path outside root exercises _rel ValueError branch via render error path.
    outside = Path("/tmp")
    findings3 = check_dbt_mod.scaffold_sql_stale_findings(
        object(), project_root=outside, pipeline_id="x"
    )
    assert findings3  # expected_silver_sql will fail on bogus config

    def _boom(*_a, **_k):
        raise RuntimeError("bad config")

    monkeypatch.setattr(check_dbt_mod, "load_pipeline_config", _boom)
    (tmp_path / "dbt" / "models" / "silver").mkdir(parents=True)
    (
        tmp_path / "dbt" / "models" / "silver" / "silver_acme__widgets.sql"
    ).write_text("select 1\n", encoding="utf-8")
    findings4 = check_pipeline_config_with_dbt(pipe, project_root=tmp_path)
    assert not any(f.code == "scaffold_sql_stale" for f in findings4)


def test_check_project_with_dbt_skips_unloadable(tmp_path: Path) -> None:
    pipe = tmp_path / "configs" / "pipelines" / "acme" / "broken.yaml"
    pipe.parent.mkdir(parents=True)
    pipe.write_text("not: valid: yaml: [[[\n", encoding="utf-8")
    (tmp_path / "dbt" / "models" / "silver").mkdir(parents=True)
    findings = check_project_with_dbt(tmp_path)
    assert isinstance(findings, list)
