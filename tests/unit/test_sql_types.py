from __future__ import annotations

from det.runtime.sql_types import (
    bronze_iceberg_columns,
    bronze_sql_columns,
    duckdb_type_for_prop,
    iceberg_type_for_prop,
    sql_type_for_prop,
    types_compatible,
)


def test_sql_type_for_prop_both_dialects():
    assert duckdb_type_for_prop({"type": "integer"}) == "INTEGER"
    assert sql_type_for_prop({"type": ["integer", "null"]}, "postgres") == "INTEGER"
    assert sql_type_for_prop({"type": "number"}, "duckdb") == "DOUBLE"
    assert sql_type_for_prop({"type": "number"}, "postgres") == "DOUBLE PRECISION"
    assert sql_type_for_prop({"type": "boolean"}, "duckdb") == "BOOLEAN"
    assert sql_type_for_prop({"type": "string"}, "duckdb") == "VARCHAR"
    assert sql_type_for_prop({"type": "string"}, "postgres") == "TEXT"
    obj = {"type": "object", "properties": {"a": {"type": "string"}}}
    arr = {"type": "array", "items": {"type": "string"}}
    assert sql_type_for_prop(obj, "duckdb") == "JSON"
    assert sql_type_for_prop(obj, "postgres") == "JSONB"
    assert sql_type_for_prop(arr, "duckdb") == "JSON"
    assert sql_type_for_prop(arr, "postgres") == "JSONB"


def test_bronze_sql_columns_schema_then_meta():
    schema = {
        "type": "object",
        "properties": {
            "event_id": {"type": "integer"},
            "state": {"type": ["string", "null"]},
        },
    }
    cols = bronze_sql_columns(schema, "duckdb")
    names = [n for n, _ in cols]
    assert names[:2] == ["event_id", "state"]
    assert names[-7:] == [
        "__row_hash",
        "__filename",
        "__extract_run_datetime",
        "__bronze_loaded_at",
        "__interval_start_datetime",
        "__interval_end_datetime",
        "__data_interval_date",
    ]
    by_name = dict(cols)
    assert by_name["event_id"] == "INTEGER"
    assert by_name["state"] == "VARCHAR"
    assert by_name["__extract_run_datetime"] == "TIMESTAMP"
    assert by_name["__data_interval_date"] == "DATE"
    pg = dict(bronze_sql_columns(schema, "postgres"))
    assert pg["state"] == "TEXT"
    assert pg["__extract_run_datetime"] == "TIMESTAMPTZ"


def test_iceberg_type_for_prop_and_columns():
    assert iceberg_type_for_prop({"type": "integer"}) == "INTEGER"
    assert iceberg_type_for_prop({"type": ["integer", "null"]}) == "INTEGER"
    assert iceberg_type_for_prop({"type": "number"}) == "DOUBLE"
    assert iceberg_type_for_prop({"type": "boolean"}) == "BOOLEAN"
    assert iceberg_type_for_prop({"type": "string"}) == "STRING"
    obj = {"type": "object", "properties": {"a": {"type": "string"}}}
    arr = {"type": "array", "items": {"type": "string"}}
    assert iceberg_type_for_prop(obj) == "STRING"
    assert iceberg_type_for_prop(arr) == "STRING"
    schema = {
        "type": "object",
        "properties": {
            "event_id": {"type": "integer"},
            "payload": {"type": "object", "properties": {"k": {"type": "string"}}},
        },
    }
    cols = dict(bronze_iceberg_columns(schema))
    assert cols["event_id"] == "INTEGER"
    assert cols["payload"] == "STRING"
    assert cols["__extract_run_datetime"] == "TIMESTAMPTZ"
    assert cols["__data_interval_date"] == "DATE"
    assert cols["__row_hash"] == "STRING"


def test_types_compatible_aliases_not_varchar_timestamp():
    assert types_compatible("BIGINT", "INTEGER")
    assert types_compatible("integer", "INT")
    assert types_compatible("DOUBLE PRECISION", "DOUBLE")
    assert types_compatible("VARCHAR", "TEXT")
    assert types_compatible("JSON", "JSONB")
    assert types_compatible("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
    assert types_compatible("STRING", "VARCHAR")
    assert types_compatible("INT32", "INTEGER")
    assert types_compatible("TIMESTAMPNTZ", "TIMESTAMP")
    assert not types_compatible("VARCHAR", "TIMESTAMP")
    assert not types_compatible("TEXT", "JSONB")
    assert not types_compatible("VARCHAR", "INTEGER")
