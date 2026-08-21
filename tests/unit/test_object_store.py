"""Unit tests for shared S3/MinIO env → fsspec / Iceberg property mapping."""

from __future__ import annotations

from det.runtime.object_store import (
    fsspec_s3_kwargs,
    iceberg_s3_properties,
    s3_endpoint_from_env,
)


def test_s3_env_empty():
    assert s3_endpoint_from_env({}) is None
    assert fsspec_s3_kwargs({}) == {}
    assert iceberg_s3_properties({}) == {}


def test_fsspec_s3_kwargs_minio():
    env = {
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
    }
    kwargs = fsspec_s3_kwargs(env)
    assert kwargs["endpoint_url"] == "http://127.0.0.1:9000"
    assert kwargs["key"] == "minioadmin"
    assert kwargs["secret"] == "minioadmin"
    assert kwargs["client_kwargs"]["region_name"] == "us-east-1"
    assert kwargs["config_kwargs"]["s3"]["addressing_style"] == "path"


def test_iceberg_s3_properties_minio():
    env = {
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_DEFAULT_REGION": "eu-west-1",
    }
    props = iceberg_s3_properties(env)
    assert props["s3.endpoint"] == "http://127.0.0.1:9000"
    assert props["s3.access-key-id"] == "minioadmin"
    assert props["s3.secret-access-key"] == "minioadmin"
    assert props["s3.region"] == "eu-west-1"
    assert props["s3.force-virtual-addressing"] == "false"
    assert props["s3.resolve-region"] == "false"


def test_iceberg_s3_defaults_region_when_endpoint_set():
    props = iceberg_s3_properties({"AWS_ENDPOINT_URL": "http://minio:9000"})
    assert props["s3.region"] == "us-east-1"
    assert "s3.access-key-id" not in props
