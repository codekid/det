"""Unit tests for shared S3/MinIO and GCS env → fsspec / Iceberg property mapping."""

from __future__ import annotations

from det.runtime.object_store import (
    fsspec_gcs_kwargs,
    fsspec_s3_kwargs,
    gcs_emulator_host_from_env,
    iceberg_gcs_properties,
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


def test_gcs_emulator_host_adds_scheme():
    assert gcs_emulator_host_from_env({}) is None
    assert gcs_emulator_host_from_env({"STORAGE_EMULATOR_HOST": "localhost:4443"}) == (
        "http://localhost:4443"
    )
    assert gcs_emulator_host_from_env(
        {"STORAGE_EMULATOR_HOST": "https://127.0.0.1:4443"}
    ) == ("https://127.0.0.1:4443")


def test_fsspec_gcs_kwargs_emulator():
    kwargs = fsspec_gcs_kwargs(
        {
            "STORAGE_EMULATOR_HOST": "127.0.0.1:4443",
            "GOOGLE_CLOUD_PROJECT": "demo",
        }
    )
    assert kwargs["endpoint_url"] == "http://127.0.0.1:4443"
    assert kwargs["token"] == "anon"
    assert kwargs["project"] == "demo"


def test_fsspec_gcs_kwargs_adc_empty():
    assert fsspec_gcs_kwargs({}) == {}


def test_iceberg_gcs_properties_emulator():
    props = iceberg_gcs_properties({"STORAGE_EMULATOR_HOST": "localhost:4443"})
    assert props["gcs.service.host"] == "http://localhost:4443"
    assert props["gcs.project-id"] == "det-local"


def test_duckdb_s3_endpoint_parts_minio():
    from det.runtime.object_store import duckdb_s3_endpoint_parts

    host, use_ssl = duckdb_s3_endpoint_parts(
        {"AWS_ENDPOINT_URL": "http://127.0.0.1:9000"}
    )
    assert host == "127.0.0.1:9000"
    assert use_ssl is False


def test_duckdb_s3_endpoint_parts_https():
    from det.runtime.object_store import duckdb_s3_endpoint_parts

    host, use_ssl = duckdb_s3_endpoint_parts(
        {"AWS_ENDPOINT_URL": "https://s3.example.com"}
    )
    assert host == "s3.example.com"
    assert use_ssl is True


def test_duckdb_s3_secret_params_minio():
    from det.runtime.object_store import duckdb_s3_secret_params

    params = duckdb_s3_secret_params(
        {
            "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_REGION": "us-east-1",
        }
    )
    assert params["key_id"] == "minioadmin"
    assert params["endpoint"] == "127.0.0.1:9000"
    assert params["url_style"] == "path"
    assert params["use_ssl"] is False


def test_duckdb_s3_credentials_required_raises():
    import pytest

    from det.runtime.object_store import duckdb_s3_credentials_required

    with pytest.raises(ValueError, match="AWS_ACCESS_KEY_ID"):
        duckdb_s3_credentials_required({})


def test_duckdb_s3_profile_env_exports_endpoint():
    from det.runtime.object_store import duckdb_s3_profile_env

    env = duckdb_s3_profile_env(
        {
            "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
            "AWS_ACCESS_KEY_ID": "k",
            "AWS_SECRET_ACCESS_KEY": "s",
        }
    )
    assert env["DET_DUCKDB_S3_ENDPOINT"] == "127.0.0.1:9000"
    assert env["DET_DUCKDB_S3_USE_SSL"] == "false"
