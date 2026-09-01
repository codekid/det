# Local Polaris + MinIO for DET Iceberg REST soaks.
#
# Starts MinIO (API :9000, console :9001) and Polaris REST (:8181).
# Catalog warehouse name: det_lake. Bucket: det-ci.
#
#   make polaris-up
#   make polaris-down
#
# Env for DET (also exported by make polaris-env):
#   DET_LAKE_MODE=cloud
#   DET_LAKE_PATH=s3://det-ci/det-lake
#   AWS_ENDPOINT_URL=http://127.0.0.1:9000
#   AWS_ACCESS_KEY_ID=minioadmin
#   AWS_SECRET_ACCESS_KEY=minioadmin
#   AWS_REGION=us-east-1
#   DET_ICEBERG_CATALOG=rest
#   DET_ICEBERG_REST_URI=http://127.0.0.1:8181/api/catalog
#   DET_ICEBERG_REST_WAREHOUSE=det_lake
#   DET_ICEBERG_REST_CREDENTIAL=root:s3cr3t
#
# Bootstrap scripts under scripts/ are adapted from Apache Polaris
# getting-started (Apache License 2.0).
