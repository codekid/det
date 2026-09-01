#!/bin/sh
# Create Polaris catalog for DET local/CI soaks.
# Adapted from Apache Polaris getting-started (Apache-2.0); POSIX sh, no jq/apk.
set -e

realm="${1:-POLARIS}"
TOKEN="${2:-}"
BASEDIR=$(dirname "$0")

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  # shellcheck source=obtain-token.sh
  . "$BASEDIR/obtain-token.sh"
fi

echo "Obtained access token (len=${#TOKEN})"

if [ -z "${STORAGE_LOCATION}" ]; then
  STORAGE_LOCATION="file:///var/tmp/quickstart_catalog/"
  STORAGE_TYPE="FILE"
else
  case "$STORAGE_LOCATION" in
    s3*) STORAGE_TYPE="S3" ;;
    gs*) STORAGE_TYPE="GCS" ;;
    *) STORAGE_TYPE="AZURE" ;;
  esac
fi

if [ -z "${STORAGE_CONFIG_INFO}" ]; then
  STORAGE_CONFIG_INFO="{\"storageType\": \"$STORAGE_TYPE\", \"allowedLocations\": [\"$STORAGE_LOCATION\"]}"
fi

CATALOG_NAME="${CATALOG_NAME:-det_lake}"
echo "Creating catalog ${CATALOG_NAME} in realm ${realm}..."

PAYLOAD=$(
  cat <<EOF
{
  "catalog": {
    "name": "${CATALOG_NAME}",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "${STORAGE_LOCATION}"
    },
    "storageConfigInfo": ${STORAGE_CONFIG_INFO}
  }
}
EOF
)

echo "$PAYLOAD"

HTTP_CODE=$(curl -sS -o /tmp/create-catalog.out -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: ${realm}" \
  http://polaris:8181/api/management/v1/catalogs \
  -d "$PAYLOAD")

cat /tmp/create-catalog.out
echo
echo "create-catalog HTTP ${HTTP_CODE}"

# 201 created, 409 already exists
case "$HTTP_CODE" in
  200|201|409) ;;
  *)
    echo "Failed to create catalog" >&2
    exit 1
    ;;
esac

echo Done.
