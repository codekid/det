#!/bin/sh
# Obtain Polaris OAuth token. Adapted from Apache Polaris getting-started (Apache-2.0).
# Pure POSIX + curl (no jq/apk) for alpine/curl on read-only mounts.
set -e

realm="${1:-POLARIS}"

RESP=$(curl -sS http://polaris:8181/api/catalog/v1/oauth/tokens \
  --user "${CLIENT_ID}:${CLIENT_SECRET}" \
  -H "Polaris-Realm: ${realm}" \
  -d grant_type=client_credentials \
  -d scope=PRINCIPAL_ROLE:ALL)

# Extract access_token without jq (alpine/curl has no jq by default).
TOKEN=$(printf '%s' "$RESP" | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

if [ -z "${TOKEN}" ] || [ "${TOKEN}" = "null" ]; then
  echo "Failed to obtain access token: ${RESP}" >&2
  exit 1
fi

export TOKEN
