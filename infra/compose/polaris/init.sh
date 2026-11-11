#!/bin/sh
# Create the Iceberg catalog in Polaris, backed by the SeaweedFS bucket. Safe to re-run.
# Only endpointInternal (Polaris's own access) is set: with a client-facing "endpoint",
# Polaris would push one S3 address to every client, but Spark in Docker and tools on
# the host reach SeaweedFS at different addresses, so each client configures its own.
set -eu

POLARIS=http://polaris:8181

token=$(curl -sf "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id="$POLARIS_CLIENT_ID" \
  -d client_secret="$POLARIS_CLIENT_SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "$token" ]; then
  echo "Could not get a Polaris token" >&2
  exit 1
fi

status=$(curl -s -o /tmp/response -w '%{http_code}' "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "catalog": {
    "name": "$POLARIS_CATALOG",
    "type": "INTERNAL",
    "properties": {"default-base-location": "s3://$S3_BUCKET/$POLARIS_CATALOG"},
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://$S3_BUCKET/"],
      "endpointInternal": "http://seaweedfs:8333",
      "pathStyleAccess": true,
      "stsUnavailable": true
    }
  }
}
EOF
)

case "$status" in
  201) echo "Created catalog '$POLARIS_CATALOG'" ;;
  409) echo "Catalog '$POLARIS_CATALOG' already exists" ;;
  *) echo "Catalog creation failed with HTTP $status:" >&2; cat /tmp/response >&2; exit 1 ;;
esac
