#!/bin/sh
# Create the Iceberg catalog in Polaris, backed by the SeaweedFS bucket. Safe to re-run.
# The catalog allows DROP TABLE ... PURGE, and catalog_admin is granted the privilege it
# needs, so tests can delete their tables and data files. Neither is on by default.
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

api="$POLARIS/api/management/v1/catalogs"
properties="\"default-base-location\": \"s3://$S3_BUCKET/$POLARIS_CATALOG\", \"polaris.config.drop-with-purge.enabled\": \"true\""

status=$(curl -s -o /tmp/response -w '%{http_code}' "$api" \
  -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "catalog": {
    "name": "$POLARIS_CATALOG",
    "type": "INTERNAL",
    "properties": {$properties},
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

# Catalogs created before purge was enabled: turn it on. The update replaces the
# properties and must name the version it changes.
catalog=$(curl -sf "$api/$POLARIS_CATALOG" -H "Authorization: Bearer $token")
case "$catalog" in
  *'"polaris.config.drop-with-purge.enabled":"true"'*) ;;
  *)
    version=$(echo "$catalog" | sed -n 's/.*"entityVersion":\([0-9]*\).*/\1/p')
    curl -sf -o /dev/null -X PUT "$api/$POLARIS_CATALOG" \
      -H "Authorization: Bearer $token" \
      -H "Content-Type: application/json" \
      -d "{\"currentEntityVersion\": $version, \"properties\": {$properties}}"
    echo "Enabled drop with purge on '$POLARIS_CATALOG'"
    ;;
esac

# catalog_admin starts with only CATALOG_MANAGE_ACCESS and _METADATA; purging also needs _CONTENT.
# Granting a privilege the role already has succeeds, so this is safe to repeat.
curl -sf -o /dev/null -X PUT "$api/$POLARIS_CATALOG/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
echo "Granted CATALOG_MANAGE_CONTENT to catalog_admin"
