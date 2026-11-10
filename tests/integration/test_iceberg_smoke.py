"""Smoke test: create, write and read an Iceberg table through Polaris and SeaweedFS.

Needs the stack running (make up). Run with: make smoke
"""

import os
import uuid

import pyarrow as pa
import pytest

pytestmark = pytest.mark.integration


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


@pytest.fixture
def catalog():
    from pyiceberg.catalog import load_catalog

    client_id = env("POLARIS_CLIENT_ID", "root")
    client_secret = env("POLARIS_CLIENT_SECRET", "polaris-secret")
    return load_catalog(
        "polaris",
        type="rest",
        uri=f"http://localhost:{env('POLARIS_HOST_PORT', '8181')}/api/catalog",
        warehouse=env("POLARIS_CATALOG", "lakehouse"),
        credential=f"{client_id}:{client_secret}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "oauth2-server-uri": f"http://localhost:{env('POLARIS_HOST_PORT', '8181')}"
            "/api/catalog/v1/oauth/tokens",
            # Clients bring their own S3 keys; SeaweedFS has no STS for credential vending.
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": f"http://localhost:{env('S3_HOST_PORT', '8333')}",
            "s3.access-key-id": env("S3_ACCESS_KEY", "lakehouse"),
            "s3.secret-access-key": env("S3_SECRET_KEY", "lakehouse-secret"),
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def test_create_write_read_table(catalog):
    namespace = "smoke"
    table_name = f"{namespace}.positions_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace_if_not_exists(namespace)

    rows = pa.table(
        {
            "icao24": pa.array(["3c6444", "4ca7b4"], pa.string()),
            "latitude": pa.array([40.47, 41.29], pa.float64()),
            "longitude": pa.array([-3.56, 2.07], pa.float64()),
        }
    )
    table = catalog.create_table(table_name, schema=rows.schema)
    try:
        table.append(rows)
        result = catalog.load_table(table_name).scan().to_arrow().sort_by("icao24")
        assert result.equals(rows)
    finally:
        catalog.drop_table(table_name)
