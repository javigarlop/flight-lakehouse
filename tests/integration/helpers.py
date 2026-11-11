"""Connections from the host to the running stack, shared by the integration tests."""

import os


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def host_catalog():
    """PyIceberg catalog for Polaris, reaching SeaweedFS on the host port."""
    from pyiceberg.catalog import load_catalog

    polaris = f"http://localhost:{env('POLARIS_HOST_PORT', '8181')}/api/catalog"
    client_id = env("POLARIS_CLIENT_ID", "root")
    client_secret = env("POLARIS_CLIENT_SECRET", "polaris-secret")
    return load_catalog(
        "polaris",
        type="rest",
        uri=polaris,
        warehouse=env("POLARIS_CATALOG", "lakehouse"),
        credential=f"{client_id}:{client_secret}",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "oauth2-server-uri": f"{polaris}/v1/oauth/tokens",
            # Clients bring their own S3 keys; SeaweedFS has no STS for credential vending.
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": f"http://localhost:{env('S3_HOST_PORT', '8333')}",
            "s3.access-key-id": env("S3_ACCESS_KEY", "lakehouse"),
            "s3.secret-access-key": env("S3_SECRET_KEY", "lakehouse-secret"),
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
