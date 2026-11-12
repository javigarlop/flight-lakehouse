"""Connections from the host to the running stack, shared by the integration tests."""

import contextlib
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "infra/compose/docker-compose.yml"),
    "--env-file",
    str(ROOT / ".env"),
    "--profile",
    "stream",
]


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


def spark_submit(job: str, *args: str) -> list[str]:
    """Command that runs a streaming job (bronze or silver) in the Spark container."""
    return [
        "/opt/spark/bin/spark-submit",
        "--master=local[2]",
        "--driver-memory=1g",
        "--conf=spark.ui.showConsoleProgress=false",
        f"src/flight_lakehouse/streaming/{job}.py",
        *args,
    ]


def table_rows(catalog, table: str):
    """The table's rows as Arrow, or None if it doesn't exist yet."""
    try:
        return catalog.load_table(table).scan().to_arrow()
    except Exception:  # noqa: BLE001 - not created yet
        return None


def purge_tables(catalog, *tables: str) -> None:
    """Drop test tables and delete their files. A table the test never created is skipped."""
    from pyiceberg.exceptions import NoSuchTableError

    for table in tables:
        with contextlib.suppress(NoSuchTableError):
            catalog.purge_table(table)
