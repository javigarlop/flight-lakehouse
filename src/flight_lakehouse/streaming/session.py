"""SparkSession wired to the Polaris catalog and SeaweedFS.

Runs inside the Spark container (Python 3.10, see infra/spark/Dockerfile), where
services are reached by their Compose names.
"""

import os

from pyspark.sql import SparkSession

CATALOG = "lakehouse"


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def build_session(app_name: str) -> SparkSession:
    polaris = _env("POLARIS_URI", "http://polaris:8181/api/catalog")
    prefix = f"spark.sql.catalog.{CATALOG}"
    client_id = _env("POLARIS_CLIENT_ID", "root")
    client_secret = _env("POLARIS_CLIENT_SECRET", "polaris-secret")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", polaris)
        .config(f"{prefix}.warehouse", _env("POLARIS_CATALOG", CATALOG))
        .config(f"{prefix}.credential", f"{client_id}:{client_secret}")
        .config(f"{prefix}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"{prefix}.oauth2-server-uri", f"{polaris}/v1/oauth/tokens")
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.endpoint", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
        .config(f"{prefix}.s3.path-style-access", "true")
        .config(f"{prefix}.s3.access-key-id", _env("S3_ACCESS_KEY", "lakehouse"))
        .config(f"{prefix}.s3.secret-access-key", _env("S3_SECRET_KEY", "lakehouse-secret"))
        .config(f"{prefix}.client.region", "us-east-1")
        # Polaris rejects write-metrics reports for this principal; they are optional.
        .config(f"{prefix}.rest-metrics-reporting-enabled", "false")
        # Small local stack: few shuffle partitions keep micro-batches fast.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
