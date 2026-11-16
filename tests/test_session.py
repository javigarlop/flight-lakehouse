from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from flight_lakehouse.streaming import session  # noqa: E402

PREFIX = "spark.sql.catalog.lakehouse"
ENV = (
    "POLARIS_URI",
    "POLARIS_CLIENT_ID",
    "POLARIS_CLIENT_SECRET",
    "POLARIS_CATALOG",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
)


def configs(monkeypatch) -> dict[str, str]:
    spark_session = MagicMock()
    monkeypatch.setattr(session, "SparkSession", spark_session)
    session.build_session("job")
    spark_session.builder.appName.assert_called_once_with("job")
    return {c.args[0]: c.args[1] for c in spark_session.mock_calls if c[0].endswith(".config")}


def test_defaults_point_at_the_compose_services(monkeypatch):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)

    conf = configs(monkeypatch)

    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf[PREFIX] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf[f"{PREFIX}.uri"] == "http://polaris:8181/api/catalog"
    assert conf[f"{PREFIX}.oauth2-server-uri"] == "http://polaris:8181/api/catalog/v1/oauth/tokens"
    assert conf[f"{PREFIX}.warehouse"] == "lakehouse"
    assert conf[f"{PREFIX}.credential"] == "root:polaris-secret"
    assert conf[f"{PREFIX}.s3.endpoint"] == "http://seaweedfs:8333"


def test_environment_overrides_defaults(monkeypatch):
    for name in ENV:
        monkeypatch.setenv(name, name.lower())
    # An empty variable, as Compose passes an unset one, falls back to the default.
    monkeypatch.setenv("S3_ENDPOINT", "")

    conf = configs(monkeypatch)

    assert conf[f"{PREFIX}.uri"] == "polaris_uri"
    assert conf[f"{PREFIX}.warehouse"] == "polaris_catalog"
    assert conf[f"{PREFIX}.credential"] == "polaris_client_id:polaris_client_secret"
    assert conf[f"{PREFIX}.s3.access-key-id"] == "s3_access_key"
    assert conf[f"{PREFIX}.s3.secret-access-key"] == "s3_secret_key"
    assert conf[f"{PREFIX}.s3.endpoint"] == "http://seaweedfs:8333"
