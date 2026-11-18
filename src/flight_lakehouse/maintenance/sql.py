"""Spark SQL helpers the maintenance steps share.

Runs inside the Spark container (Python 3.10), so no datetime.UTC.
"""

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Row, SparkSession


def utcnow() -> datetime:
    return datetime.now(timezone.utc)  # noqa: UP017 - Python 3.10 has no datetime.UTC


def timestamp(value: datetime) -> str:
    """A Spark SQL timestamp literal, in UTC whatever the session time zone."""
    if value.tzinfo is None:
        raise ValueError(f"naive datetime {value}: pass a timezone-aware one")
    return f"TIMESTAMP '{value.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S}Z'"  # noqa: UP017


def call(spark: SparkSession, procedure: str, table: str, **args: Any) -> list[Row]:
    """Run an Iceberg procedure on a table: CALL <catalog>.system.<procedure>(table => ...).

    `table` is fully qualified (catalog.namespace.table); `args` are SQL expressions.
    """
    catalog, name = table.split(".", 1)
    params = ", ".join([f"table => '{name}'", *(f"{k} => {v}" for k, v in args.items())])
    return spark.sql(f"CALL {catalog}.system.{procedure}({params})").collect()
