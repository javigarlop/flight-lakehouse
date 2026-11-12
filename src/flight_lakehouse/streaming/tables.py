"""Create the Iceberg tables the streaming jobs write to."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import partitioning
from pyspark.sql.types import StructType


def ensure_table(spark: SparkSession, table: str, schema: StructType, day_column: str) -> None:
    """Create the table and its namespace if missing, partitioned by day of `day_column`."""
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if spark.catalog.tableExists(table):
        return
    (
        spark.createDataFrame([], schema)
        .writeTo(table)
        .using("iceberg")
        .partitionedBy(partitioning.days(day_column))
        .tableProperty("format-version", "2")
        .create()
    )
