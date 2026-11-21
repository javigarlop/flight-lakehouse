"""Create the Iceberg tables the streaming jobs write to, and evolve their schemas."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import partitioning
from pyspark.sql.types import StructType


def ensure_table(spark: SparkSession, table: str, schema: StructType, day_column: str) -> list[str]:
    """Create the table if missing, partitioned by day of `day_column`; otherwise add
    the columns of `schema` it lacks.

    Adding a column is Iceberg schema evolution: a metadata change only, so no data
    file is rewritten and rows written before it read the new column as null. This
    is how a field from a new Avro schema version (such as v2's emergency) reaches
    bronze and silver without recreating the tables or resetting the checkpoints.

    Returns the table's columns in order. Iceberg checks that a write's columns are
    in the table's order, and added columns go last, so select the rows in this
    order before writing them.
    """
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if not spark.catalog.tableExists(table):
        (
            spark.createDataFrame([], schema)
            .writeTo(table)
            .using("iceberg")
            .partitionedBy(partitioning.days(day_column))
            .tableProperty("format-version", "2")
            .create()
        )
        return schema.fieldNames()

    existing = list(spark.table(table).columns)
    added = [field for field in schema.fields if field.name not in existing]
    for field in added:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")
    return [*existing, *(field.name for field in added)]
