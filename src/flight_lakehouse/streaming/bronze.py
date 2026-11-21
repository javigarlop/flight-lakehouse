"""Bronze job: Kafka topic -> Iceberg table lakehouse.bronze.flight_positions.

Every message is kept as received, decoded from Avro, with its Kafka metadata.
Messages that fail to decode are kept too, with their raw bytes, so nothing is
silently dropped; silver decides what is valid.

Schema versions: each message is decoded with every known writer schema
(schemas/flight_position.v<N>.avsc), newest first, read into the newest one, and
the first that decodes wins; `schema_version` records which. This works because
each version only appends optional fields: older bytes run out before a newer
writer schema is done, so that attempt fails and an older one is tried. v1 and
v2 events can therefore share the topic, and when the newest schema adds a
field, the table gains the column at startup (see tables.ensure_table).

Exactly-once: the Spark checkpoint records which offsets each micro-batch read,
and Iceberg's streaming sink skips a batch already committed, so a restart
neither loses nor duplicates rows.

Run inside the Spark container:
    spark-submit src/flight_lakehouse/streaming/bronze.py [--once]
"""

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from flight_lakehouse.config import schema_versions
from flight_lakehouse.streaming.metrics import serve_metrics
from flight_lakehouse.streaming.session import CATALOG, build_session
from flight_lakehouse.streaming.tables import ensure_table

SCHEMAS_DIR = Path(os.environ.get("SCHEMAS_DIR", "/app/schemas"))
TABLE = f"{CATALOG}.bronze.flight_positions"

# Confluent wire format: 1 magic byte, 4-byte schema id, then the Avro body.
WIRE_HEADER_BYTES = 5


def load_schemas(directory: Path | None = None) -> dict[int, str]:
    """Every position schema's text, by version number (default: from SCHEMAS_DIR)."""
    versions = schema_versions(directory or SCHEMAS_DIR)
    return {version: path.read_text() for version, path in versions.items()}


def decode(kafka: DataFrame, schemas: dict[int, str]) -> DataFrame:
    """Turn raw Kafka rows into bronze rows, whichever schema version wrote them."""
    body = F.expr(f"substring(value, {WIRE_HEADER_BYTES + 1}, length(value) - {WIRE_HEADER_BYTES})")
    schema_id = F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int")
    newest_first = sorted(schemas, reverse=True)
    reader = schemas[newest_first[0]]
    # PERMISSIVE: a message that doesn't decode gives a struct of nulls instead of failing
    # the job. avroSchema is the reader schema: fields a writer lacks get their default.
    attempts = [
        from_avro(body, schemas[version], {"mode": "PERMISSIVE", "avroSchema": reader}).alias(
            f"v{version}"
        )
        for version in newest_first
    ]
    # icao24 is required by every version, so a null one means that attempt didn't decode.
    decoded = [F.col(f"v{version}.icao24").isNotNull() for version in newest_first]
    position = F.col(f"v{newest_first[-1]}")  # all nulls when nothing decoded
    version_used = F.lit(None).cast("int")
    for version, ok in reversed(list(zip(newest_first, decoded, strict=True))):
        position = F.when(ok, F.col(f"v{version}")).otherwise(position)
        version_used = F.when(ok, F.lit(version)).otherwise(version_used)
    return (
        kafka.select(*attempts, "value", "topic", "partition", "offset", "timestamp")
        .select(
            position.alias("p"),
            version_used.alias("schema_version"),
            schema_id.alias("schema_id"),
            F.col("value").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "p.*",
            "schema_id",
            "schema_version",
            # Raw bytes only when decoding failed, so the table doesn't store everything twice.
            # icao24 is required by the schema, so a null one means the message didn't decode.
            F.when(F.col("p.icao24").isNull(), F.col("raw_value")).alias("raw_value"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.current_timestamp().alias("ingested_at"),
        )
    )


def run(
    topic: str, table: str, checkpoint: str, once: bool, trigger_s: int, max_offsets: int
) -> None:
    spark = build_session("bronze")
    serve_metrics(spark)
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # Kafka deletes messages after 7 days; if the job was down longer, carry on.
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .load()
    )
    rows = decode(kafka, load_schemas())
    rows = rows.select(*ensure_table(spark, table, rows.schema, day_column="ingested_at"))

    writer = (
        rows.writeStream.format("iceberg")
        .queryName(table)
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
    )
    writer = (
        writer.trigger(availableNow=True)
        if once
        else writer.trigger(processingTime=f"{trigger_s} seconds")
    )
    query = writer.toTable(table)
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--topic", default=os.environ.get("POSITIONS_TOPIC", "flight.positions.v1"))
    parser.add_argument("--table", default=TABLE)
    parser.add_argument("--checkpoint", default=None, help="default: $CHECKPOINT_DIR/<table>")
    parser.add_argument(
        "--once", action="store_true", help="process what is in Kafka now, then stop"
    )
    parser.add_argument(
        "--trigger-seconds", type=int, default=int(os.environ.get("TRIGGER_SECONDS", "30"))
    )
    parser.add_argument(
        "--max-offsets", type=int, default=50000, help="most messages read per micro-batch"
    )
    args = parser.parse_args()
    checkpoint = (
        args.checkpoint or f"{os.environ.get('CHECKPOINT_DIR', '/checkpoints')}/{args.table}"
    )
    run(args.topic, args.table, checkpoint, args.once, args.trigger_seconds, args.max_offsets)


if __name__ == "__main__":
    main()
