"""Silver job: bronze.flight_positions -> silver.flight_positions, rejected_positions and flights.

Each bronze row is checked against the validation rules below:
- Rows that pass are deduplicated on (icao24, event_time) and appended to
  silver.flight_positions, partitioned by event day.
- Rows that fail go to silver.rejected_positions with every reason that applies,
  so nothing is silently dropped.

Duplicates are expected. If an aircraft's position doesn't change between two
polls, adsb.lol returns it again with the same event_time, and replays repeat
whole recordings. The deduplication state only covers a watermark on
event_time (default 15 minutes, longer than the 4-minute poll interval). After
that the state is dropped, and a row arriving further behind the newest
event_time seen is discarded as late. Spark reports those rows as
numRowsDroppedByWatermark in the query progress. Bronze still holds them, so
a backfill can recover them.

A third query reads silver.flight_positions and sessionises it into silver.flights
(see flights.py). It matches airports from reference.airports, which make dbt-seed
loads; the job waits for that table, like it waits for bronze.

Each streaming query has its own checkpoint. Iceberg's sink skips a micro-batch it
already committed, so a restart neither loses nor duplicates rows.

Run inside the Spark container:
    spark-submit src/flight_lakehouse/streaming/silver.py [--once]
"""

import argparse
import logging
import os
import time

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from flight_lakehouse.streaming.bronze import TABLE as BRONZE_TABLE
from flight_lakehouse.streaming.flights import AIRPORTS_TABLE, load_airports, sessionise
from flight_lakehouse.streaming.session import CATALOG, build_session
from flight_lakehouse.streaming.tables import ensure_table

POSITIONS_TABLE = f"{CATALOG}.silver.flight_positions"
REJECTED_TABLE = f"{CATALOG}.silver.rejected_positions"
FLIGHTS_TABLE = f"{CATALOG}.silver.flights"
DEDUP_KEY = ["icao24", "event_time"]

# Columns kept in silver: the position fields plus lineage back to bronze and Kafka.
POSITION_COLUMNS = [
    "icao24",
    "callsign",
    "registration",
    "aircraft_type",
    "category",
    "source",
    "event_time",
    "received_at",
    "latitude",
    "longitude",
    "baro_altitude",
    "geo_altitude",
    "velocity",
    "true_track",
    "vertical_rate",
    "on_ground",
    "squawk",
    "schema_id",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "ingested_at",
]

# Plausibility bounds in SI units, wide enough for any civil or military aircraft.
MIN_ALTITUDE_M = -500.0  # below sea level airports, plus sensor noise
MAX_ALTITUDE_M = 25_000.0
MAX_VELOCITY_MS = 700.0  # about Mach 2 at cruise altitude

log = logging.getLogger(__name__)


def _outside(column: str, low: float, high: float) -> Column:
    # Null means "not reported", which is valid; only a reported value can be out of range.
    return F.col(column).isNotNull() & ~F.col(column).between(low, high)


def rules() -> list[tuple[str, Column]]:
    """(reason, condition) pairs; a row is rejected when any condition is true.

    Built on demand because Column expressions need an active SparkContext.
    """
    return [
        # icao24 is required by the Avro schema; a null one means the message didn't decode.
        # (Bronze rows written before raw_value was kept on failure have no raw bytes.)
        ("decode_failed", F.col("icao24").isNull()),
        ("invalid_icao24", ~F.col("icao24").rlike("^~?[0-9a-f]{6}$")),
        ("event_after_receipt", F.col("event_time") > F.col("received_at")),
        ("latitude_out_of_range", _outside("latitude", -90.0, 90.0)),
        ("longitude_out_of_range", _outside("longitude", -180.0, 180.0)),
        ("baro_altitude_out_of_range", _outside("baro_altitude", MIN_ALTITUDE_M, MAX_ALTITUDE_M)),
        ("geo_altitude_out_of_range", _outside("geo_altitude", MIN_ALTITUDE_M, MAX_ALTITUDE_M)),
        ("velocity_out_of_range", _outside("velocity", 0.0, MAX_VELOCITY_MS)),
        ("true_track_out_of_range", _outside("true_track", 0.0, 360.0)),
    ]


def with_reject_reasons(bronze: DataFrame) -> DataFrame:
    """Add `reject_reasons`: the reasons that apply to each row, empty when it is valid."""
    # A condition that evaluates to null (a failed decode leaves every field null)
    # gives a null entry, which the filter removes, so only true conditions count.
    reasons = F.array(*(F.when(condition, F.lit(reason)) for reason, condition in rules()))
    return bronze.withColumn("reject_reasons", F.filter(reasons, lambda r: r.isNotNull()))


def valid_positions(checked: DataFrame, watermark: str) -> DataFrame:
    """Rows that pass every rule, deduplicated on (icao24, event_time)."""
    return (
        checked.where(F.size("reject_reasons") == 0)
        .select(*POSITION_COLUMNS)
        .withWatermark("event_time", watermark)
        # The key includes the watermark column, so Spark drops each key's state
        # once the watermark passes it, and the state stays bounded.
        .dropDuplicates(DEDUP_KEY)
        .withColumn("processed_at", F.current_timestamp())
    )


def rejected_positions(checked: DataFrame) -> DataFrame:
    """Rows that fail at least one rule, with every bronze column kept for diagnosis."""
    return checked.where(F.size("reject_reasons") > 0).withColumn(
        "rejected_at", F.current_timestamp()
    )


def read_table(spark: SparkSession, table: str, max_files: int) -> DataFrame:
    """Stream the rows appended to an Iceberg table."""
    return (
        spark.readStream.format("iceberg")
        # Retention deletes and compaction rewrites add no new rows; skip them
        # instead of failing the stream.
        .option("streaming-skip-delete-snapshots", "true")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-max-files-per-micro-batch", str(max_files))
        .load(table)
    )


def wait_for_table(
    spark: SparkSession, table: str, timeout_s: float = 600, created_by: str = "bronze"
) -> None:
    """Wait for a table another process creates, such as bronze, which may start with us."""
    deadline = time.monotonic() + timeout_s
    while not spark.catalog.tableExists(table):
        if time.monotonic() > deadline:
            raise TimeoutError(f"{table} doesn't exist after {timeout_s:.0f} s")
        log.info("Waiting for %s to be created by %s", table, created_by)
        time.sleep(10)


def start(
    rows: DataFrame, table: str, checkpoint: str, once: bool, trigger_s: int
) -> StreamingQuery:
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
    return writer.toTable(table)


def run(
    source: str,
    positions_table: str,
    rejected_table: str,
    flights_table: str,
    airports_table: str,
    checkpoint_dir: str,
    once: bool,
    trigger_s: int,
    max_files: int,
    watermark: str,
    session_gap: str,
) -> None:
    spark = build_session("silver")
    wait_for_table(spark, source)
    checked = with_reject_reasons(read_table(spark, source, max_files))

    outputs = [
        (valid_positions(checked, watermark), positions_table, "event_time"),
        (rejected_positions(checked), rejected_table, "ingested_at"),
    ]
    queries = []
    for rows, table, day_column in outputs:
        ensure_table(spark, table, rows.schema, day_column)
        queries.append(start(rows, table, f"{checkpoint_dir}/{table}", once, trigger_s))

    wait_for_table(spark, airports_table, created_by="make dbt-seed")
    flights = sessionise(
        read_table(spark, positions_table, max_files),
        session_gap,
        watermark,
        load_airports(spark, airports_table),
    )
    ensure_table(spark, flights_table, flights.schema, "first_seen")
    flights_checkpoint = f"{checkpoint_dir}/{flights_table}"

    if once:
        for query in queries:
            query.awaitTermination()
        # Flights read the positions table, so they start once this run's positions are in.
        start(flights, flights_table, flights_checkpoint, once, trigger_s).awaitTermination()
    else:
        queries.append(start(flights, flights_table, flights_checkpoint, once, trigger_s))
        # Raises as soon as any query fails, so the container restarts them all.
        spark.streams.awaitAnyTermination()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default=BRONZE_TABLE, help="bronze table to read")
    parser.add_argument("--positions-table", default=POSITIONS_TABLE)
    parser.add_argument("--rejected-table", default=REJECTED_TABLE)
    parser.add_argument("--flights-table", default=FLIGHTS_TABLE)
    parser.add_argument("--airports-table", default=AIRPORTS_TABLE, help="loaded by dbt")
    parser.add_argument(
        "--checkpoint-dir",
        default=os.environ.get("CHECKPOINT_DIR", "/checkpoints"),
        help="each query checkpoints to <dir>/<table>",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="process what is in bronze now, then stop (flights still open stay in state)",
    )
    parser.add_argument(
        "--trigger-seconds", type=int, default=int(os.environ.get("TRIGGER_SECONDS", "30"))
    )
    parser.add_argument(
        "--max-files", type=int, default=100, help="most bronze files read per micro-batch"
    )
    parser.add_argument(
        "--watermark",
        default=os.environ.get("SILVER_WATERMARK", "15 minutes"),
        help="how far behind the newest event_time a duplicate or late position is still used",
    )
    parser.add_argument(
        "--session-gap",
        default=os.environ.get("FLIGHT_SESSION_GAP", "20 minutes"),
        help="a longer gap between an aircraft's positions starts a new flight",
    )
    args = parser.parse_args()
    run(
        args.source,
        args.positions_table,
        args.rejected_table,
        args.flights_table,
        args.airports_table,
        args.checkpoint_dir,
        args.once,
        args.trigger_seconds,
        args.max_files,
        args.watermark,
        args.session_gap,
    )


if __name__ == "__main__":
    main()
