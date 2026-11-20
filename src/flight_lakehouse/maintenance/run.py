"""Maintenance run: retention, compaction, snapshot expiry and orphan cleanup, per table.

The order matters. Retention and compaction leave files only old snapshots reference;
expiry then deletes them, and orphan cleanup catches what failed writes left behind.
Gold has no retention: its hourly and daily aggregates are tiny and kept for good.

Run with spark-submit, like the streaming jobs: Airflow's daily DAG does (dags/lakehouse.py),
and so does make maintain.
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import SparkSession

from flight_lakehouse.maintenance.compaction import compact
from flight_lakehouse.maintenance.retention import delete_older_than
from flight_lakehouse.maintenance.snapshots import expire_snapshots, remove_orphan_files
from flight_lakehouse.maintenance.sql import utcnow
from flight_lakehouse.streaming import silver
from flight_lakehouse.streaming.bronze import TABLE as BRONZE_TABLE
from flight_lakehouse.streaming.session import CATALOG, build_session

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Table:
    name: str
    # The day partition column retention deletes by; None keeps every row.
    retention_column: str | None = None


GOLD_MODELS = (
    "airport_traffic_hourly",
    "route_stats_daily",
    "holding_patterns",
    "pipeline_freshness",
)
TABLES = (
    Table(BRONZE_TABLE, "ingested_at"),
    Table(silver.POSITIONS_TABLE, "event_time"),
    Table(silver.REJECTED_TABLE, "ingested_at"),
    Table(silver.FLIGHTS_TABLE, "first_seen"),
    *(Table(f"{CATALOG}.gold.{model}") for model in GOLD_MODELS),
)


def maintain(
    spark: SparkSession, tables: Iterable[Table] = TABLES, now: datetime | None = None
) -> list[str]:
    """Maintain each table that exists; returns the names of those maintained."""
    now = now or utcnow()
    done = []
    for table in tables:
        if not spark.catalog.tableExists(table.name):
            log.info("Skipping %s: it doesn't exist yet", table.name)
            continue
        if table.retention_column:
            delete_older_than(spark, table.name, table.retention_column, now=now)
        compact(spark, table.name)
        expire_snapshots(spark, table.name, now=now)
        remove_orphan_files(spark, table.name, now=now)
        done.append(table.name)
    return done


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = build_session("maintenance")
    try:
        done = maintain(spark)
    finally:
        spark.stop()
    log.info("Maintained %d tables: %s", len(done), ", ".join(done))


if __name__ == "__main__":
    main()
