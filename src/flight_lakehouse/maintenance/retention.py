"""Retention: delete bronze and silver rows older than 7 days.

The cutoff is the start of a UTC day, so a delete drops whole day partitions and
Iceberg only rewrites metadata. Everything newer than the cutoff stays: between 7 and
8 days of rows. Deleting frees no storage by itself; expiring the snapshots that still
reference the files does (see snapshots.py). Streaming readers skip delete snapshots,
so the jobs keep running.
"""

import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from flight_lakehouse.maintenance.sql import timestamp, utcnow

log = logging.getLogger(__name__)

RETENTION_DAYS = 7


def cutoff(now: datetime, days: int) -> datetime:
    """Start of the day that contains `now` minus `days`: rows before it are deleted."""
    return (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)


def delete_older_than(
    spark: SparkSession,
    table: str,
    column: str,
    days: int = RETENTION_DAYS,
    now: datetime | None = None,
) -> datetime:
    """Delete rows whose `column` (the table's day partition column) is before the cutoff."""
    limit = cutoff(now or utcnow(), days)
    log.info("Deleting rows of %s with %s before %s", table, column, limit)
    spark.sql(f"DELETE FROM {table} WHERE {column} < {timestamp(limit)}")  # noqa: S608 - names come from code
    return limit
