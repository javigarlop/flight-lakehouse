"""Compaction: rewrite the small files the streaming jobs write into fewer, larger ones.

A 30-second trigger commits up to 2,880 times a day, each with a few tiny Parquet files
and a manifest (streaming writes use fast appends, which never merge manifests). Queries
then open thousands of files. Rewriting data files and manifests keeps the rows and
adds a replace snapshot, which streaming readers ignore.
"""

import logging

from pyspark.sql import SparkSession

from flight_lakehouse.maintenance.sql import call

log = logging.getLogger(__name__)


def compact(spark: SparkSession, table: str) -> dict[str, int]:
    """Rewrite the table's data files, then its manifests; returns Iceberg's counts."""
    (files,) = call(spark, "rewrite_data_files", table)
    (manifests,) = call(spark, "rewrite_manifests", table)
    counts = {**files.asDict(), **manifests.asDict()}
    log.info("Compacted %s: %s", table, counts)
    return counts
