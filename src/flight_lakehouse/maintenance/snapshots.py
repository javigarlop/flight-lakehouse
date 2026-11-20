"""Snapshot expiry and orphan-file cleanup: the steps that free storage.

Expiry drops old snapshots and deletes the files only they referenced, such as rows
removed by retention and small files replaced by compaction. It keeps a day of
snapshots: a stopped silver job resumes from the bronze snapshot in its checkpoint,
and fails if that snapshot is gone.

Orphan files are files no snapshot references, left by writes that failed before
committing (a killed Spark job). A running job's files look the same until it commits,
so only files older than a few days are removed.
"""

import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from flight_lakehouse.maintenance.sql import call, timestamp, utcnow

log = logging.getLogger(__name__)

SNAPSHOT_MAX_AGE = timedelta(days=1)
RETAIN_LAST = 10  # however old, so a quiet table keeps some history
ORPHAN_MIN_AGE = timedelta(days=3)
# Below this, files of a write still in progress could be deleted.
SAFE_ORPHAN_MIN_AGE = timedelta(days=1)


def expire_snapshots(
    spark: SparkSession,
    table: str,
    max_age: timedelta = SNAPSHOT_MAX_AGE,
    retain_last: int = RETAIN_LAST,
    now: datetime | None = None,
) -> dict[str, int]:
    """Expire snapshots older than `max_age`, keeping at least `retain_last`."""
    older_than = (now or utcnow()) - max_age
    (result,) = call(
        spark,
        "expire_snapshots",
        table,
        older_than=timestamp(older_than),
        retain_last=retain_last,
    )
    counts = result.asDict()
    log.info("Expired snapshots of %s before %s: %s", table, older_than, counts)
    return counts


def remove_orphan_files(
    spark: SparkSession,
    table: str,
    min_age: timedelta = ORPHAN_MIN_AGE,
    now: datetime | None = None,
) -> int:
    """Delete files older than `min_age` that no snapshot references; returns how many."""
    if min_age < SAFE_ORPHAN_MIN_AGE:
        raise ValueError(f"min_age {min_age} could delete files of a write in progress")
    older_than = (now or utcnow()) - min_age
    removed = len(
        call(
            spark,
            "remove_orphan_files",
            table,
            older_than=timestamp(older_than),
            # List files with the table's S3FileIO: the image has no Hadoop S3 filesystem,
            # so the default Hadoop listing fails with 'No FileSystem for scheme "s3"'.
            prefix_listing="true",
        )
    )
    log.info("Removed %d orphan files of %s older than %s", removed, table, older_than)
    return removed
