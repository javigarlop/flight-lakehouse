"""Maintenance steps: the SQL each one sends, with a mocked Spark session."""

from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row  # noqa: E402

from flight_lakehouse.maintenance import compaction, retention, run, snapshots, sql  # noqa: E402
from flight_lakehouse.maintenance.run import TABLES, Table, maintain  # noqa: E402

NOW = datetime(2026, 11, 18, 8, 30, 15, tzinfo=UTC)
TABLE = "lakehouse.silver.flight_positions"


def session(*results: list[Row]) -> MagicMock:
    """A Spark session whose spark.sql(...).collect() returns `results` in turn."""
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = list(results)
    return spark


def statements(spark: MagicMock) -> list[str]:
    return [c.args[0] for c in spark.sql.call_args_list]


def test_timestamp_literal_is_utc():
    madrid = datetime(2026, 11, 18, 10, 30, tzinfo=timezone(timedelta(hours=1)))
    assert sql.timestamp(madrid) == "TIMESTAMP '2026-11-18 09:30:00Z'"


def test_utcnow_is_timezone_aware():
    assert sql.utcnow().utcoffset() == timedelta(0)


def test_timestamp_refuses_naive_datetimes():
    with pytest.raises(ValueError, match="naive datetime"):
        sql.timestamp(datetime(2026, 11, 18))


def test_timestamp_literal_parses_in_spark(spark):
    spark.conf.set("spark.sql.session.timeZone", "Europe/Madrid")
    try:
        value = spark.sql(f"SELECT {sql.timestamp(NOW)} AS t").first()["t"]
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Collected timestamps come back naive in the JVM's zone, UTC in the tests.
    assert value == datetime(2026, 11, 18, 8, 30, 15)


def test_call_runs_the_procedure_from_the_tables_catalog():
    spark = session([Row(n=1)])
    assert sql.call(spark, "expire_snapshots", TABLE, retain_last=5) == [Row(n=1)]
    assert statements(spark) == [
        "CALL lakehouse.system.expire_snapshots"
        "(table => 'silver.flight_positions', retain_last => 5)"
    ]


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (NOW, datetime(2026, 11, 15, tzinfo=UTC)),
        (datetime(2026, 11, 18, tzinfo=UTC), datetime(2026, 11, 15, tzinfo=UTC)),
        (datetime(2026, 3, 2, 23, 59, tzinfo=UTC), datetime(2026, 2, 27, tzinfo=UTC)),
    ],
)
def test_cutoff_is_the_start_of_a_day_at_least_3_days_back(now, expected):
    limit = retention.cutoff(now, 3)
    assert limit == expected
    assert now - limit >= timedelta(days=3)


def test_delete_older_than_drops_whole_days():
    spark = session()
    limit = retention.delete_older_than(spark, TABLE, "event_time", now=NOW)

    assert limit == datetime(2026, 11, 15, tzinfo=UTC)
    assert statements(spark) == [
        "DELETE FROM lakehouse.silver.flight_positions "
        "WHERE event_time < TIMESTAMP '2026-11-15 00:00:00Z'"
    ]


def test_compact_rewrites_data_files_then_manifests():
    spark = session(
        [Row(rewritten_data_files_count=40, added_data_files_count=2)],
        [Row(rewritten_manifests_count=30, added_manifests_count=1)],
    )
    counts = compaction.compact(spark, TABLE)

    assert statements(spark) == [
        "CALL lakehouse.system.rewrite_data_files(table => 'silver.flight_positions')",
        "CALL lakehouse.system.rewrite_manifests(table => 'silver.flight_positions')",
    ]
    assert counts == {
        "rewritten_data_files_count": 40,
        "added_data_files_count": 2,
        "rewritten_manifests_count": 30,
        "added_manifests_count": 1,
    }


def test_expire_snapshots_keeps_a_day_and_the_last_ten():
    spark = session([Row(deleted_data_files_count=12)])
    counts = snapshots.expire_snapshots(spark, TABLE, now=NOW)

    assert counts == {"deleted_data_files_count": 12}
    assert statements(spark) == [
        "CALL lakehouse.system.expire_snapshots(table => 'silver.flight_positions', "
        "older_than => TIMESTAMP '2026-11-17 08:30:15Z', retain_last => 10)"
    ]


def test_remove_orphan_files_counts_what_it_removed():
    spark = session([Row(orphan_file_location="s3://a"), Row(orphan_file_location="s3://b")])
    assert snapshots.remove_orphan_files(spark, TABLE, now=NOW) == 2
    assert statements(spark) == [
        "CALL lakehouse.system.remove_orphan_files(table => 'silver.flight_positions', "
        "older_than => TIMESTAMP '2026-11-15 08:30:15Z', prefix_listing => true)"
    ]


def test_remove_orphan_files_refuses_a_short_minimum_age():
    spark = session()
    with pytest.raises(ValueError, match="write in progress"):
        snapshots.remove_orphan_files(spark, TABLE, min_age=timedelta(hours=1), now=NOW)
    spark.sql.assert_not_called()


def test_every_bronze_and_silver_table_has_retention_and_gold_has_none():
    retained = {t.name: t.retention_column for t in TABLES if t.retention_column}
    assert retained == {
        "lakehouse.bronze.flight_positions": "ingested_at",
        "lakehouse.silver.flight_positions": "event_time",
        "lakehouse.silver.rejected_positions": "ingested_at",
        "lakehouse.silver.flights": "first_seen",
    }
    assert all(t.name.startswith("lakehouse.gold.") for t in TABLES if not t.retention_column)


def test_maintain_runs_the_steps_in_order_and_skips_missing_tables(monkeypatch):
    calls = []
    for module, name in [
        ("retention", "delete_older_than"),
        ("compact", "compact"),
        ("expire", "expire_snapshots"),
        ("orphans", "remove_orphan_files"),
    ]:
        monkeypatch.setattr(
            f"flight_lakehouse.maintenance.run.{name}",
            lambda _spark, table, *args, step=module, **kwargs: calls.append(
                (step, table, *args, kwargs.get("now"))
            ),
        )
    spark = MagicMock()
    spark.catalog.tableExists.side_effect = lambda name: name != "missing"
    tables = [Table("silver", "event_time"), Table("missing", "x"), Table("gold")]

    assert maintain(spark, tables, now=NOW) == ["silver", "gold"]
    assert calls == [
        ("retention", "silver", "event_time", NOW),
        ("compact", "silver", None),
        ("expire", "silver", NOW),
        ("orphans", "silver", NOW),
        ("compact", "gold", None),
        ("expire", "gold", NOW),
        ("orphans", "gold", NOW),
    ]


def test_main_maintains_every_table_and_stops_the_session(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr(run, "build_session", lambda _name: spark)
    monkeypatch.setattr(run, "maintain", lambda session: [] if session is spark else None)

    run.main()

    spark.stop.assert_called_once()


def test_main_stops_the_session_when_a_step_fails(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr(run, "build_session", lambda _name: spark)

    def fail(_spark):
        raise RuntimeError("commit conflict")

    monkeypatch.setattr(run, "maintain", fail)

    with pytest.raises(RuntimeError, match="commit conflict"):
        run.main()
    spark.stop.assert_called_once()
