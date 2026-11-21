"""Silver validation, deduplication and job wiring, run on a local Spark in batch mode.

Needs Java 17 or later. Without it these tests are skipped locally; in CI they must run.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, call

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flight_lakehouse.streaming import silver  # noqa: E402
from flight_lakehouse.streaming.flights import Airport  # noqa: E402
from flight_lakehouse.streaming.tables import ensure_table  # noqa: E402

# The columns bronze.decode produced before schema v2, in the same order. Silver adds
# v2's columns (EVOLVED_COLUMNS) to such rows; the v2 tests below add them here.
BRONZE_SCHEMA = StructType(
    [
        StructField("icao24", StringType()),
        StructField("callsign", StringType()),
        StructField("registration", StringType()),
        StructField("aircraft_type", StringType()),
        StructField("category", StringType()),
        StructField("source", StringType()),
        StructField("event_time", TimestampType()),
        StructField("received_at", TimestampType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("baro_altitude", DoubleType()),
        StructField("geo_altitude", DoubleType()),
        StructField("velocity", DoubleType()),
        StructField("true_track", DoubleType()),
        StructField("vertical_rate", DoubleType()),
        StructField("on_ground", BooleanType()),
        StructField("squawk", StringType()),
        StructField("schema_id", IntegerType()),
        StructField("raw_value", BinaryType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingested_at", TimestampType()),
    ]
)
# Naive datetimes: Spark returns them naive, so comparisons stay simple.
T0 = datetime(2026, 11, 1, 12, 0)
# The fields decoded from Avro, before bronze's own columns.
AVRO_FIELDS = BRONZE_SCHEMA.fieldNames()[: BRONZE_SCHEMA.fieldNames().index("schema_id")]


_offsets = iter(range(1_000_000))


def bronze_row(**overrides):
    """A valid bronze row; keyword arguments replace fields."""
    row = {
        "icao24": "4d2260",
        "callsign": "RYR3LL",
        "registration": "9H-QDS",
        "aircraft_type": "B738",
        "category": "A3",
        "source": "adsb_icao",
        "event_time": T0,
        "received_at": T0 + timedelta(seconds=2),
        "latitude": 41.3,
        "longitude": 2.08,
        "baro_altitude": 11270.0,
        "geo_altitude": 11620.5,
        "velocity": 207.8,
        "true_track": 197.13,
        "vertical_rate": 0.0,
        "on_ground": False,
        "squawk": "1000",
        "schema_id": 1,
        "raw_value": None,
        "kafka_topic": "flight.positions.v1",
        "kafka_partition": 0,
        "kafka_offset": next(_offsets),
        "kafka_timestamp": T0 + timedelta(seconds=3),
        "ingested_at": T0 + timedelta(seconds=30),
    }
    row.update(overrides)
    return row


def checked(spark, *rows):
    frame = spark.createDataFrame(
        [tuple(r[f.name] for f in BRONZE_SCHEMA) for r in rows], BRONZE_SCHEMA
    )
    return silver.with_reject_reasons(frame)


def reasons_of(spark, row) -> list[str]:
    return checked(spark, row).first()["reject_reasons"]


def test_valid_row_has_no_reasons(spark):
    assert reasons_of(spark, bronze_row()) == []


def test_missing_optional_fields_are_valid(spark):
    row = bronze_row(
        callsign=None,
        baro_altitude=None,
        geo_altitude=None,
        velocity=None,
        true_track=None,
        vertical_rate=None,
    )
    assert reasons_of(spark, row) == []


def test_non_icao_address_is_valid(spark):
    assert reasons_of(spark, bronze_row(icao24="~2a3b4c")) == []


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"icao24": "4D2260"}, "invalid_icao24"),
        ({"icao24": "4d22"}, "invalid_icao24"),
        ({"event_time": T0 + timedelta(minutes=5)}, "event_after_receipt"),
        ({"latitude": 91.0}, "latitude_out_of_range"),
        ({"longitude": -180.5}, "longitude_out_of_range"),
        ({"baro_altitude": 30_000.0}, "baro_altitude_out_of_range"),
        ({"geo_altitude": -1_000.0}, "geo_altitude_out_of_range"),
        ({"velocity": -1.0}, "velocity_out_of_range"),
        ({"true_track": 361.0}, "true_track_out_of_range"),
    ],
)
def test_each_rule_rejects(spark, overrides, reason):
    assert reasons_of(spark, bronze_row(**overrides)) == [reason]


def test_every_failing_rule_is_listed(spark):
    row = bronze_row(latitude=95.0, velocity=900.0)
    assert reasons_of(spark, row) == ["latitude_out_of_range", "velocity_out_of_range"]


def test_decode_failure_gives_one_reason(spark):
    # bronze.decode leaves every position field null and keeps the raw bytes.
    fields = dict.fromkeys(AVRO_FIELDS)
    row = bronze_row(**fields, raw_value=b"\x00\x00\x00\x00\x01garbage")
    assert reasons_of(spark, row) == ["decode_failed"]


def test_decode_failure_without_raw_bytes_is_rejected(spark):
    # Older bronze rows: the failed decode was stored without its raw bytes.
    row = bronze_row(**dict.fromkeys(AVRO_FIELDS))
    assert reasons_of(spark, row) == ["decode_failed"]


def test_valid_positions_drop_duplicates_and_rejects(spark):
    first = bronze_row()
    rows = checked(
        spark,
        first,
        bronze_row(),  # same aircraft and event_time, as a later poll returns it
        bronze_row(event_time=T0 + timedelta(seconds=1), received_at=T0 + timedelta(seconds=5)),
        bronze_row(icao24="3c6444"),
        bronze_row(latitude=200.0),
    )
    valid = silver.valid_positions(rows, "15 minutes")

    keys = sorted((r["icao24"], r["event_time"]) for r in valid.collect())
    assert keys == [
        ("3c6444", T0),
        ("4d2260", T0),
        ("4d2260", (T0 + timedelta(seconds=1))),
    ]
    assert valid.columns == [*silver.POSITION_COLUMNS, "processed_at"]


def test_rejected_positions_keep_bronze_columns(spark):
    rows = checked(spark, bronze_row(), bronze_row(latitude=200.0))
    rejected = silver.rejected_positions(rows).collect()

    assert len(rejected) == 1
    assert rejected[0]["reject_reasons"] == ["latitude_out_of_range"]
    assert rejected[0]["latitude"] == 200.0
    assert rejected[0]["rejected_at"] is not None
    assert silver.rejected_positions(rows).columns == [
        *BRONZE_SCHEMA.fieldNames(),
        *silver.EVOLVED_COLUMNS,  # this bronze schema is from before v2
        "reject_reasons",
        "rejected_at",
    ]


# Job wiring. Iceberg isn't on the local Spark's classpath, so the session is a mock;
# real DataFrames still go through the transforms. The Docker test covers it end to end.


def option_calls(mock) -> dict[str, str]:
    return {c.args[0]: c.args[1] for c in mock.mock_calls if c[0].endswith(".option")}


def test_read_table_skips_maintenance_snapshots():
    spark = MagicMock()
    silver.read_table(spark, "lakehouse.bronze.t", max_files=7)

    spark.readStream.format.assert_called_once_with("iceberg")
    assert option_calls(spark) == {
        "streaming-skip-delete-snapshots": "true",
        "streaming-skip-overwrite-snapshots": "true",
        "streaming-max-files-per-micro-batch": "7",
    }
    assert call.readStream.format().option().option().option().load("lakehouse.bronze.t") in (
        spark.mock_calls
    )


@pytest.mark.parametrize(
    ("once", "trigger"),
    [(True, {"availableNow": True}), (False, {"processingTime": "5 seconds"})],
)
def test_start_writes_to_iceberg_with_checkpoint(once, trigger):
    rows = MagicMock()
    silver.start(rows, "lakehouse.silver.t", "/checkpoints/t", once=once, trigger_s=5)

    rows.writeStream.format.assert_called_once_with("iceberg")
    assert option_calls(rows) == {
        "checkpointLocation": "/checkpoints/t",
        "fanout-enabled": "true",
    }
    trigger_calls = [c for c in rows.mock_calls if c[0].endswith(".trigger")]
    assert [c.kwargs for c in trigger_calls] == [trigger]
    assert any(
        c[0].endswith(".toTable") and c.args == ("lakehouse.silver.t",) for c in rows.mock_calls
    )


def test_wait_for_table_polls_until_bronze_exists(monkeypatch):
    sleeps = []
    monkeypatch.setattr(silver.time, "sleep", sleeps.append)
    spark = MagicMock()
    spark.catalog.tableExists.side_effect = [False, False, True]

    silver.wait_for_table(spark, "lakehouse.bronze.t")

    assert sleeps == [10, 10]


def test_wait_for_table_gives_up(monkeypatch):
    monkeypatch.setattr(silver.time, "sleep", lambda _s: None)
    clock = iter([0.0, 5.0, 11.0])
    monkeypatch.setattr(silver.time, "monotonic", lambda: next(clock))
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False

    with pytest.raises(TimeoutError, match="doesn't exist after 10 s"):
        silver.wait_for_table(spark, "lakehouse.bronze.t", timeout_s=10)


def test_wait_for_table_says_what_creates_it(monkeypatch, caplog):
    monkeypatch.setattr(silver.time, "sleep", lambda _s: None)
    spark = MagicMock()
    spark.catalog.tableExists.side_effect = [False, True]

    with caplog.at_level("INFO"):
        silver.wait_for_table(spark, "lakehouse.reference.airports", created_by="make dbt-seed")

    assert "lakehouse.reference.airports to be created by make dbt-seed" in caplog.text


@pytest.fixture
def job(spark, monkeypatch):
    """silver.run with the session, table creation and queries replaced by mocks."""
    session = MagicMock()
    tables, started, read, waited = [], [], [], []
    bronze = spark.createDataFrame([], BRONZE_SCHEMA)
    monkeypatch.setattr(silver, "build_session", lambda _name: session)
    monkeypatch.setattr(silver, "serve_metrics", lambda _spark: None)
    monkeypatch.setattr(
        silver, "wait_for_table", lambda _spark, table, **_kwargs: waited.append(table)
    )
    monkeypatch.setattr(
        silver, "load_airports", lambda _spark, table: [Airport(table, 41.3, 2.08, 4.0)]
    )

    def read_table(_spark, table, _max_files):
        read.append(table)
        return bronze  # it has every column the flights query needs too

    monkeypatch.setattr(silver, "read_table", read_table)
    monkeypatch.setattr(
        silver,
        "ensure_table",
        lambda _spark, table, schema, day: tables.append((table, day)) or schema.fieldNames(),
    )

    def start(_rows, table, checkpoint, once, _trigger_s):
        started.append((table, checkpoint, once))
        return MagicMock()

    monkeypatch.setattr(silver, "start", start)

    def run(once: bool):
        silver.run(
            "b",
            "p",
            "r",
            "f",
            "a",
            "/cp",
            once=once,
            trigger_s=30,
            max_files=10,
            watermark="1 minute",
            session_gap="20 minutes",
        )

    return run, session, tables, started, read, waited


def test_run_once_starts_flights_after_positions(job):
    run, session, tables, started, read, waited = job
    run(once=True)

    assert waited == ["b", "a"]  # bronze, and the airports dbt loads
    assert read == ["b", "p"]  # flights read the positions table
    assert tables == [("p", "event_time"), ("r", "ingested_at"), ("f", "first_seen")]
    assert started == [("p", "/cp/p", True), ("r", "/cp/r", True), ("f", "/cp/f", True)]
    session.streams.awaitAnyTermination.assert_not_called()


def test_run_continuous_runs_all_queries_until_one_stops(job):
    run, session, _tables, started, _read, _waited = job
    run(once=False)

    assert [table for table, _cp, _once in started] == ["p", "r", "f"]
    session.streams.awaitAnyTermination.assert_called_once()


def test_main_passes_arguments_to_run(monkeypatch):
    calls = []
    monkeypatch.setattr(silver, "run", lambda *args: calls.append(args))
    monkeypatch.setattr(
        "sys.argv",
        [
            "silver.py",
            "--source=b",
            "--once",
            "--max-files=3",
            "--watermark=5 minutes",
            "--session-gap=30 minutes",
        ],
    )
    monkeypatch.setenv("CHECKPOINT_DIR", "/cp")

    silver.main()

    assert calls == [
        (
            "b",
            silver.POSITIONS_TABLE,
            silver.REJECTED_TABLE,
            silver.FLIGHTS_TABLE,
            silver.AIRPORTS_TABLE,
            "/cp",
            True,
            30,
            3,
            "5 minutes",
            "30 minutes",
        )
    ]


def test_ensure_table_creates_partitioned_iceberg_table(spark):  # noqa: ARG001 - F.days needs it
    session = MagicMock()
    session.catalog.tableExists.return_value = False

    ensure_table(session, "lakehouse.silver.t", BRONZE_SCHEMA, "event_time")

    session.sql.assert_called_once_with("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    names = [c[0] for c in session.mock_calls]
    assert "createDataFrame().writeTo().using().partitionedBy().tableProperty().create" in names
    assert call.createDataFrame().writeTo().using("iceberg") in session.mock_calls


def test_ensure_table_leaves_an_existing_table(spark):  # noqa: ARG001
    session = MagicMock()
    session.catalog.tableExists.return_value = True
    session.table.return_value.columns = BRONZE_SCHEMA.fieldNames()

    columns = ensure_table(session, "lakehouse.silver.t", BRONZE_SCHEMA, "event_time")

    session.createDataFrame.assert_not_called()
    assert columns == BRONZE_SCHEMA.fieldNames()
    assert session.sql.call_args_list == [call("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")]


def test_ensure_table_adds_new_columns_last(spark):  # noqa: ARG001
    session = MagicMock()
    session.catalog.tableExists.return_value = True
    old = [f for f in BRONZE_SCHEMA.fieldNames() if f != "squawk"]
    session.table.return_value.columns = old
    evolved = StructType(
        [*BRONZE_SCHEMA.fields, StructField("emergency", StringType())]  # v2's field
    )

    columns = ensure_table(session, "lakehouse.silver.t", evolved, "event_time")

    assert columns == [*old, "squawk", "emergency"]
    assert session.sql.call_args_list[1:] == [
        call("ALTER TABLE lakehouse.silver.t ADD COLUMN `squawk` string"),
        call("ALTER TABLE lakehouse.silver.t ADD COLUMN `emergency` string"),
    ]


def test_v1_era_bronze_rows_get_the_evolved_columns(spark):
    row = checked(spark, bronze_row()).first()
    assert row["emergency"] is None
    assert row["schema_version"] == 1  # bronze didn't record versions before v2


def test_v2_rows_keep_their_version_and_new_field(spark):
    schema = StructType(
        [
            *BRONZE_SCHEMA.fields,
            StructField("emergency", StringType()),
            StructField("schema_version", IntegerType()),
        ]
    )
    rows = [
        {**bronze_row(), "emergency": "general", "schema_version": 2},
        {**bronze_row(icao24="4d2261"), "emergency": None, "schema_version": 1},
    ]
    frame = spark.createDataFrame([tuple(r[f.name] for f in schema) for r in rows], schema)

    valid = silver.valid_positions(silver.with_reject_reasons(frame), "1 minute").collect()

    assert sorted((r["icao24"], r["emergency"], r["schema_version"]) for r in valid) == [
        ("4d2260", "general", 2),
        ("4d2261", None, 1),
    ]


def test_undecoded_rows_have_no_version(spark):
    row = checked(spark, bronze_row(icao24=None)).first()
    assert row["schema_version"] is None
