"""Bronze decoding and job wiring, run on a local Spark in batch mode.

Needs Java 17 or later. Without it these tests are skipped locally; in CI they must run.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flight_lakehouse.streaming import bronze  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)
T0 = datetime(2026, 11, 1, 12, 0)


SCHEMAS = {1: "schema-v1", 2: "schema-v2"}


def fake_from_avro(body, schema, options):
    """Stands in for spark-avro, which isn't on the local Spark's classpath.

    A body is "<version>:<icao24>", or b"bad", which no schema decodes. Like real
    Avro, a writer schema decodes bytes written by its own version or a newer one
    (newer ones only append fields, so it stops early), but not an older one (the
    bytes run out). PERMISSIVE mode gives a struct of nulls when decoding fails.
    """
    assert options == {"mode": "PERMISSIVE", "avroSchema": SCHEMAS[2]}
    writer = int(schema.removeprefix("schema-v"))
    text = body.cast("string")
    written_by = F.split(text, ":").getItem(0).try_cast("int")
    icao24 = F.split(text, ":").getItem(1)
    return F.struct(F.when(written_by >= writer, icao24).alias("icao24"))


def kafka_rows(spark, *rows):
    return spark.createDataFrame(list(rows), KAFKA_SCHEMA)


def test_decode_splits_header_and_keeps_raw_bytes_only_on_failure(spark, monkeypatch):
    monkeypatch.setattr(bronze, "from_avro", fake_from_avro)
    kafka = kafka_rows(
        spark,
        (b"\x00\x00\x00\x00\x07" + b"1:4d2260", "flight.positions.v1", 0, 10, T0),
        (b"\x00\x00\x00\x01\x02" + b"bad", "flight.positions.v1", 1, 11, T0),
    )

    rows = {r["kafka_offset"]: r for r in bronze.decode(kafka, SCHEMAS).collect()}

    assert rows[10]["icao24"] == "4d2260"
    assert rows[10]["schema_id"] == 7
    assert rows[10]["raw_value"] is None
    assert rows[11]["icao24"] is None
    assert rows[11]["schema_id"] == 258
    assert rows[11]["schema_version"] is None
    assert rows[11]["raw_value"] == bytearray(b"\x00\x00\x00\x01\x02bad")
    assert rows[11]["kafka_partition"] == 1
    assert rows[11]["ingested_at"] is not None


def test_decode_picks_the_version_that_wrote_each_message(spark, monkeypatch):
    monkeypatch.setattr(bronze, "from_avro", fake_from_avro)
    kafka = kafka_rows(
        spark,
        (b"\x00\x00\x00\x00\x01" + b"1:aaaaaa", "t", 0, 1, T0),
        (b"\x00\x00\x00\x00\x02" + b"2:bbbbbb", "t", 0, 2, T0),
    )

    rows = {r["kafka_offset"]: r for r in bronze.decode(kafka, SCHEMAS).collect()}

    # v2 bytes would also "decode" as v1 (dropping the new field), so newest goes first.
    assert (rows[1]["icao24"], rows[1]["schema_version"]) == ("aaaaaa", 1)
    assert (rows[2]["icao24"], rows[2]["schema_version"]) == ("bbbbbb", 2)
    columns = bronze.decode(kafka, SCHEMAS).columns
    assert columns[columns.index("schema_id") + 1] == "schema_version"


def test_load_schemas_reads_every_version(tmp_path):
    (tmp_path / "flight_position.v1.avsc").write_text("one")
    (tmp_path / "flight_position.v2.avsc").write_text("two")
    (tmp_path / "other.avsc").write_text("ignored")
    assert bronze.load_schemas(tmp_path) == {1: "one", 2: "two"}


# Job wiring. Kafka and Iceberg aren't on the local Spark's classpath, so the session
# is a mock. The Docker test covers it end to end.


def option_calls(mock) -> dict[str, str]:
    return {c.args[0]: c.args[1] for c in mock.mock_calls if c[0].endswith(".option")}


@pytest.mark.parametrize(
    ("once", "trigger"),
    [(True, {"availableNow": True}), (False, {"processingTime": "5 seconds"})],
)
def test_run_reads_kafka_and_writes_iceberg(monkeypatch, tmp_path, once, trigger):
    session, rows, tables, served = MagicMock(), MagicMock(), [], []
    rows.select.return_value = rows  # aligning to the table's column order
    (tmp_path / "flight_position.v1.avsc").write_text("{}")
    monkeypatch.setattr(bronze, "SCHEMAS_DIR", tmp_path)
    monkeypatch.setattr(bronze, "build_session", lambda _name: session)
    monkeypatch.setattr(bronze, "serve_metrics", served.append)
    monkeypatch.setattr(bronze, "decode", lambda _kafka, schemas: rows)
    monkeypatch.setattr(
        bronze,
        "ensure_table",
        lambda _spark, table, _schema, day_column: tables.append((table, day_column)) or ["c"],
    )

    bronze.run("topic", "lakehouse.bronze.t", "/cp/t", once, trigger_s=5, max_offsets=100)

    session.readStream.format.assert_called_once_with("kafka")
    assert option_calls(session) == {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "topic",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "100",
    }
    assert served == [session]
    assert tables == [("lakehouse.bronze.t", "ingested_at")]
    rows.select.assert_called_once_with("c")
    rows.writeStream.format.assert_called_once_with("iceberg")
    assert option_calls(rows) == {"checkpointLocation": "/cp/t", "fanout-enabled": "true"}
    rows.writeStream.format().queryName.assert_called_once_with("lakehouse.bronze.t")
    trigger_calls = [c for c in rows.mock_calls if c[0].endswith(".trigger")]
    assert [c.kwargs for c in trigger_calls] == [trigger]
    assert any(
        c[0].endswith(".toTable") and c.args == ("lakehouse.bronze.t",) for c in rows.mock_calls
    )


def test_main_passes_arguments_to_run(monkeypatch):
    calls = []
    monkeypatch.setattr(bronze, "run", lambda *args: calls.append(args))
    monkeypatch.setattr("sys.argv", ["bronze.py", "--once", "--max-offsets=9"])
    monkeypatch.setenv("CHECKPOINT_DIR", "/cp")

    bronze.main()

    assert calls == [("flight.positions.v1", bronze.TABLE, f"/cp/{bronze.TABLE}", True, 30, 9)]
