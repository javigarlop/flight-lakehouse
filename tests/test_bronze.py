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


def fake_from_avro(body, _schema, options):
    """Stands in for spark-avro, which isn't on the local Spark's classpath.

    The "decoded" icao24 is the body as text, or null for a body of b"bad", as
    PERMISSIVE mode gives for a message that doesn't decode.
    """
    assert options == {"mode": "PERMISSIVE"}
    text = body.cast("string")
    return F.struct(F.when(text != "bad", text).alias("icao24"))


def test_decode_splits_header_and_keeps_raw_bytes_only_on_failure(spark, monkeypatch):
    monkeypatch.setattr(bronze, "from_avro", fake_from_avro)
    kafka = spark.createDataFrame(
        [
            (b"\x00\x00\x00\x00\x07" + b"4d2260", "flight.positions.v1", 0, 10, T0),
            (b"\x00\x00\x00\x01\x02" + b"bad", "flight.positions.v1", 1, 11, T0),
        ],
        KAFKA_SCHEMA,
    )

    rows = {r["kafka_offset"]: r for r in bronze.decode(kafka, "{}").collect()}

    assert rows[10]["icao24"] == "4d2260"
    assert rows[10]["schema_id"] == 7
    assert rows[10]["raw_value"] is None
    assert rows[11]["icao24"] is None
    assert rows[11]["schema_id"] == 258
    assert rows[11]["raw_value"] == bytearray(b"\x00\x00\x00\x01\x02bad")
    assert rows[11]["kafka_partition"] == 1
    assert rows[11]["ingested_at"] is not None


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
    schema = tmp_path / "schema.avsc"
    schema.write_text("{}")
    monkeypatch.setattr(bronze, "SCHEMA_PATH", schema)
    monkeypatch.setattr(bronze, "build_session", lambda _name: session)
    monkeypatch.setattr(bronze, "serve_metrics", served.append)
    monkeypatch.setattr(bronze, "decode", lambda _kafka, _schema: rows)
    monkeypatch.setattr(
        bronze,
        "ensure_table",
        lambda _spark, table, _schema, day_column: tables.append((table, day_column)),
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
