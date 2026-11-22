"""Benchmark measurement, aggregation and report logic, with fakes for Kafka, Trino and Spark."""

import json
from datetime import UTC, datetime

import pytest

from flight_lakehouse.benchmark import report, run
from flight_lakehouse.benchmark.report import OffsetRange
from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.replay.replay import last_response_time, read_recording


class FakeClock:
    """perf_counter stand-in: every call moves time on by `step` seconds."""

    def __init__(self, step: float = 0.5):
        self.now, self.step = 0.0, step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


def test_offset_ranges_keep_only_partitions_that_grew():
    ranges = report.offset_ranges({0: 10, 1: 5, 2: 7}, {0: 15, 1: 5, 2: 9, 3: 2})
    assert ranges == [OffsetRange(0, 10, 15), OffsetRange(2, 7, 9), OffsetRange(3, 0, 2)]
    assert sum(r.count for r in ranges) == 9


def test_offset_ranges_refuse_a_partition_that_shrank():
    with pytest.raises(ValueError, match="went back"):
        report.offset_ranges({0: 10}, {0: 3})


def test_offset_filter_matches_each_range():
    ranges = [OffsetRange(0, 10, 15), OffsetRange(2, 7, 9)]
    assert report.offset_filter(ranges, "s") == (
        "((s.kafka_partition = 0 AND s.kafka_offset >= 10 AND s.kafka_offset < 15)"
        " OR (s.kafka_partition = 2 AND s.kafka_offset >= 7 AND s.kafka_offset < 9))"
    )
    assert report.offset_filter([]) == "FALSE"


def test_rate_needs_a_duration():
    assert report.rate(1000, 4.0) == 250.0
    assert report.rate(1000, 0) is None
    assert report.rate(1000, None) is None


def test_summarise_gives_median_min_max():
    assert report.summarise([30.0, 10.0, 20.0, 50.0]) == {
        "runs": 4,
        "median_ms": 25.0,
        "min_ms": 10.0,
        "max_ms": 50.0,
    }
    with pytest.raises(ValueError, match="no samples"):
        report.summarise([])


def test_time_queries_warms_up_then_times_each_run():
    executed = []
    clock = FakeClock(step=0.01)  # each run spans two clock reads: 10 ms
    timings = report.time_queries(executed.append, {"a": "SELECT 1", "b": "SELECT 2"}, 3, clock)
    assert executed == ["SELECT 1"] * 4 + ["SELECT 2"] * 4
    assert timings["a"]["runs"] == 3
    assert timings["a"]["median_ms"] == pytest.approx(10.0)
    with pytest.raises(ValueError, match="at least 1"):
        report.time_queries(executed.append, {}, 0, clock)


def test_change_pct():
    assert report.change_pct(200.0, 50.0) == -75.0
    assert report.change_pct(0, 5) is None
    assert report.change_pct(None, 5) is None


SETTINGS = run.Settings(recording=None, topic="t", poll_s=1, stable_polls=2, timeout_s=10)


def test_wait_for_rows_waits_for_bronze_then_a_stable_silver():
    counts = iter([(5, 0), (10, 4), (10, 8), (10, 8), (10, 8), (10, 9)])
    sleeps = []
    bronze, silver, _waited = run.wait_for_rows(
        lambda: next(counts), 10, SETTINGS, FakeClock(), sleeps.append
    )
    assert (bronze, silver) == (10, 8)
    assert sleeps == [1, 1, 1, 1]  # returned on the fifth poll, after two unchanged


def test_wait_for_rows_times_out_with_the_counts():
    with pytest.raises(TimeoutError, match="bronze has 3 of 10 rows and silver 1"):
        run.wait_for_rows(lambda: (3, 1), 10, SETTINGS, FakeClock(step=4), lambda _s: None)


def test_sql_is_scoped_to_the_run():
    ranges = [OffsetRange(1, 100, 200)]
    sql = run.silver_sql(ranges, "it's")
    assert "s.kafka_topic = 'it''s'" in sql
    assert "b.kafka_partition = 1 AND b.kafka_offset >= 100" in sql
    assert "s.processed_at - b.kafka_timestamp" in sql
    assert "ingested_at - kafka_timestamp" in run.bronze_sql(ranges, "t")
    assert run.count_sql(ranges, "t").count("kafka_offset < 200") == 2


class FakeTrino:
    """Answers the benchmark's SQL with canned rows; compaction halves the file count."""

    def __init__(self, bronze_rows: int, silver_rows: int):
        self.bronze_rows, self.silver_rows = bronze_rows, silver_rows
        self.files = {"bronze": 120, "silver": 80}
        self.sql: list[str] = []

    def compact(self) -> None:
        self.files = {name: n // 4 for name, n in self.files.items()}

    def __call__(self, sql: str) -> list[tuple]:
        self.sql.append(sql)
        if sql.startswith("SELECT (SELECT count(*)"):
            return [(self.bronze_rows, self.silver_rows)]
        if "flight_positions$files" in sql:
            return [(self.files["bronze" if "bronze." in sql else "silver"],)]
        if " JOIN " in sql:  # silver: rows, span ms, p50, p95, p99, max latency ms
            return [(self.silver_rows, 40_000, 31_000, 55_000, 58_000, 61_000)]
        if "to_milliseconds(ingested_at" in sql:
            return [(self.bronze_rows, 20_000, 12_000, 25_000, 29_000, 30_000)]
        return [("row",)]


@pytest.fixture
def benchmark(recording, publisher):
    expected = sum(len(to_positions(r)) for r in read_recording(recording))
    trino = FakeTrino(bronze_rows=expected, silver_rows=expected - 10)
    offsets = iter([{0: 0, 1: 50}, {0: expected - 20, 1: 70}])
    services = run.Services(
        publisher=publisher, end_offsets=lambda: next(offsets), execute=trino, compact=trino.compact
    )
    started = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
    settings = run.Settings(recording, "flight.positions.v1", repeats=3, poll_s=1, stable_polls=1)
    results = run.run_benchmark(
        services,
        settings,
        clock=FakeClock(),
        sleep=lambda _s: None,
        now=lambda: started,
        env={"platform": "test-os", "machine": "x", "cpus": 8, "memory_gb": 16.0},
    )
    return results, publisher, trino, expected, started


def test_run_benchmark_replays_shifted_to_now(benchmark, recording):
    results, publisher, _trino, expected, started = benchmark
    assert results["run"]["events_published"] == expected
    assert results["run"]["events_in_kafka"] == expected
    newest = max(p["received_at"] for p in publisher.records)
    assert newest == int(started.timestamp() * 1000)
    assert results["run"]["time_shift_ms"] == newest - last_response_time(recording)
    assert results["run"]["offsets"] == [
        {"partition": 0, "start": 0, "end": expected - 20},
        {"partition": 1, "start": 50, "end": 70},
    ]


def test_run_benchmark_squeezes_the_replay_inside_the_watermark(recording, publisher, tmp_path):
    # A recording spanning an hour, longer than silver's 15-minute watermark.
    lines = recording.read_text().splitlines()
    long = tmp_path / "long.jsonl"
    responses = [json.loads(line) for line in lines]
    for i, response in enumerate(responses):
        response["now"] = responses[0]["now"] + i * 1_800_000
    long.write_text("\n".join(json.dumps(r) for r in responses) + "\n")
    expected = sum(len(to_positions(r)) for r in responses)
    trino = FakeTrino(bronze_rows=expected, silver_rows=expected)
    offsets = iter([{0: 0}, {0: expected}])
    services = run.Services(
        publisher=publisher, end_offsets=lambda: next(offsets), execute=trino, compact=trino.compact
    )
    started = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
    settings = run.Settings(long, "flight.positions.v1", repeats=1, poll_s=1, stable_polls=1)
    results = run.run_benchmark(
        services, settings, clock=FakeClock(), sleep=lambda _s: None, now=lambda: started, env={}
    )
    times = [p["received_at"] for p in publisher.records]
    assert max(times) == int(started.timestamp() * 1000)
    assert max(times) - min(times) == settings.squeeze_ms
    assert results["run"]["squeeze_ms"] == settings.squeeze_ms


def test_run_benchmark_measures_the_pipeline(benchmark):
    results, _publisher, _trino, expected, _started = benchmark
    pipe = results["pipeline"]
    assert pipe["bronze_rows"] == expected
    assert pipe["bronze_throughput_eps"] == pytest.approx(expected / 20)
    assert pipe["silver_throughput_eps"] == pytest.approx((expected - 10) / 40)
    assert pipe["silver_latency_ms"] == {"p50": 31_000, "p95": 55_000, "p99": 58_000, "max": 61_000}
    assert results["run"]["publish_rate_eps"] == pytest.approx(expected / 0.5)


def test_run_benchmark_times_queries_around_compaction(benchmark):
    results, _publisher, trino, _expected, _started = benchmark
    assert results["compaction"]["files_before"] == {run.BRONZE: 120, run.SILVER: 80}
    assert results["compaction"]["files_after"] == {run.BRONZE: 30, run.SILVER: 20}
    assert set(results["queries"]["before"]) == set(run.QUERIES)
    assert results["queries"]["after"]["silver_count"]["runs"] == 3
    # Every query ran once to warm up plus three timed runs, before and after.
    assert trino.sql.count(run.QUERIES["silver_count"]) == 8


def test_results_are_written_as_json_and_markdown(benchmark, tmp_path):
    results = benchmark[0]
    json_path, md_path = report.write_results(results, tmp_path / "out")

    assert (
        json.loads(json_path.read_text())["run"]["events_published"]
        == (results["run"]["events_published"])
    )
    markdown = md_path.read_text()
    assert "test-os, 8 CPUs, 16.0 GB RAM" in markdown
    assert (
        "| Latency, Kafka to silver: p50 / p95 / p99 / max (s) | 31.00 / 55.00 / 58.00 / 61.00 |"
        in (markdown)
    )
    assert f"| `{run.BRONZE}` | 120 | 30 | -75% |" in markdown
    assert "| `silver_count` |" in markdown
    assert "n/a" not in markdown


def test_markdown_shows_missing_values_as_na(benchmark):
    results = json.loads(json.dumps(benchmark[0]))
    results["pipeline"]["silver_throughput_eps"] = None
    results["queries"]["after"] = {}
    markdown = report.render_markdown(results)
    assert "| Silver throughput (events/s) | n/a |" in markdown
    assert "| n/a | n/a |" in markdown


def test_environment_describes_the_machine():
    env = run.environment()
    assert env["cpus"] >= 1
    assert env["memory_gb"] is None or env["memory_gb"] > 0


def test_shell_command_runs_and_fails_loudly(tmp_path):
    marker = tmp_path / "ran"
    run.shell_command(f"touch {marker}")()
    assert marker.exists()
    with pytest.raises(Exception, match="returned non-zero"):
        run.shell_command("false")()


def test_kafka_end_offsets_reads_each_partition_high_watermark(monkeypatch):
    import confluent_kafka

    closed = []

    class FakeConsumer:
        def __init__(self, conf):
            assert conf["enable.auto.commit"] is False

        def list_topics(self, topic, timeout):
            partitions = {0: None, 1: None}
            return type("Meta", (), {"topics": {topic: type("T", (), {"partitions": partitions})}})

        def get_watermark_offsets(self, tp, timeout):
            return (0, 100 + tp.partition)

        def close(self):
            closed.append(True)

    monkeypatch.setattr(confluent_kafka, "Consumer", FakeConsumer)
    assert run.kafka_end_offsets("broker:9092", "t")() == {0: 100, 1: 101}
    assert closed == [True]


def test_trino_execute_fetches_every_row(monkeypatch):
    import trino

    class FakeCursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return [[1, "a"], [2, "b"]]

    connects = []
    monkeypatch.setattr(
        trino.dbapi,
        "connect",
        lambda **kwargs: (
            connects.append(kwargs) or type("C", (), {"cursor": lambda self: FakeCursor()})()
        ),
    )
    execute = run.trino_execute("localhost", 8080)
    assert execute("SELECT 1") == [(1, "a"), (2, "b")]
    assert connects == [
        {"host": "localhost", "port": 8080, "user": "benchmark", "catalog": "lakehouse"}
    ]


def test_run_benchmark_warns_when_another_producer_wrote(recording, publisher, caplog):
    expected = sum(len(to_positions(r)) for r in read_recording(recording))
    trino = FakeTrino(bronze_rows=expected + 5, silver_rows=expected)
    offsets = iter([{0: 0}, {0: expected + 5}])
    services = run.Services(publisher, lambda: next(offsets), trino, trino.compact)
    settings = run.Settings(recording, "t", repeats=1, poll_s=1, stable_polls=1)
    results = run.run_benchmark(services, settings, clock=FakeClock(), sleep=lambda _s: None)
    assert results["run"]["events_in_kafka"] == expected + 5
    assert "the topic grew by" in caplog.text
    assert results["environment"]["cpus"] >= 1
