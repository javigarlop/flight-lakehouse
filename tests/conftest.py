import os
import time
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def recording() -> Path:
    """Three real adsb.lol responses for 100 nm around Barcelona airport, 60 s apart."""
    return FIXTURES / "adsblol_bcn_100nm.jsonl"


class FakePublisher:
    """Collects what would be sent to Kafka."""

    def __init__(self):
        self.batches: list[list[dict]] = []
        self.flushes = 0

    def publish(self, positions) -> int:
        batch = list(positions)
        self.batches.append(batch)
        return len(batch)

    def flush(self) -> None:
        self.flushes += 1

    @property
    def records(self) -> list[dict]:
        return [r for batch in self.batches for r in batch]


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


@pytest.fixture(scope="session")
def spark():
    """A local Spark session for batch-mode tests of the streaming transforms.

    Needs Java 17 or later. Without it the test is skipped locally; in CI it must run.
    Everything runs in UTC (Python, the JVM and Spark SQL), so naive datetimes in tests
    mean UTC and round-trip unchanged.
    """
    from pyspark.sql import SparkSession

    os.environ["TZ"] = "UTC"
    time.tzset()
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:  # noqa: BLE001 - no usable Java, most likely
        if os.environ.get("CI"):
            raise
        pytest.skip(f"local Spark unavailable (needs Java 17+): {exc}")
    yield session
    session.stop()
