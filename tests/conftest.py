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
