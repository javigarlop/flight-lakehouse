import pytest

from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.replay.replay import read_recording, replay


def gaps_s(recording) -> list[float]:
    nows = [int(r["now"]) for r in read_recording(recording)]
    return [(b - a) / 1000 for a, b in zip(nows, nows[1:], strict=False)]


def test_replay_publishes_every_response_in_order(recording, publisher):
    total = replay(recording, publisher, speed=None)
    expected = [to_positions(r) for r in read_recording(recording)]
    assert publisher.batches == expected
    assert total == sum(len(b) for b in expected)
    assert publisher.flushes == 1


def test_real_time_replay_keeps_original_gaps(recording, publisher):
    sleeps: list[float] = []
    replay(recording, publisher, speed=1.0, sleep=sleeps.append)
    assert sleeps == pytest.approx(gaps_s(recording))


def test_speed_divides_the_gaps(recording, publisher):
    sleeps: list[float] = []
    replay(recording, publisher, speed=10.0, sleep=sleeps.append)
    assert sleeps == pytest.approx([g / 10 for g in gaps_s(recording)])


def test_max_speed_never_sleeps(recording, publisher):
    sleeps: list[float] = []
    replay(recording, publisher, speed=None, sleep=sleeps.append)
    assert sleeps == []


def test_two_replays_publish_identical_records(recording):
    from tests.conftest import FakePublisher

    first, second = FakePublisher(), FakePublisher()
    replay(recording, first, speed=None)
    replay(recording, second, speed=None)
    assert first.records == second.records


def test_invalid_speed(recording, publisher):
    with pytest.raises(ValueError):
        replay(recording, publisher, speed=0)
