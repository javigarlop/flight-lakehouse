import pytest

from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.replay.replay import (
    last_response_time,
    read_recording,
    replay,
    shift_to_end_at,
)


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
    with pytest.raises(ValueError, match="speed must be positive"):
        replay(recording, publisher, speed=0)


def test_blank_lines_are_skipped(recording, tmp_path):
    lines = recording.read_text().splitlines()
    padded = tmp_path / "padded.jsonl"
    padded.write_text("\n".join([lines[0], "", "   ", *lines[1:], ""]) + "\n")
    assert list(read_recording(padded)) == list(read_recording(recording))


def test_shift_moves_every_time_and_keeps_the_gaps(recording, publisher):
    end = last_response_time(recording)
    shift = shift_to_end_at(recording, end + 3_600_000)
    assert shift == 3_600_000
    replay(recording, publisher, speed=None, shift_ms=shift)
    original = [p for r in read_recording(recording) for p in to_positions(r)]
    assert len(publisher.records) == len(original)
    for shifted, p in zip(publisher.records, original, strict=True):
        assert shifted["event_time"] == p["event_time"] + shift
        assert shifted["received_at"] == p["received_at"] + shift
        assert shifted["icao24"] == p["icao24"]
    assert max(p["received_at"] for p in publisher.records) == end + shift


def test_last_response_time_of_an_empty_recording_fails(tmp_path):
    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n")
    with pytest.raises(ValueError, match="no responses"):
        last_response_time(empty)
