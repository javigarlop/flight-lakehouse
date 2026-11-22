import json

import pytest

from flight_lakehouse.producer.positions import to_positions
from flight_lakehouse.replay.replay import (
    last_response_time,
    read_recording,
    replay,
    shift_to_end_at,
    squeezer,
    time_span,
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


def test_time_span_is_first_and_last_response(recording):
    nows = [int(r["now"]) for r in read_recording(recording)]
    assert time_span(recording) == (nows[0], nows[-1])


def test_squeeze_packs_the_recording_into_the_span(recording, publisher):
    first, last = time_span(recording)
    span = (last - first) // 4
    replay(recording, publisher, speed=None, squeeze_ms=span)
    nows = [b[0]["received_at"] for b in publisher.batches]
    assert nows[-1] == last
    assert nows[-1] - nows[0] == span
    assert nows == sorted(nows)
    original = [p for r in read_recording(recording) for p in to_positions(r)]
    assert [p["icao24"] for p in publisher.records] == [p["icao24"] for p in original]


def test_squeeze_keeps_a_repeated_position_on_one_event_time(tmp_path, publisher):
    # The aircraft sent nothing new between the polls, so adsb.lol repeats its
    # position 240 s older: both copies have one event_time, which silver dedups.
    aircraft = {"hex": "abc123", "lat": 41.3, "lon": 2.1}
    responses = [
        {"now": 1_000_000, "ac": [{**aircraft, "seen_pos": 1.0}]},
        {"now": 1_240_000, "ac": [{**aircraft, "seen_pos": 241.0}]},
        {"now": 1_480_000, "ac": [{**aircraft, "seen_pos": 0.5}]},
    ]
    path = tmp_path / "repeat.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in responses))
    replay(path, publisher, speed=None, squeeze_ms=48_000, shift_ms=5_000)
    first, repeated, newer = publisher.records
    assert repeated["event_time"] == first["event_time"]
    assert newer["event_time"] > first["event_time"]
    assert [p["received_at"] for p in publisher.records] == [1_437_000, 1_461_000, 1_485_000]


def test_squeeze_leaves_a_short_recording_unchanged(recording, publisher):
    first, last = time_span(recording)
    replay(recording, publisher, speed=None, squeeze_ms=last - first)
    expected = [to_positions(r) for r in read_recording(recording)]
    assert publisher.batches == expected


def test_squeezed_real_time_replay_sleeps_the_squeezed_gaps(recording, publisher):
    sleeps: list[float] = []
    first, last = time_span(recording)
    replay(recording, publisher, speed=1.0, sleep=sleeps.append, squeeze_ms=(last - first) // 2)
    assert sum(sleeps) == pytest.approx((last - first) / 2 / 1000, abs=0.002)


def test_invalid_squeeze(recording):
    with pytest.raises(ValueError, match="span must be positive"):
        squeezer(recording, 0)
