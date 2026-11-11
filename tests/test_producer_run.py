from flight_lakehouse.producer.adsblol import RateLimit, RateLimited
from flight_lakehouse.producer.run import RESERVE_REQUESTS, next_wait, run
from flight_lakehouse.replay.replay import read_recording

AREA = {"latitude": 41.297, "longitude": 2.078, "radius_nm": 100}
PLENTY = RateLimit(limit=500, remaining=400, reset_s=3600)


class FakeSource:
    """Returns scripted results in order: a response dict or an exception to raise."""

    def __init__(self, results, rate_limit=PLENTY):
        self.results = list(results)
        self.rate_limit = rate_limit
        self.calls = 0

    def point(self, latitude, longitude, radius_nm):
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result, self.rate_limit


def test_next_wait_uses_interval_while_budget_lasts():
    assert next_wait(240, PLENTY) == 240
    assert next_wait(240, RateLimit(None, None, None)) == 240


def test_next_wait_waits_for_reset_when_budget_is_low():
    low = RateLimit(limit=500, remaining=RESERVE_REQUESTS, reset_s=5000)
    assert next_wait(240, low) == 5000
    assert next_wait(240, RateLimit(500, 0, 60)) == 240


def test_run_publishes_each_poll_and_sleeps_between(recording, publisher):
    responses = list(read_recording(recording))
    sleeps: list[float] = []
    total = run(
        FakeSource(responses),
        publisher,
        **AREA,
        interval_s=240,
        polls=3,
        sleep=sleeps.append,
    )
    assert len(publisher.batches) == 3
    assert publisher.flushes == 3
    assert total == len(publisher.records) > 0
    assert sleeps == [240, 240]  # no sleep after the last poll


def test_run_records_raw_responses(recording, tmp_path):
    responses = list(read_recording(recording))
    out = tmp_path / "rec" / "out.jsonl"
    run(
        FakeSource(responses),
        None,
        **AREA,
        interval_s=1,
        record_to=out,
        polls=3,
        sleep=lambda _: None,
    )
    assert list(read_recording(out)) == responses


def test_run_backs_off_when_rate_limited(recording, publisher):
    response = next(read_recording(recording))
    source = FakeSource([RateLimited(600), response])
    sleeps: list[float] = []
    run(source, publisher, **AREA, interval_s=240, polls=1, sleep=sleeps.append)
    assert source.calls == 2
    assert sleeps == [600]
    assert len(publisher.batches) == 1
