import httpx
import pytest

from flight_lakehouse.producer.adsblol import AdsbLolClient, RateLimit, RateLimited

HEADERS = {
    "x-ratelimit-limit": "500, 500;w=86400",
    "x-ratelimit-remaining": "499",
    "x-ratelimit-reset": "43159",
}


def client_returning(status: int, body: dict | None = None, headers: dict | None = None):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, json=body or {}, headers=headers or {})

    return AdsbLolClient(transport=httpx.MockTransport(handler)), requests


def test_point_returns_body_and_rate_limit():
    client, requests = client_returning(200, {"now": 1, "ac": []}, HEADERS)
    body, rate_limit = client.point(41.297, 2.078, 100)
    assert body == {"now": 1, "ac": []}
    assert rate_limit == RateLimit(limit=500, remaining=499, reset_s=43159)
    assert requests[0].url.path == "/v2/point/41.297/2.078/100"


def test_missing_or_malformed_headers_are_none():
    assert RateLimit.from_headers(httpx.Headers({})) == RateLimit(None, None, None)
    malformed = httpx.Headers({"x-ratelimit-remaining": "lots"})
    assert RateLimit.from_headers(malformed).remaining is None


def test_429_uses_retry_after():
    client, _ = client_returning(429, headers={"retry-after": "120"})
    with pytest.raises(RateLimited) as exc:
        client.point(41.297, 2.078, 100)
    assert exc.value.retry_after_s == 120


def test_429_falls_back_to_reset():
    client, _ = client_returning(429, headers={**HEADERS, "x-ratelimit-remaining": "0"})
    with pytest.raises(RateLimited) as exc:
        client.point(41.297, 2.078, 100)
    assert exc.value.retry_after_s == 43159


def test_server_error_raises():
    client, _ = client_returning(503)
    with pytest.raises(httpx.HTTPStatusError):
        client.point(41.297, 2.078, 100)


def test_close_closes_the_http_client():
    client, _ = client_returning(200)
    client.close()
    assert client._http.is_closed
