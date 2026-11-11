"""Minimal client for the adsb.lol v2 API, with its rate-limit headers."""

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class RateLimit:
    limit: int | None
    remaining: int | None
    reset_s: int | None

    @classmethod
    def from_headers(cls, headers: httpx.Headers) -> "RateLimit":
        def first_int(name: str) -> int | None:
            # Values look like "500, 500;w=86400"; the first number is the one in force.
            value = headers.get(name)
            if not value:
                return None
            try:
                return int(value.split(",")[0].split(";")[0].strip())
            except ValueError:
                return None

        return cls(
            limit=first_int("x-ratelimit-limit"),
            remaining=first_int("x-ratelimit-remaining"),
            reset_s=first_int("x-ratelimit-reset"),
        )


class RateLimited(Exception):
    def __init__(self, retry_after_s: float):
        super().__init__(f"rate limited, retry in {retry_after_s:.0f} s")
        self.retry_after_s = retry_after_s


class AdsbLolClient:
    def __init__(
        self,
        base_url: str = "https://api.adsb.lol",
        timeout_s: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ):
        self._http = httpx.Client(
            base_url=base_url,
            headers={"User-Agent": "flight-lakehouse"},
            timeout=timeout_s,
            transport=transport,
        )

    def point(
        self, latitude: float, longitude: float, radius_nm: int
    ) -> tuple[dict[str, Any], RateLimit]:
        """All aircraft within radius_nm of a point, plus the current rate-limit state."""
        response = self._http.get(f"/v2/point/{latitude}/{longitude}/{radius_nm}")
        rate_limit = RateLimit.from_headers(response.headers)
        if response.status_code == 429:
            retry_after = response.headers.get("retry-after")
            fallback = rate_limit.reset_s or 600
            raise RateLimited(float(retry_after) if retry_after else float(fallback))
        response.raise_for_status()
        return response.json(), rate_limit

    def close(self) -> None:
        self._http.close()
