# 0002: Data source

- **Status:** Accepted
- **Date:** 2026-11-10

## Context

The pipeline needs a free, live feed of aircraft positions. Because the repo is public, the data licence must also allow committing recorded samples for tests, demos and benchmarks. The plan listed three candidates: OpenSky Network, adsb.lol and Wikimedia EventStreams (a fallback in a different domain).

## Options

**adsb.lol.** Community-fed ADS-B data.
- **Licence:** the data is under ODbL, which allows use and redistribution with attribution, share-alike for derived databases. The API code is BSD-3-Clause.
- **Access:** no account is needed today. A feeder-only API key has been announced for the future.
- **Limits:** rate limits are dynamic and depend on load.
- **Coverage:** you query by area (point plus radius), which suits a single region.
- **History:** yearly history dumps are published on GitHub under ODbL. The 2026 dump is 652 GiB.

**OpenSky Network.** A research network with a well-documented REST API.
- **Access:** OAuth2 client credentials. Basic auth is no longer supported.
- **Limits:** 4,000 API credits per day, less for anonymous use, and HTTP 429 with a retry-after header when exceeded.
- **Coverage:** global state vectors in one call.
- **Terms:** research-oriented, with limits on redistribution, so committing recorded data to a public repo is doubtful.

**Wikimedia EventStreams.** A reliable public stream, but not aviation data. None of the gold models (congestion, routes, holding patterns) would apply.

## Decision

Use **adsb.lol** as the primary source, scoped to a 100 nm radius around Barcelona airport (LEBL), with 3 days of retention. See the plan's *Scope and retention* section.

- The producer polls one area at a modest interval (about 5 to 10 seconds), backs off on 429 responses and accepts an optional API key from `.env`.
- Recordings for `tests/fixtures/` and the replay tool come from the live API or the ODbL history dumps, and are committed with attribution.
- OpenSky stays an optional second producer, publishing to the same Avro schema.

## Consequences

- CI and anyone who clones the repo can run without credentials.
- Recorded data can live in the public repo legally. The README must credit adsb.lol and state that the data and derived tables are under ODbL.
- Rate limits are not guaranteed. The replay tool keeps tests, demos and benchmarks independent of the live API.
- A single region limits the scale of live data. The benchmark uses replay at up to 100x instead.
- If a feeder-only key becomes mandatory, the live producer may need OpenSky or a feeder setup, but replay keeps working.

## To verify before Phase 1

- The current terms and attribution wording on adsb.lol.
- The maximum query radius and the current rate-limit behaviour.
- Whether the API key requirement is in effect.

## References

- [adsblol/api on GitHub](https://github.com/adsblol/api)
- [adsb.lol re-api docs](https://www.adsb.lol/docs/feeders-only/re-api/)
- [adsblol/globe_history_2025 releases](https://github.com/adsblol/globe_history_2025/releases)
- [OpenSky REST API docs](https://openskynetwork.github.io/opensky-api/rest.html)
- [openskynetwork/opensky-api on GitHub](https://github.com/openskynetwork/opensky-api)
