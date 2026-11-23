# 0002: Data source

- **Status:** Accepted
- **Date:** 2026-11-10

## Context

The pipeline needs a free, live feed of aircraft positions. Because the repo is public, the data licence must also allow committing recorded samples for tests, demos and benchmarks. The plan listed three candidates: OpenSky Network, adsb.lol and Wikimedia EventStreams (a fallback in a different domain).

## Options

**adsb.lol.** Community-fed ADS-B data.
- **Licence:** the data is under ODbL, which allows use and redistribution with attribution, share-alike for derived databases. The API code is BSD-3-Clause.
- **Access:** no account is needed today. A feeder-only API key has been announced for the future.
- **Limits:** the README calls them dynamic. A test call on 2026-11-10 returned `x-ratelimit-limit: 500, 500;w=86400`, which is 500 requests per day, with a fixed daily reset.
- **Coverage:** you query by area (point plus radius), which suits a single region.
- **History:** yearly history dumps are published on GitHub under ODbL. The 2026 dump is 652 GiB.

**OpenSky Network.** A research network with a well-documented REST API.
- **Access:** OAuth2 client credentials. Basic auth is no longer supported.
- **Limits:** 4,000 API credits per day, less for anonymous use, and HTTP 429 with a retry-after header when exceeded.
- **Coverage:** global state vectors in one call.
- **Terms:** research-oriented, with limits on redistribution, so committing recorded data to a public repo is doubtful.

**Wikimedia EventStreams.** A reliable public stream, but not aviation data. None of the gold models (congestion, routes, holding patterns) would apply.

## Decision

Use **adsb.lol** as the primary source, scoped to a 100 nm radius around Barcelona airport (LEBL), with 7 days of retention (first 3, later extended). See the plan's *Scope and retention* section.

- The producer polls the area every 4 minutes (360 requests a day, under the 500 limit), reads the rate-limit headers, backs off on 429 responses and accepts an optional API key from `.env`.
- Recordings for `tests/fixtures/` and the replay tool come from the live API or the ODbL history dumps, and are committed with attribution.
- OpenSky stays an optional second producer, publishing to the same Avro schema.

## Consequences

- CI and anyone who clones the repo can run without credentials.
- Recorded data can live in the public repo legally. The README must credit adsb.lol and state that the data and derived tables are under ODbL.
- Live data is coarse: one position per aircraft every 4 minutes. Fine-grained analysis such as holding patterns, and the benchmarks, use replayed recordings and history dumps instead.
- Rate limits are not guaranteed. The replay tool keeps tests, demos and benchmarks independent of the live API.
- A single region limits the scale of live data. The benchmark uses max-speed replay instead.
- If a feeder-only key becomes mandatory, the live producer may need OpenSky or a feeder setup, but replay keeps working.

## Checked in Phase 1

- The live producer runs without an API key.
- A 100 nm query radius works, and the API returns `x-ratelimit-limit: 500;w=86400`, so 360 requests a day stays under the limit.
- The README credits adsb.lol and states the ODbL terms for recorded data and derived tables.
- Still open: whether the 500-per-day limit is per IP address. It doesn't matter at one producer.

## References

- [adsblol/api on GitHub](https://github.com/adsblol/api)
- [adsb.lol re-api docs](https://www.adsb.lol/docs/feeders-only/re-api/)
- [adsblol/globe_history_2025 releases](https://github.com/adsblol/globe_history_2025/releases)
- [OpenSky REST API docs](https://openskynetwork.github.io/opensky-api/rest.html)
- [openskynetwork/opensky-api on GitHub](https://github.com/openskynetwork/opensky-api)
