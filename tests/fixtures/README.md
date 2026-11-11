# Test fixtures

`adsblol_bcn_100nm.jsonl`: three raw responses from the adsb.lol API
(`/v2/point/41.297/2.078/100`, 100 nm around Barcelona airport), recorded on
2026-11-11 about 60 seconds apart. One JSON response per line, the same format
`flight-lakehouse produce --record` writes and `flight-lakehouse replay` reads.

Data from [adsb.lol](https://adsb.lol), available under the
[Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/).
