# Seeds: reference data

Small, rarely changing reference tables, kept as CSV so changes show up in review.
`make dbt-build` loads them as tables in the `reference` schema, such as
`lakehouse.reference.airports`, and tests them (`airports.yml`). The Spark silver job
still reads the CSV directly: the Spark containers mount this folder at `/app/dbt/seeds`.

## `airports.csv`

Airports around the polling area, used to match each flight's departure and arrival
(`src/flight_lakehouse/streaming/flights.py`).

- **Source:** [OurAirports](https://ourairports.com/data/) `airports.csv`, downloaded from
  <https://davidmegginson.github.io/ourairports-data/airports.csv>.
- **Licence:** public domain.
- **Filter:** airports within the polling radius plus 20 nm (120 nm around Barcelona
  airport by default). It keeps every large and medium airport, plus small ones with a
  4-letter ICAO code.
- **Columns:** `airport_code` is the ICAO code, or OurAirports' own id when an airport has
  none. The other columns come straight from OurAirports.

**To rebuild it** after changing the polling area (`AREA_LATITUDE`, `AREA_LONGITUDE`,
`AREA_RADIUS_NM`), or to pick up corrections from OurAirports:

```sh
make airports
```

Then review the diff and commit it. The commit date is the extract's date. A restart of
the silver job picks up the new file, and the next `make dbt-build` reloads the table.
