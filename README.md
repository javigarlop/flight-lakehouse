# flight-lakehouse

A real-time lakehouse for live aircraft positions from open ADS-B feeds: Kafka, Spark Structured Streaming, Apache Iceberg, Trino and dbt, all running locally in Docker Compose.

Work in progress. See [PLAN.md](PLAN.md) for the build plan and [docs/decisions/](docs/decisions/) for decision records.

## Quick start

Requires Python 3.12, [uv](https://docs.astral.sh/uv/) and Docker.

```sh
make install   # install dependencies
make test      # run unit tests
make lint      # ruff checks
make up        # start Kafka, Apicurio, SeaweedFS and Polaris
make smoke     # create, write and read an Iceberg table on the stack
make up-ui     # also start the web UIs (Kafka, schema registry, SeaweedFS)
make down      # stop the stack (data is kept in Docker volumes)
```

## Data and attribution

Aircraft position data comes from [adsb.lol](https://adsb.lol) under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/). Recorded samples in this repo and tables derived from them are available under the same licence.
