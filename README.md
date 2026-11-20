# flight-lakehouse

A real-time lakehouse for live aircraft positions from open ADS-B feeds: Kafka, Spark Structured Streaming, Apache Iceberg, Trino and dbt, all running locally in Docker Compose.

Work in progress. See [PLAN.md](PLAN.md) for the build plan and [docs/decisions/](docs/decisions/) for decision records.

## Quick start

Requires Python 3.12, [uv](https://docs.astral.sh/uv/) and Docker. `make duplication` also needs Node.

```sh
make install   # install dependencies
make test      # run unit tests
make lint      # ruff checks
make check     # every quality check: lint, SQL lint (sqlfluff), duplication (jscpd), coverage, audit
make up        # start Kafka, Apicurio, SeaweedFS, Polaris and its Postgres
make smoke     # create, write and read an Iceberg table on the stack
make up-ui     # also start the web UIs (Kafka, schema registry, SeaweedFS)
make produce   # run the producer in this terminal instead (Ctrl+C to stop)
make replay    # replay the recorded test data into Kafka
make airports  # rebuild the airports seed for the polling area (see dbt/seeds/README.md)
make stream    # run the pipeline in the background: live producer (every 4 min) + Spark bronze and silver jobs
               # (silver's flights need reference.airports: run make trino && make dbt-seed once)
make stream-logs  # follow both; make stream-stop stops them
make test-stream  # Spark tests: bronze kill-and-restart, silver end to end (slow, ~1.5 min)
make trino     # start Trino on the Iceberg catalog (catalog lakehouse, port 8080)
make test-trino   # read and write Iceberg tables through Trino
make dbt-seed     # load and test only the reference tables, such as reference.airports
make dbt-build    # load reference.airports, build the gold tables and run the dbt tests (needs make trino)
make dbt-backfill # rebuild every gold bucket silver still holds (3 days)
make maintain     # run the Iceberg maintenance once: 3-day retention, compaction, snapshot expiry
make airflow      # start Airflow (port 8090): gold every 15 min, daily maintenance, backfill on demand
make test-dags    # check the Airflow DAGs parse
make grafana      # start Grafana (port 3000) on the gold tables, through Trino
make down      # stop the stack (data is kept in Docker volumes)
```

## Data and attribution

Aircraft position data comes from [adsb.lol](https://adsb.lol) under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/). Recorded samples in this repo and tables derived from them are available under the same licence.

Airport reference data comes from [OurAirports](https://ourairports.com/data/), which is in the public domain.

## Licence

The code is licensed under the [Apache License 2.0](LICENSE). Data recorded from adsb.lol stays under ODbL, as described above.
