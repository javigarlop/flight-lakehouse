.DEFAULT_GOAL := help
-include .env
COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file .env
# Keep in step with infra/airflow/Dockerfile.
AIRFLOW_VERSION := 3.3.2

.PHONY: help install lint lint-sql duplication format test cov diff-cov audit check smoke test-stream test-trino up up-ui down logs setup produce replay airports spark-build stream stream-logs stream-stop trino dbt-seed dbt-build dbt-backfill test-dags maintain airflow grafana deploy schema-demo benchmark

help: ## Show available targets
	@grep -E '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	uv sync

lint: ## Check code style and lint rules
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

lint-sql: ## Lint the dbt SQL models with sqlfluff
	uv run sqlfluff lint dbt

duplication: ## Check for copy-pasted code with jscpd (fails above 3%; needs Node)
	npx --yes jscpd@5.4.2 --config .jscpd.json --no-tips

format: ## Apply formatting and safe lint fixes
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run unit tests
	uv run pytest

cov: ## Run unit tests with a coverage report
	uv run pytest --cov

diff-cov: ## Check coverage of lines changed since main (CI requires 80% on PRs)
	uv run pytest --cov --cov-report=xml -q
	uv run diff-cover coverage.xml --compare-branch=origin/main --fail-under=80

audit: ## Check dependencies for known vulnerabilities and the repo for secrets
	uv export --format requirements-txt --no-emit-project --no-dev -q | uvx pip-audit -r /dev/stdin --disable-pip --require-hashes
	gitleaks git --no-banner .

check: lint lint-sql duplication cov audit ## Run every quality check (CI also runs Dockerfile lint)

smoke: .env ## Run the integration tests against the running stack (not Spark or Trino)
	uv run --env-file .env pytest -m "integration and not spark and not trino"

test-stream: .env ## Run the Spark streaming tests (slow; needs make up and make spark-build)
	uv run --env-file .env pytest -m spark

test-trino: .env ## Run the Trino integration tests (needs make trino)
	uv run --env-file .env pytest -m trino

test-dags: ## Check the Airflow DAGs parse (installs Airflow for this run only)
	uv run --with apache-airflow==$(AIRFLOW_VERSION) pytest tests/test_dags.py

up: .env ## Start the local stack and wait until it is ready
	$(COMPOSE) up -d --wait kafka schema-registry seaweedfs polaris
	$(COMPOSE) run --rm polaris-init
	$(MAKE) --no-print-directory setup

setup: .env ## Create the Kafka topic and register the Avro schema
	uv run --env-file .env flight-lakehouse setup

produce: .env ## Run the producer in this terminal instead of Docker (Ctrl+C to stop)
	uv run --env-file .env flight-lakehouse produce --record data/recordings/live.jsonl

replay: .env ## Replay the test recording into Kafka at max speed
	uv run --env-file .env flight-lakehouse replay tests/fixtures/adsblol_bcn_100nm.jsonl --max

airports: ## Rebuild the airports seed for the polling area from OurAirports (then commit it)
	uv run --env-file .env flight-lakehouse airports

up-ui: up ## Start the stack plus the web UIs
	$(COMPOSE) --profile ui up -d kafka-ui registry-ui
	@echo "Kafka UI:          http://localhost:$(or $(KAFKA_UI_HOST_PORT),8085)"
	@echo "Registry UI:       http://localhost:$(or $(REGISTRY_UI_HOST_PORT),8086)"
	@echo "SeaweedFS filer:   http://localhost:$(or $(SEAWEEDFS_FILER_HOST_PORT),8888)/buckets/"
	@echo "SeaweedFS master:  http://localhost:$(or $(SEAWEEDFS_MASTER_HOST_PORT),9333)"
	@echo "Spark UI:          http://localhost:$(or $(SPARK_UI_HOST_PORT),4040) bronze, :$(or $(SPARK_SILVER_UI_HOST_PORT),4041) silver (while make stream runs)"

spark-build: ## Build the producer and Spark images
	$(COMPOSE) --profile stream build producer spark-bronze

stream: up ## Start the pipeline in the background: live producer + Spark bronze and silver jobs
	$(COMPOSE) --profile stream up -d --build producer spark-bronze spark-silver
	@echo "Spark UI (bronze): http://localhost:$(or $(SPARK_UI_HOST_PORT),4040)"
	@echo "Spark UI (silver): http://localhost:$(or $(SPARK_SILVER_UI_HOST_PORT),4041)"

trino: up ## Start Trino on the stack's Iceberg catalog (about 1.5 GB of RAM)
	$(COMPOSE) --profile query up -d --wait trino
	@echo "Trino: http://localhost:$(or $(TRINO_HOST_PORT),8080) (catalog lakehouse)"

DBT := uv run --env-file .env dbt --no-version-check
DBT_DIRS := --project-dir dbt --profiles-dir dbt

dbt-seed: ## Load only the reference tables, such as reference.airports, and test them (needs make trino)
	$(DBT) build $(DBT_DIRS) --select resource_type:seed --indirect-selection cautious

dbt-build: ## Load the seeds, build the gold models and run the dbt tests (needs make trino)
	$(DBT) build $(DBT_DIRS)

dbt-backfill: ## Rebuild every gold bucket silver still holds (its 7-day retention)
	$(DBT) build $(DBT_DIRS) --vars '{lookback_hours: 168}'

MAINTAIN := $(COMPOSE) --profile stream run --rm --no-deps spark-bronze /opt/spark/bin/spark-submit \
	--master=local[2] --driver-memory=1g src/flight_lakehouse/maintenance/run.py

maintain: up ## Run the Iceberg maintenance once, without Airflow: retention, compaction, expiry
	$(MAINTAIN)

DEMO_RECORDING ?= tests/fixtures/adsblol_bcn_100nm.jsonl
REPLAY := uv run --env-file .env flight-lakehouse replay $(DEMO_RECORDING) --max --shift-to-now

schema-demo: .env ## Schema evolution on the running pipeline: v1 events, then v2 events, no restart (needs make stream)
	@echo "1. An old producer publishes v1 events"
	$(REPLAY) --schema-version 1
	@echo "2. Register v2; the registry checks it is BACKWARD compatible with v1 first"
	uv run --env-file .env flight-lakehouse setup --schema-version 2
	@echo "3. A new producer publishes v2 events, with the emergency field, to the same topic"
	$(REPLAY) --schema-version 2
	@echo "Bronze decodes both. Within a minute or two (make trino first), check with:"
	@echo "  SELECT schema_version, count(*), count(emergency) FROM lakehouse.bronze.flight_positions"
	@echo "  WHERE ingested_at > current_timestamp - INTERVAL '10' MINUTE GROUP BY 1"

BENCH_RECORDING ?= data/recordings/live.jsonl

benchmark: trino spark-build ## Max-speed replay benchmark: throughput, latency, query time around compaction (16 GB+ RAM)
	$(COMPOSE) --profile stream stop producer
	$(COMPOSE) --profile stream up -d spark-bronze spark-silver
	uv run --env-file .env flight-lakehouse benchmark --recording $(BENCH_RECORDING) \
		--compact-command "$(MAINTAIN)"

airflow: trino spark-build ## Start Airflow: gold every 15 min, daily maintenance, backfills (about 1-2 GB of RAM)
	$(COMPOSE) --profile orchestrate up -d --build --wait airflow
	@echo "Airflow: http://localhost:$(or $(AIRFLOW_HOST_PORT),8090)"

grafana: trino ## Start Grafana, with Prometheus for the streaming metrics (about 450 MB of RAM, plus Trino)
	$(COMPOSE) --profile dashboard up -d --build --wait prometheus kafka-exporter grafana
	@echo "Grafana:    http://localhost:$(or $(GRAFANA_HOST_PORT),3000)"
	@echo "Prometheus: http://localhost:$(or $(PROMETHEUS_HOST_PORT),9090)"

# Trino and the airports seed first: silver's flights need reference.airports. The gold
# models aren't built here; Airflow builds them every 15 minutes once silver has data.
deploy: trino dbt-seed stream airflow grafana ## Start or update the whole pipeline (the home machine runs this on each green main)

stream-logs: ## Follow the producer and Spark logs
	$(COMPOSE) --profile stream logs -f producer spark-bronze spark-silver

stream-stop: ## Stop the producer and Spark jobs (the rest of the stack keeps running)
	$(COMPOSE) --profile stream stop producer spark-bronze spark-silver

logs: ## Follow the stack's logs
	$(COMPOSE) logs -f

down: ## Stop the local stack, including the web UIs, Spark, Trino, Airflow and Grafana
	$(COMPOSE) --profile ui --profile stream --profile query --profile orchestrate --profile dashboard down

.env:
	cp .env.example .env
