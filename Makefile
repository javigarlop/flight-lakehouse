.DEFAULT_GOAL := help
-include .env
COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file .env

.PHONY: help install lint lint-sql duplication format test cov diff-cov audit check smoke test-stream test-trino up up-ui down logs setup produce replay airports spark-build stream stream-logs stream-stop trino dbt-build dbt-backfill

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

dbt-build: ## Load the seeds, build the gold models and run the dbt tests (needs make trino)
	$(DBT) build $(DBT_DIRS)

dbt-backfill: ## Rebuild every gold bucket silver still holds (its 3-day retention)
	$(DBT) build $(DBT_DIRS) --vars '{lookback_hours: 72}'

stream-logs: ## Follow the producer and Spark logs
	$(COMPOSE) --profile stream logs -f producer spark-bronze spark-silver

stream-stop: ## Stop the producer and Spark jobs (the rest of the stack keeps running)
	$(COMPOSE) --profile stream stop producer spark-bronze spark-silver

logs: ## Follow the stack's logs
	$(COMPOSE) logs -f

down: ## Stop the local stack, including the web UIs, Spark and Trino
	$(COMPOSE) --profile ui --profile stream --profile query down

.env:
	cp .env.example .env
