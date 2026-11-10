.DEFAULT_GOAL := help
-include .env
COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file .env

.PHONY: help install lint format test smoke up up-ui down logs

help: ## Show available targets
	@grep -E '^[a-z-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	uv sync

lint: ## Check code style and lint rules
	uv run ruff check .
	uv run ruff format --check .

format: ## Apply formatting and safe lint fixes
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run unit tests
	uv run pytest

smoke: .env ## Run the Iceberg smoke test against the running stack
	uv run --env-file .env pytest -m integration

up: .env ## Start the local stack and wait until it is ready
	$(COMPOSE) up -d --wait kafka schema-registry seaweedfs polaris
	$(COMPOSE) run --rm polaris-init

up-ui: up ## Start the stack plus the web UIs
	$(COMPOSE) --profile ui up -d kafka-ui registry-ui
	@echo "Kafka UI:          http://localhost:$(or $(KAFKA_UI_HOST_PORT),8085)"
	@echo "Registry UI:       http://localhost:$(or $(REGISTRY_UI_HOST_PORT),8086)"
	@echo "SeaweedFS filer:   http://localhost:$(or $(SEAWEEDFS_FILER_HOST_PORT),8888)/buckets/"
	@echo "SeaweedFS master:  http://localhost:$(or $(SEAWEEDFS_MASTER_HOST_PORT),9333)"

logs: ## Follow the stack's logs
	$(COMPOSE) logs -f

down: ## Stop the local stack, including the web UIs
	$(COMPOSE) --profile ui down

.env:
	cp .env.example .env
