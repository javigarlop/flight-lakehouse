.DEFAULT_GOAL := help
COMPOSE := docker compose -f infra/compose/docker-compose.yml --env-file .env

.PHONY: help install lint format test up down

help: ## Show available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

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

up: .env ## Start the local stack
	$(COMPOSE) up -d

down: ## Stop the local stack
	$(COMPOSE) down

.env:
	cp .env.example .env
