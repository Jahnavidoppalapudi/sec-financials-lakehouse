.PHONY: help env setup up down ps logs ingest bronze silver facts quality gold summary asof test lint format reset clean

help:            ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

env:             ## Create .env from the example if missing
	@test -f .env || cp .env.example .env

setup: env       ## Create local virtual env with dev tools
	python3.12 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install

up: env          ## Build and start Airflow, Postgres and object storage
	docker compose up -d --build

down:            ## Stop all services (data is kept)
	docker compose down

ps:              ## Show running services
	docker compose ps

logs:            ## Follow Airflow logs
	docker compose logs -f airflow

ingest:          ## Run SEC ingestion now (silver and gold follow automatically)
	docker compose exec airflow airflow dags trigger sec_bronze_ingestion

bronze:          ## List the latest snapshots that landed in bronze
	docker compose exec airflow python -m src.inspect_lake bronze/sec/

silver:          ## Rebuild silver now (it also runs automatically after ingest)
	docker compose exec airflow airflow dags trigger sec_silver_facts

facts:           ## Summarize the silver facts table per company
	docker compose exec airflow python -m src.inspect_silver

quality:         ## Show the latest data quality report
	docker compose exec airflow python -m src.inspect_lake silver/sec/_quality/

gold:            ## Rebuild gold now (it also runs automatically after silver)
	docker compose exec airflow airflow dags trigger sec_gold_model

summary:         ## Companies, fact counts and largest restatements in gold
	docker compose exec airflow python -m src.inspect_gold

asof:            ## What was known on a date, e.g. make asof TICKER=AAPL DATE=2024-03-01
	docker compose exec airflow python -m src.asof $(or $(TICKER),AAPL) $(or $(DATE),2024-03-01)

test:            ## Run unit tests
	.venv/bin/pytest -v

lint:            ## Check code style
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

format:          ## Auto-format code
	.venv/bin/ruff format . && .venv/bin/ruff check --fix .

reset:           ## Stop everything AND delete all data volumes
	docker compose down -v

clean:           ## Remove caches
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
