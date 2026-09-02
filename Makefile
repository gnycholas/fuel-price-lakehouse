# PySpark 4 supports Java 17 and 21. A newer JDK on PATH fails at session start,
# so the toolchain is pinned here rather than left to whatever `java` resolves to.
JAVA_HOME ?= /usr/lib/jvm/java-21-openjdk-amd64
VENV      := .venv
PY        := $(VENV)/bin/python
export JAVA_HOME

.DEFAULT_GOAL := help

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

$(VENV): pyproject.toml
	python3 -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -e '.[dev]'
	@touch $(VENV)

install: $(VENV) ## Create the virtualenv and install dependencies

lint: $(VENV) ## Check formatting and lint rules
	$(VENV)/bin/ruff format --check .
	$(VENV)/bin/ruff check .

format: $(VENV) ## Apply formatting
	$(VENV)/bin/ruff format .
	$(VENV)/bin/ruff check --fix .

typecheck: $(VENV) ## Run mypy in strict mode
	$(VENV)/bin/mypy

test: $(VENV) ## Run the test suite
	$(VENV)/bin/pytest -q

check: lint typecheck test ## Everything CI runs

CLI := $(PY) -m fuel_lakehouse.cli

pipeline: $(VENV) ## Run the whole thing: discover, download, bronze, silver, gold
	$(CLI) discover
	$(CLI) download
	$(CLI) bronze
	$(CLI) silver
	$(CLI) gold

chart: $(VENV) ## Regenerate the coverage chart in docs/img
	$(PY) scripts/coverage_chart.py

up: ## Start MinIO and create the buckets
	docker compose up -d

down: ## Stop the local stack
	docker compose down

clean: ## Remove build and cache artifacts
	rm -rf $(VENV) .mypy_cache .pytest_cache .ruff_cache spark-warehouse derby.log metastore_db
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: help install lint format typecheck test check pipeline chart up down clean
