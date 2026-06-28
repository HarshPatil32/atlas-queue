PYTHON := .venv/bin/python

.PHONY: install test lint format typecheck check migrate migration

install:
	$(PYTHON) -m pip install -e ".[dev]"

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m black .
	$(PYTHON) -m ruff format .

typecheck:
	$(PYTHON) -m mypy core

check: lint typecheck

migrate:
	$(PYTHON) -m alembic upgrade head

# Usage: make migration MSG="describe the change"
migration:
	@test -n "$(MSG)" || (echo "MSG is required"; exit 1)
	$(PYTHON) -m alembic revision --autogenerate -m "$(MSG)"
