.PHONY: install lint mypy format test check migrate migration

install:
	uv sync --extra http --extra kafka --extra migrations

lint:
	uv run ruff check .

format:
	uv run ruff format . && uv run ruff check --fix .

mypy:
	uv run mypy

test:
	uv run pytest

check: format lint mypy test

# Alembic helpers. Set PACTIX_DATABASE_URL to a sync postgresql:// URL.
migrate:
	uv run alembic upgrade head

migration:
	uv run alembic revision -m "$(m)"
