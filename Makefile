.PHONY: up down logs topics ps clean install lint test migrate migrate-down seed

# ── Infrastructure ──────────────────────────────────────────────────────────

up:
	docker compose up -d --wait
	@echo "✓ Stack is up. Run 'make ps' to check health."

down:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

topics:
	docker compose run --rm redpanda-init

# ── Development ──────────────────────────────────────────────────────────────

install:
	pip install -e ".[dev]"

# ── Database ─────────────────────────────────────────────────────────────────

migrate:
	PYTHONPATH=src alembic upgrade head

migrate-down:
	PYTHONPATH=src alembic downgrade base

seed:
	PYTHONPATH=src python -m aml_sentinel.db.seed_smoke

lint:
	ruff check src tests
	ruff format --check src tests

test:
	pytest -q

test-cov:
	pytest --cov=src/aml_sentinel --cov-report=term-missing -q

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	docker compose down -v --remove-orphans
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
