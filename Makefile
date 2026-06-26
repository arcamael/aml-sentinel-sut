.PHONY: up down logs topics ps clean install lint test migrate migrate-down seed \
        normalizer screening-worker decision-engine gen-golden gen-watchlists \
        gen-matching gen-decisions verify-phase3 verify-phase4 verify-phase5 verify-phase6

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
	ruff check src tests tools scripts
	ruff format --check src tests tools scripts

# ── Workers / data generation (Phase 3) ──────────────────────────────────────

normalizer:
	PYTHONPATH=src python -m aml_sentinel.workers.normalizer

screening-worker:
	PYTHONPATH=src python -m aml_sentinel.workers.screening

decision-engine:
	PYTHONPATH=src python -m aml_sentinel.workers.decision

gen-golden:
	PYTHONPATH=src python -m tools.datagen golden --seed 42 --out data/golden/

gen-watchlists:
	PYTHONPATH=src python -m tools.datagen watchlists --seed 42 --out data/watchlists/

gen-matching:
	PYTHONPATH=src python -m tools.datagen golden --set matching --out data/golden/

gen-decisions:
	PYTHONPATH=src python -m tools.datagen golden --set decisions --out data/golden/

verify-phase3:
	PYTHONPATH=src python scripts/verify_phase3.py

verify-phase4:
	PYTHONPATH=src:. python scripts/verify_phase4.py

verify-phase5:
	PYTHONPATH=src:. python scripts/verify_phase5.py

verify-phase6:
	PYTHONPATH=src:. python scripts/verify_phase6.py

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
