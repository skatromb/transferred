# Postgres integration testing. Included by the main Makefile

PG_DIR := crates/transferred-postgres
PG_DSN := postgres://transferred:transferred@localhost:5433/transferred
PG_COMPOSE := docker compose -f $(PG_DIR)/docker-compose.yml

# Start Postgres fixture, seed it, run env-gated integration tests against it.
.PHONY: pg-test
pg-test: python-setup
	@set -e; \
	trap '$(PG_COMPOSE) down' EXIT; \
	$(PG_COMPOSE) up --wait; \
	$(PG_COMPOSE) exec -T postgres psql -q -U transferred -d transferred \
		< $(PG_DIR)/pg_seed.sql; \
	(cd crates/transferred-py && \
		TRANSFERRED_PG_DSN=$(PG_DSN) uv run --no-sync pytest tests/test_postgres_to_parquet.py)
