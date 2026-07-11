# Postgres integration testing. Included by the main Makefile

PG_DSN := postgres://transferred:transferred@localhost:5433/transferred

# Start Postgres fixture, seed it, run env-gated integration tests against it.
.PHONY: pg-test
pg-test: python-setup
	@docker compose up --wait

	@docker compose exec -T postgres psql -q -U transferred -d transferred \
		< crates/transferred-py/tests/pg_seed.sql

	@cd crates/transferred-py && \
		TRANSFERRED_PG_DSN=$(PG_DSN) uv run --no-sync pytest tests/test_postgres_to_parquet.py

	@docker compose down
