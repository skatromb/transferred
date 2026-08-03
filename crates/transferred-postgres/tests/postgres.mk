# Postgres integration testing. Included by the main Makefile

PG_DIR := crates/transferred-postgres/tests
PG_DSN := postgres://transferred:transferred@localhost:5433/transferred
PG_COMPOSE := docker compose -f $(PG_DIR)/docker-compose.yml

# Start Postgres fixture, seed it, run the `#[ignore]`d integration tests against it.
.PHONY: pg-test
pg-test:
	@set -e; \
	trap '$(PG_COMPOSE) down' EXIT; \
	$(PG_COMPOSE) up --wait; \
	$(PG_COMPOSE) exec -T postgres psql -q -U transferred -d transferred \
		< $(PG_DIR)/pg_seed.sql; \
	TRANSFERRED_PG_DSN=$(PG_DSN) cargo test -p transferred-postgres -- --ignored
