//! One binary for every Postgres integration test, so they share a single container. Needs Docker.

mod common;
mod pg_round_trip;
mod pg_to_arrow;
