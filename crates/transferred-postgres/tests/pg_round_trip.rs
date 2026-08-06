//! PG → PG round trip. Copies each fixture table through `Transfer` and reads both sides back,
//! so the destination is checked against the source mapping rather than hand-written SQL.
//! Needs Docker.
#![allow(clippy::expect_used)]

use transferred_core::Transfer;
use transferred_postgres::{PostgresDestination, PostgresSource};

mod common;
use common::{read_table, start_postgres};

/// Run a full transfer from `table` into `into` and return the run's row count.
async fn transfer_run(table: &str, into: &str) -> u64 {
    Transfer::new(
        Box::new(PostgresSource::new(
            start_postgres().await,
            table.to_owned(),
        )),
        Box::new(PostgresDestination::new(
            start_postgres().await,
            into.to_owned(),
        )),
    )
    .run()
    .await
    .expect("run transfer")
    .rows
}

/// Every fixture table must survive a round trip with the same schema and values.
async fn assert_round_trips(table: &str) {
    let into = format!("{table}_copy");
    let rows = transfer_run(table, &into).await;

    let original = read_table(table).await;
    assert_eq!(rows, original.num_rows() as u64);
    assert_eq!(read_table(&into).await, original);
}

#[tokio::test]
async fn primitives_round_trip() {
    assert_round_trips("it_primitives").await;
}

#[tokio::test]
async fn temporal_round_trip() {
    assert_round_trips("it_temporal").await;
}

#[tokio::test]
async fn numeric_round_trip() {
    assert_round_trips("it_numeric").await;
}

#[tokio::test]
async fn semantic_round_trip() {
    assert_round_trips("it_semantic").await;
}

/// A second run must replace the target, not append to it or trip over the existing table.
#[tokio::test]
async fn replaces_an_existing_target() {
    let into = "it_primitives_replaced";
    transfer_run("it_primitives", into).await;
    transfer_run("it_primitives", into).await;

    assert_eq!(read_table(into).await, read_table("it_primitives").await);
}

/// The staging table is an implementation detail and must not outlive a successful run.
#[tokio::test]
async fn leaves_no_staging_table_behind() {
    let into = "it_primitives_staged";
    transfer_run("it_primitives", into).await;

    let (client, connection) =
        tokio_postgres::connect(&start_postgres().await, tokio_postgres::NoTls)
            .await
            .expect("connect");
    tokio::spawn(connection);

    // Any staging name derives from the target, so a prefix match catches it whatever the suffix.
    // Scoped to this test's own target, since sibling tests stage concurrently in this container.
    let relations: Vec<String> = client
        .query(
            "select relname from pg_class where relname like $1",
            &[&format!("{into}%")],
        )
        .await
        .expect("list relations")
        .iter()
        .map(|row| row.get(0))
        .collect();

    assert_eq!(relations, vec![into.to_owned()]);
}
