//! PG → Arrow mapping against a live Postgres seeded by `pg_seed.sql`. Run via `make pg-test`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::env;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::concat_batches;
use arrow_schema::extension::{Json, Uuid};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::{StreamExt, TryStreamExt, stream};
use transferred_core::Source;
use transferred_postgres::PostgresSource;

/// Read a whole fixture table as one `RecordBatch`.
async fn read_table(table: &str) -> RecordBatch {
    let dsn = env::var("TRANSFERRED_PG_DSN").expect("TRANSFERRED_PG_DSN not set");
    let partitions = Box::new(PostgresSource::new(dsn, table.to_owned()))
        .stream_partitions()
        .await
        .expect("stream partitions");

    // `flatten` keeps partitions sequential, so row order stays deterministic.
    let batches: Vec<RecordBatch> = stream::iter(partitions)
        .flatten()
        .try_collect()
        .await
        .expect("collect batches");

    let schema = batches.first().expect("at least one batch").schema();
    concat_batches(&schema, &batches).expect("concat batches")
}

/// Assemble the expected batch from nullable fields and their columns.
fn expected(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("build expected batch")
}

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

#[tokio::test]
#[ignore = "needs a seeded Postgres; run via `make pg-test`"]
async fn primitives() {
    let expected = expected(
        vec![
            nullable("b", DataType::Boolean),
            nullable("i2", DataType::Int16),
            nullable("i4", DataType::Int32),
            nullable("i8", DataType::Int64),
            nullable("f4", DataType::Float32),
            nullable("f8", DataType::Float64),
            nullable("t", DataType::Utf8),
            nullable("bin", DataType::Binary),
        ],
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(Int16Array::from(vec![Some(1), Some(-1), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int64Array::from(vec![Some(3), Some(-3), None])),
            Arc::new(Float32Array::from(vec![Some(1.5), Some(-1.5), None])),
            Arc::new(Float64Array::from(vec![Some(2.5), Some(-2.5), None])),
            Arc::new(StringArray::from(vec![Some("one"), Some(""), None])),
            Arc::new(BinaryArray::from(vec![
                Some(&[1u8, 2][..]),
                Some(&[][..]),
                None,
            ])),
        ],
    );

    assert_eq!(read_table("it_primitives").await, expected);
}

#[tokio::test]
#[ignore = "needs a seeded Postgres; run via `make pg-test`"]
async fn temporal() {
    // Epoch-relative days and micros, computed independently of the mapping under test.
    let days = vec![Some(19737), Some(-165), None];
    let micros = vec![Some(1_705_322_096_789_012), Some(-14_182_940_000_000), None];

    let expected = expected(
        vec![
            nullable("d", DataType::Date32),
            nullable("ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            nullable(
                "tstz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        ],
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")),
        ],
    );

    assert_eq!(read_table("it_temporal").await, expected);
}

#[tokio::test]
#[ignore = "needs a seeded Postgres; run via `make pg-test`"]
async fn semantic() {
    const A0EE: [u8; 16] = [
        0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a,
        0x11,
    ];
    let docs = vec![Some(r#"{"a": [1]}"#), Some("[]"), None];

    let expected = expected(
        vec![
            nullable("u", DataType::FixedSizeBinary(16)).with_extension_type(Uuid),
            nullable("j", DataType::Utf8).with_extension_type(Json::default()),
            nullable("jb", DataType::Utf8).with_extension_type(Json::default()),
        ],
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(A0EE), Some([0; 16]), None].into_iter(),
                    16,
                )
                .unwrap(),
            ),
            Arc::new(StringArray::from(docs.clone())),
            Arc::new(StringArray::from(docs)),
        ],
    );

    assert_eq!(read_table("it_semantic").await, expected);
}
