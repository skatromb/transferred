//! PG → Arrow mapping against a throwaway Postgres container seeded by `pg_seed.sql`. Needs Docker.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::IntervalMonthDayNano;
use arrow_schema::extension::{Json, Uuid};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};

mod common;
use common::read_table;

/// Assemble the expected batch from nullable fields and their columns.
fn expected(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("build expected batch")
}

fn nullable(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

#[tokio::test]
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
async fn temporal() {
    // Epoch-relative days and micros, computed independently of the mapping under test.
    let days = vec![Some(19737), Some(-165), None];
    let micros = vec![Some(1_705_322_096_789_012), Some(-14_182_940_000_000), None];

    // Months, days and micros stay separate — PG carries all three independently.
    let intervals = vec![
        Some(IntervalMonthDayNano::new(14, 3, 14_706_789_000_000)),
        Some(IntervalMonthDayNano::new(-1, -2, -10_800_000_000_000)),
        None,
    ];

    let expected = expected(
        vec![
            nullable("d", DataType::Date32),
            nullable("ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            nullable(
                "tstz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            nullable("iv", DataType::Interval(IntervalUnit::MonthDayNano)),
        ],
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")),
            Arc::new(IntervalMonthDayNanoArray::from(intervals)),
        ],
    );

    assert_eq!(read_table("it_temporal").await, expected);
}

#[tokio::test]
async fn numeric() {
    // Arrow stores decimals as integer counts of 10^-scale units.
    let expected = expected(
        vec![
            nullable("n", DataType::Decimal128(38, 9)),
            nullable("small", DataType::Decimal128(28, 4)),
            nullable("wide", DataType::Decimal128(38, 9)),
        ],
        vec![
            // Row 3 arrives at scale 10, so bare `n` is the one column the mapping rounds itself.
            Arc::new(
                Decimal128Array::from(vec![
                    Some(1_500_000_000),
                    Some(-1_234_567_890_123_456_789_123_456_789),
                    Some(123_456_789),
                    None,
                ])
                .with_precision_and_scale(38, 9)
                .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(15_000),
                    Some(-12_345_678_901_234_567_891_235),
                    Some(1_235),
                    None,
                ])
                .with_precision_and_scale(28, 4)
                .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(1_500_000_000),
                    Some(-1_234_567_890_123_456_789_123_456_789),
                    Some(123_456_789),
                    None,
                ])
                .with_precision_and_scale(38, 9)
                .unwrap(),
            ),
        ],
    );

    assert_eq!(read_table("it_numeric").await, expected);
}

#[tokio::test]
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
