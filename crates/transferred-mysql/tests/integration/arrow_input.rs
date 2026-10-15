//! Checks destination validation against real pre-existing target data.

#![expect(
    clippy::too_many_lines,
    reason = "keeps each Arrow input matrix beside its native database assertions"
)]

use std::collections::HashMap;
use std::error::Error as _;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BinaryViewArray, Date32Array, Decimal128Array, DurationMicrosecondArray,
    DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, NullArray, RecordBatch,
    StringArray, StringViewArray, Time32SecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::IntervalMonthDayNanoType;
use arrow_schema::{Field, Schema};
use mysql_async::prelude::Queryable as _;
use transferred_core::{Result, RunReport, Transfer};
use transferred_mysql::MySqlDestination;

use crate::batch_source::BatchSource;
use crate::server::Server;
use crate::source::read;

fn batch(column: ArrayRef, extension: Option<&str>) -> RecordBatch {
    let mut field = Field::new("offending", column.data_type().clone(), true);
    if let Some(tag) = extension {
        field.set_metadata(HashMap::from([
            ("ARROW:extension:name".to_owned(), tag.to_owned()),
            ("ARROW:extension:metadata".to_owned(), String::new()),
        ]));
    }
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).unwrap()
}

async fn write(server: &Server, batches: Vec<RecordBatch>) -> Result<RunReport> {
    Transfer::new(
        Box::new(BatchSource(batches.into_iter().map(Ok).collect())),
        Box::new(MySqlDestination::new(&server.dsn(), "target").unwrap()),
    )
    .run()
    .await
}

#[tokio::test]
async fn inexact_or_out_of_range_values_preserve_the_old_table() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (old INT); INSERT INTO target VALUES (99)")
        .await
        .unwrap();
    let invalid: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![f64::NAN])),
        Arc::new(Float32Array::from(vec![f32::INFINITY])),
        Arc::new(
            Decimal128Array::from(vec![1234])
                .with_precision_and_scale(3, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![1])
                .with_precision_and_scale(38, -1)
                .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![i32::MAX])),
        Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(vec![
            -62_167_219_200_000_000,
        ])),
        Arc::new(DurationMicrosecondArray::from(vec![3_020_400_000_000])),
        Arc::new(DurationNanosecondArray::from(vec![1])),
        Arc::new(DurationSecondArray::from(vec![i64::MAX])),
        Arc::new(Time32SecondArray::from(vec![-1])),
        Arc::new(Time64MicrosecondArray::from(vec![86_400_000_000])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(1, 0, 0),
        ])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(0, 1, 0),
        ])),
        Arc::new(NullArray::new(1)),
    ];
    for column in invalid {
        let kind = column.data_type().clone();
        let error = write(&server, vec![batch(column, None)]).await.unwrap_err();
        let message = error.source().unwrap().to_string();
        assert!(message.contains("offending"), "{kind}: {message}");
        assert!(message.contains("`testdb`.`target`"), "{kind}: {message}");
        let stored: i32 = conn
            .query_first("SELECT old FROM target")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored, 99, "{kind}");
        let artifacts: u64 = conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME LIKE '\\_transferred\\_%'").await.unwrap().unwrap();
        assert_eq!(artifacts, 0, "{kind}");
    }
}

#[tokio::test]
async fn extension_tags_are_validated_before_binary_storage() {
    let server = Server::start().await;
    let binary: ArrayRef = Arc::new(BinaryArray::from(vec![b"{}".as_slice()]));
    for extension in [
        "geoarrow.wkb",
        "arrow.opaque",
        "transferred.pg_range",
        "unknown",
        "arrow.json",
        "arrow.uuid",
    ] {
        let error = write(&server, vec![batch(binary.clone(), Some(extension))])
            .await
            .unwrap_err();
        assert!(
            error.source().unwrap().to_string().contains("offending"),
            "{extension}"
        );
    }
    let invalid_json: ArrayRef = Arc::new(StringArray::from(vec!["{invalid}"]));
    assert!(
        write(&server, vec![batch(invalid_json, Some("arrow.json"))])
            .await
            .is_err()
    );
    let invalid_uuid: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([[1_u8; 17]].into_iter()).unwrap());
    assert!(
        write(&server, vec![batch(invalid_uuid, Some("arrow.uuid"))])
            .await
            .is_err()
    );
    let count: u64 = server
        .client()
        .await
        .query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='testdb'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn schema_drift_and_schemaless_sources_preserve_existing_rows() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE target (old INT); INSERT INTO target VALUES (99)")
        .await
        .unwrap();
    let text = batch(Arc::new(StringArray::from(vec!["first"])), None);
    let bytes = batch(
        Arc::new(BinaryArray::from(vec![b"second".as_slice()])),
        None,
    );
    assert!(write(&server, vec![text, bytes]).await.is_err());
    assert!(write(&server, vec![]).await.is_err());
    assert!(
        write(
            &server,
            vec![RecordBatch::new_empty(Arc::new(Schema::empty()))]
        )
        .await
        .is_err()
    );
    let stored: i32 = conn
        .query_first("SELECT old FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, 99);
}

#[tokio::test]
async fn supported_string_and_binary_variants_keep_their_contents() {
    let server = Server::start().await;
    let text = "Привет '\\ \0";
    let bytes = [0_u8, 128, 255];
    let inputs: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![text])),
        Arc::new(LargeStringArray::from(vec![text])),
        Arc::new(StringViewArray::from(vec![text])),
        Arc::new(BinaryArray::from(vec![bytes.as_slice()])),
        Arc::new(LargeBinaryArray::from(vec![bytes.as_slice()])),
        Arc::new(BinaryViewArray::from(vec![bytes.as_slice()])),
        Arc::new(FixedSizeBinaryArray::try_from_iter([bytes].into_iter()).unwrap()),
    ];
    for (index, column) in inputs.into_iter().enumerate() {
        assert_eq!(
            write(&server, vec![batch(column, None)])
                .await
                .unwrap()
                .rows,
            1
        );
        let stored: Vec<u8> = server
            .client()
            .await
            .query_first("SELECT offending FROM target")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored,
            if index < 3 {
                text.as_bytes()
            } else {
                bytes.as_slice()
            }
        );
        assert!(
            read(&server, "target").await[0]
                .schema()
                .field(0)
                .extension_type_name()
                .is_none()
        );
    }
    let json = " {\"n\":1, \"n\":123456789012345678901234567890, \"nil\":null} ";
    assert_eq!(
        write(
            &server,
            vec![batch(
                Arc::new(LargeStringArray::from(vec![json])),
                Some("arrow.json")
            )]
        )
        .await
        .unwrap()
        .rows,
        1
    );
    let stored: String = server
        .client()
        .await
        .query_first("SELECT offending FROM target")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored, json);
}
