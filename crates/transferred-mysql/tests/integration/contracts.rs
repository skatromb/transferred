//! Exercises MySQL-specific types and server identity against real SQL values.
#![expect(
    clippy::too_many_lines,
    clippy::type_complexity,
    reason = "Native column matrices keep database setup beside assertions"
)]

use std::error::Error as _;
use std::sync::Arc;

use arrow::array::{
    Array as _, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, RecordBatch, StringArray,
    UInt64Array,
};
use arrow::datatypes::i256;
use arrow_schema::extension::{Json, Uuid};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use mysql_async::prelude::Queryable as _;
use transferred_core::{Source as _, Transfer};
use transferred_mysql::{MySqlDestination, MySqlSource};

use crate::batch_source::BatchSource;
use crate::server::Server;

#[tokio::test]
async fn reads_native_json_and_marked_uuid_without_guessing_plain_binary() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE typed (document JSON, checked LONGTEXT COMMENT 'transferred:arrow.json:v1' CHECK(JSON_VALID(checked)), identifier BINARY(16) COMMENT 'transferred:arrow.uuid:v1', plain BINARY(16)); INSERT INTO typed VALUES ('{\"a\":1}', '{ \"a\":1, \"a\":2 }', UNHEX('00112233445566778899AABBCCDDEEFF'), UNHEX('00112233445566778899AABBCCDDEEFF'))").await.unwrap();
    let source = MySqlSource::new(&server.dsn(), "typed").unwrap();
    let batches: Vec<RecordBatch> =
        stream::iter(Box::new(source).stream_partitions().await.unwrap())
            .flatten()
            .try_collect()
            .await
            .unwrap();
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).extension_type_name(), Some("arrow.json"));
    assert_eq!(schema.field(1).extension_type_name(), Some("arrow.json"));
    assert_eq!(schema.field(2).extension_type_name(), Some("arrow.uuid"));
    assert_eq!(schema.field(3).extension_type_name(), None);
    assert_eq!(schema.field(3).data_type(), &DataType::Binary);
    let identifiers = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        identifiers.value(0),
        uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff")
            .unwrap()
            .as_bytes()
    );
    let plugin: String = conn
        .query_first("SELECT plugin FROM mysql.user WHERE User='root' AND Host='%'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plugin, "caching_sha2_password");
}

#[tokio::test]
async fn writes_checked_json_binary_uuid_and_exact_decimal() {
    let server = Server::start().await;
    let document = "{ \"a\":1, \"a\":2, \"n\":1234567890123456789012345678901234567890 }";
    let coefficient = "12345678901234567890123456789012345123456789012345678901234567890"
        .parse::<i256>()
        .unwrap();
    let identifier = uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
    let fields = vec![
        Field::new("document", DataType::Utf8, true).with_extension_type(Json::default()),
        Field::new("identifier", DataType::FixedSizeBinary(16), true).with_extension_type(Uuid),
        Field::new("amount", DataType::Decimal256(65, 30), true),
        Field::new("number", DataType::UInt64, true),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(StringArray::from(vec![document])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([identifier.as_bytes()].into_iter()).unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![coefficient])
                    .with_precision_and_scale(65, 30)
                    .unwrap(),
            ),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ],
    )
    .unwrap();
    let destination = MySqlDestination::new(&server.dsn(), "written").unwrap();
    assert_eq!(
        Transfer::new(
            Box::new(BatchSource(vec![Ok(batch)])),
            Box::new(destination)
        )
        .run()
        .await
        .unwrap()
        .rows,
        1
    );
    let mut conn = server.client().await;
    let stored: (String, String, String, u64) = conn
        .query_first("SELECT document, HEX(identifier), CAST(amount AS CHAR), number FROM written")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored,
        (
            document.to_owned(),
            "00112233445566778899AABBCCDDEEFF".to_owned(),
            "12345678901234567890123456789012345.123456789012345678901234567890".to_owned(),
            u64::MAX
        )
    );
    let physical: (String, String) = conn.query_first("SELECT DATA_TYPE, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='testdb' AND TABLE_NAME='written' AND COLUMN_NAME='document'").await.unwrap().unwrap();
    assert_eq!(
        physical,
        (
            "longtext".to_owned(),
            "transferred:arrow.json:v1".to_owned()
        )
    );
}

#[tokio::test]
async fn rejects_scale_above_thirty_before_replacement() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE unchanged (number INT); INSERT INTO unchanged VALUES (7)")
        .await
        .unwrap();
    let field = Field::new("amount", DataType::Decimal128(38, 31), true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(
            Decimal128Array::from(vec![1])
                .with_precision_and_scale(38, 31)
                .unwrap(),
        )],
    )
    .unwrap();
    let destination = MySqlDestination::new(&server.dsn(), "unchanged").unwrap();
    let error = Transfer::new(
        Box::new(BatchSource(vec![Ok(batch)])),
        Box::new(destination),
    )
    .run()
    .await
    .unwrap_err();
    assert!(
        error.source().unwrap().to_string().contains("scale"),
        "{error:?}"
    );
    let number: i32 = conn
        .query_first("SELECT number FROM unchanged")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(number, 7);
}

#[tokio::test]
async fn refuses_a_mariadb_server_before_reading() {
    let server = Server::start_image("mariadb:10.11.19", &[]).await;
    let source = MySqlSource::new(&server.dsn(), "ignored").unwrap();
    let failure = Box::new(source).stream_partitions().await.err().unwrap();
    let message = failure.source().unwrap().to_string();
    assert!(message.contains("not MariaDB"), "{message}");
}
