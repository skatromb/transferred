//! Checks native SQL values independently of the destination encoder.

#![expect(
    clippy::too_many_lines,
    reason = "keeps the native type matrix beside its expected Arrow values"
)]

use std::error::Error as _;

use arrow::array::AsArray as _;
use arrow::datatypes::{
    Date32Type, DurationMicrosecondType, Float64Type, TimestampMicrosecondType,
};
use arrow::util::display::array_value_to_string;
use arrow_schema::{DataType, TimeUnit};
use mysql_async::prelude::Queryable as _;
use transferred_core::Transfer;
use transferred_mysql::{MySqlDestination, MySqlSource};

use crate::server::Server;
use crate::source::{read, try_read};

#[tokio::test]
async fn integer_and_decimal_boundaries_are_exact() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    let cases = [
        ("s8", "TINYINT", DataType::Int8, "-128", "127"),
        ("s16", "SMALLINT", DataType::Int16, "-32768", "32767"),
        ("s24", "MEDIUMINT", DataType::Int32, "-8388608", "8388607"),
        ("s32", "INT", DataType::Int32, "-2147483648", "2147483647"),
        (
            "s64",
            "BIGINT",
            DataType::Int64,
            "-9223372036854775808",
            "9223372036854775807",
        ),
        ("u8", "TINYINT UNSIGNED", DataType::UInt8, "0", "255"),
        ("u16", "SMALLINT UNSIGNED", DataType::UInt16, "0", "65535"),
        (
            "u24",
            "MEDIUMINT UNSIGNED",
            DataType::UInt32,
            "0",
            "16777215",
        ),
        ("u32", "INT UNSIGNED", DataType::UInt32, "0", "4294967295"),
        (
            "u64",
            "BIGINT UNSIGNED",
            DataType::UInt64,
            "0",
            "18446744073709551615",
        ),
        (
            "bits",
            "BIT(64)",
            DataType::UInt64,
            "0",
            "18446744073709551615",
        ),
        ("yr", "YEAR", DataType::UInt16, "0", "2155"),
        ("tiny", "BOOLEAN", DataType::Int8, "-2", "2"),
        (
            "d38",
            "DECIMAL(38,0)",
            DataType::Decimal128(38, 0),
            "-99999999999999999999999999999999999999",
            "99999999999999999999999999999999999999",
        ),
        (
            "d39",
            "DECIMAL(39,1)",
            DataType::Decimal256(39, 1),
            "-99999999999999999999999999999999999999.9",
            "99999999999999999999999999999999999999.9",
        ),
        (
            "d65",
            "DECIMAL(65,0)",
            DataType::Decimal256(65, 0),
            "-99999999999999999999999999999999999999999999999999999999999999999",
            "99999999999999999999999999999999999999999999999999999999999999999",
        ),
    ];
    let declarations = cases
        .iter()
        .map(|(name, native, ..)| format!("`{name}` {native}"))
        .collect::<Vec<_>>()
        .join(",");
    conn.query_drop(format!("CREATE TABLE numbers ({declarations})"))
        .await
        .unwrap();
    let lower = cases
        .iter()
        .map(|(_, _, _, minimum, _)| *minimum)
        .collect::<Vec<_>>()
        .join(",");
    let upper = cases
        .iter()
        .map(|(_, _, _, _, maximum)| *maximum)
        .collect::<Vec<_>>()
        .join(",");
    conn.query_drop(format!("INSERT INTO numbers VALUES ({lower}),({upper})"))
        .await
        .unwrap();
    let batches = read(&server, "numbers").await;
    let batch = &batches[0];
    for (index, (name, _, kind, minimum, maximum)) in cases.iter().enumerate() {
        assert_eq!(batch.schema().field(index).data_type(), kind, "{name}");
        assert_eq!(
            array_value_to_string(batch.column(index), 0).unwrap(),
            *minimum,
            "{name}"
        );
        assert_eq!(
            array_value_to_string(batch.column(index), 1).unwrap(),
            *maximum,
            "{name}"
        );
    }
    let copied = Transfer::new(
        Box::new(MySqlSource::new(&server.dsn(), "numbers").unwrap()),
        Box::new(MySqlDestination::new(&server.dsn(), "copied_numbers").unwrap()),
    )
    .run()
    .await
    .unwrap();
    assert_eq!(copied.rows, 2);
    assert_eq!(read(&server, "copied_numbers").await, batches);
}

#[tokio::test]
async fn preserves_text_binary_enum_set_and_generated_values() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE strings (txt LONGTEXT, bytes LONGBLOB, fixed BINARY(16), choice ENUM('Первый','Второй'), selected SET('a','b'), computed VARCHAR(100) GENERATED ALWAYS AS (CONCAT(txt,'!')) VIRTUAL, real32 FLOAT, real64 DOUBLE)").await.unwrap();
    conn.exec_drop("INSERT INTO strings (txt,bytes,fixed,choice,selected,real32,real64) VALUES (?,?,?,?,?,?,?)", ("Привет '\\ \0", vec![0_u8, 255, 128], vec![1_u8; 16], "Второй", "b,a", -1.25_f32, 1.25e200_f64)).await.unwrap();
    let batches = read(&server, "strings").await;
    let batch = &batches[0];
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "Привет '\\ \0");
    assert_eq!(batch.column(1).as_binary::<i32>().value(0), &[0, 255, 128]);
    assert_eq!(batch.column(2).as_binary::<i32>().value(0), &[1; 16]);
    assert!(batch.schema().field(2).extension_type_name().is_none());
    assert_eq!(batch.column(3).as_string::<i32>().value(0), "Второй");
    assert_eq!(batch.column(4).as_string::<i32>().value(0), "a,b");
    assert_eq!(
        batch.column(5).as_string::<i32>().value(0),
        "Привет '\\ \0!"
    );
    assert_eq!(array_value_to_string(batch.column(6), 0).unwrap(), "-1.25");
    assert_eq!(
        batch
            .column(7)
            .as_primitive::<Float64Type>()
            .value(0)
            .to_bits(),
        1.25e200_f64.to_bits()
    );
}

#[tokio::test]
async fn date_datetime_timestamp_and_duration_keep_their_semantics() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("SET SESSION time_zone='+00:00'; CREATE TABLE times (d DATE, dt DATETIME(6), ts TIMESTAMP(6) NULL, duration TIME(6)); INSERT INTO times VALUES ('0001-01-01','1969-12-31 23:59:59.999999','1970-01-01 00:00:01.000001','-838:59:59.000000'),('9999-12-31','9999-12-31 23:59:59.999999','2038-01-19 03:14:07.999999','838:59:59.000000')").await.unwrap();
    let batches = read(&server, "times").await;
    let batch = &batches[0];
    assert_eq!(
        batch.column(0).as_primitive::<Date32Type>().values(),
        &[-719_162, 2_932_896]
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<TimestampMicrosecondType>()
            .values(),
        &[-1, 253_402_300_799_999_999]
    );
    assert_eq!(
        batch
            .column(2)
            .as_primitive::<TimestampMicrosecondType>()
            .values(),
        &[1_000_001, 2_147_483_647_999_999]
    );
    assert_eq!(
        batch
            .column(3)
            .as_primitive::<DurationMicrosecondType>()
            .values(),
        &[-3_020_399_000_000, 3_020_399_000_000]
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        batch.schema().field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[tokio::test]
async fn invalid_native_dates_never_become_null_or_valid_dates() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop(
        "SET SESSION sql_mode='ALLOW_INVALID_DATES'; CREATE TABLE invalid_date (calendar DATE)",
    )
    .await
    .unwrap();
    for date in ["0000-00-00", "2020-00-01", "2020-01-00", "2023-02-31"] {
        conn.query_drop("TRUNCATE invalid_date").await.unwrap();
        conn.exec_drop("INSERT INTO invalid_date VALUES (?)", (date,))
            .await
            .unwrap();
        let error = try_read(&server, "invalid_date").await.unwrap_err();
        let message = error.source().unwrap().to_string();
        assert!(message.contains("calendar"), "{message}");
        assert!(message.contains("`testdb`.`invalid_date`"), "{message}");
    }
}

#[tokio::test]
async fn case_and_accents_do_not_combine_table_metadata() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    conn.query_drop("CREATE TABLE `Case` (upper_column INT); CREATE TABLE `case` (lower_column INT); CREATE TABLE `cáse` (accent_column INT)").await.unwrap();
    for (table, column) in [
        ("Case", "upper_column"),
        ("case", "lower_column"),
        ("cáse", "accent_column"),
    ] {
        let batches = read(&server, table).await;
        assert_eq!(batches[0].num_columns(), 1, "{table}");
        assert_eq!(batches[0].schema().field(0).name(), column, "{table}");
    }
}

#[tokio::test]
async fn unsupported_catalog_shapes_and_markers_are_refused() {
    let server = Server::start().await;
    let mut conn = server.client().await;
    for declaration in [
        "(id INT) ENGINE=MyISAM",
        "(id INT) PARTITION BY HASH(id) PARTITIONS 2",
        "(payload LONGTEXT COMMENT 'transferred:arrow.json:v1')",
        "(uid BINARY(15) COMMENT 'transferred:arrow.uuid:v1')",
        "(uid VARBINARY(16) COMMENT 'transferred:arrow.uuid:v1')",
        "(payload LONGTEXT COMMENT 'transferred:arrow.json:v1', CHECK(JSON_VALID(payload)) NOT ENFORCED)",
        "(payload LONGTEXT COMMENT 'transferred:arrow.json:v2')",
    ] {
        conn.query_drop(format!("CREATE TABLE unsupported {declaration}"))
            .await
            .unwrap();
        assert!(
            try_read(&server, "unsupported").await.is_err(),
            "{declaration}"
        );
        conn.query_drop("DROP TABLE unsupported").await.unwrap();
    }
    conn.query_drop("CREATE VIEW unsupported AS SELECT 1 AS id")
        .await
        .unwrap();
    assert!(try_read(&server, "unsupported").await.is_err());
}
