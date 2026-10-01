//! Dogfood: in-memory batches → `FilesDestination` → `FilesSource` → in-memory collector,
//! both legs orchestrated by `Transfer`. Wide schema of round-trip-safe Arrow types.

#![cfg(test)]
#![expect(clippy::arithmetic_side_effects, reason = "tests code")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float64Array,
    Int32Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray, UInt16Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::record_batch::RecordBatch;
use arrow_schema::extension::{ExtensionType, Json, Opaque, Uuid};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tempfile::tempdir;
use transferred_core::Transfer;
use transferred_core::test_utils::{TestDestination, TestSource};
use transferred_files::{Compression, FilesDestination, FilesSource, GlobOrPaths, Parquet};

#[tokio::test]
async fn parquet_dogfood() {
    // Arrange
    let dir = tempdir().unwrap();
    let path = dir.path().join("out");
    let schema = input_schema();
    let input = vec![input_batch(&schema, 5, 0), input_batch(&schema, 3, 100)];
    let total_rows: usize = input.iter().map(RecordBatch::num_rows).sum();
    let total_rows = u64::try_from(total_rows).unwrap();
    let memory_destination = TestDestination::new();
    let collected = memory_destination.batches.clone();

    // Act
    // Dump to directory
    let write_report = Transfer::new(
        Box::new(TestSource::new(input.clone())),
        Box::new(FilesDestination::new(
            path.clone(),
            Arc::new(Parquet::new(Compression::Zstd)),
            false,
        )),
    )
    .run()
    .await
    .unwrap();

    // Read the written parts back to memory
    let parts: Vec<PathBuf> = write_report
        .written_objects
        .iter()
        .map(PathBuf::from)
        .collect();
    let read_report = Transfer::new(
        Box::new(FilesSource::new(
            GlobOrPaths::Paths(parts),
            Arc::new(Parquet::default()),
        )),
        Box::new(memory_destination),
    )
    .run()
    .await
    .unwrap();

    // Assert
    assert!(path.is_dir());
    assert_eq!(write_report.written_objects.len(), 1);
    assert_eq!(write_report.rows, total_rows);
    assert!(write_report.bytes_written > 0);
    assert_eq!(read_report.rows, total_rows);

    let read = collected.lock().unwrap();
    let read_schema = read[0].schema();
    assert_eq!(read_schema.fields(), schema.fields());

    // Names spelled out, not taken from `Uuid::NAME`: a wrong constant would agree with itself.
    // The equality above passes when metadata is absent on both sides, so assert it is present.
    assert_eq!(extension_name(&read_schema, "uuid"), Some("arrow.uuid"));
    assert_eq!(extension_name(&read_schema, "json"), Some("arrow.json"));
    assert_eq!(extension_name(&read_schema, "opaque"), Some(Opaque::NAME));
    assert_eq!(
        extension_metadata(&read_schema, "opaque"),
        Some(r#"{"type_name":"macaddr","vendor_name":"PostgreSQL"}"#)
    );

    let concat_in = arrow::compute::concat_batches(&schema, &input).unwrap();
    let concat_read = arrow::compute::concat_batches(&read_schema, read.iter()).unwrap();
    assert_eq!(concat_in, concat_read);
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, true),
        Field::new("u16", DataType::UInt16, false),
        Field::new("f64", DataType::Float64, true),
        Field::new("bool", DataType::Boolean, true),
        Field::new("utf8", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
        Field::new("date", DataType::Date32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        // Extension types live in field metadata; the round-trip must carry it through Parquet.
        Field::new("uuid", DataType::FixedSizeBinary(16), true).with_extension_type(Uuid),
        Field::new("json", DataType::Utf8, true).with_extension_type(Json::default()),
        // `uuid` and `json` serialize no metadata of their own, so only this field proves the
        // second reserved key survives — the one every CRS-carrying geo type rides in.
        Field::new("opaque", DataType::Binary, true)
            .with_extension_type(Opaque::new("macaddr", "PostgreSQL")),
    ]))
}

#[expect(clippy::too_many_lines, reason = "tests code")]
fn input_batch(schema: &Arc<Schema>, rows: u8, offset: u8) -> RecordBatch {
    let i32_arr: ArrayRef = Arc::new(Int32Array::from(
        (0..rows).map(|i| i32::from(i + offset)).collect::<Vec<_>>(),
    ));
    let i64_arr: ArrayRef = Arc::new(Int64Array::from(
        (0..rows)
            .map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Some(i64::from(i + offset))
                }
            })
            .collect::<Vec<_>>(),
    ));
    let u16_arr: ArrayRef = Arc::new(UInt16Array::from(
        (0..rows).map(u16::from).collect::<Vec<_>>(),
    ));
    let f64_arr: ArrayRef = Arc::new(Float64Array::from(
        (0..rows)
            .map(|i| {
                if i % 2 == 0 {
                    Some(f64::from(i) * 1.25)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    ));
    let bool_arr: ArrayRef = Arc::new(BooleanArray::from(
        (0..rows)
            .map(|i| match i % 3 {
                0 => Some(true),
                1 => Some(false),
                _ => None,
            })
            .collect::<Vec<_>>(),
    ));
    let utf8_arr: ArrayRef = Arc::new(StringArray::from(
        (0..rows)
            .map(|i| {
                if i % 4 == 0 {
                    None
                } else {
                    Some(format!("s{}", i + offset))
                }
            })
            .collect::<Vec<_>>(),
    ));
    let bin_arr: ArrayRef = Arc::new(
        (0..rows)
            .map(|i| (i % 2 == 0).then_some([i, i + 1, i + 2]))
            .collect::<BinaryArray>(),
    );
    let date_arr: ArrayRef = Arc::new(Date32Array::from(
        (0..rows)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(19_000 + i32::from(i))
                }
            })
            .collect::<Vec<_>>(),
    ));
    let ts_arr: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            (0..rows)
                .map(|i| Some(1_700_000_000_000_000 + i64::from(i) * 1_000_000))
                .collect::<Vec<_>>(),
        )
        .with_timezone("UTC"),
    );

    let list_values = Int32Array::from((0..i32::from(rows) * 2).collect::<Vec<_>>());
    let list_offsets = OffsetBuffer::from_lengths((0..rows).map(|_| 2usize));
    let list_field = Arc::new(Field::new("item", DataType::Int32, true));
    let list_arr: ArrayRef = Arc::new(ListArray::new(
        list_field,
        list_offsets,
        Arc::new(list_values),
        None,
    ));

    let uuid_arr: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            (0..rows).map(|i| (i % 3 != 0).then_some([i; 16])),
            16,
        )
        .unwrap(),
    );
    let json_arr: ArrayRef = Arc::new(StringArray::from(
        (0..rows)
            .map(|i| (i % 2 == 0).then(|| format!(r#"{{"i": {}}}"#, i + offset)))
            .collect::<Vec<_>>(),
    ));
    // Six macaddr bytes, as an unmapped Postgres type reaches Arrow.
    let opaque_arr: ArrayRef = Arc::new(BinaryArray::from_opt_vec(
        (0..rows)
            .map(|i| (i % 2 == 0).then_some(&b"\x08\x00\x2b\x01\x02\x03"[..]))
            .collect::<Vec<_>>(),
    ));

    RecordBatch::try_new(
        schema.clone(),
        vec![
            i32_arr, i64_arr, u16_arr, f64_arr, bool_arr, utf8_arr, bin_arr, date_arr, ts_arr,
            list_arr, uuid_arr, json_arr, opaque_arr,
        ],
    )
    .unwrap()
}

/// Canonical Arrow extension name a field carries in its metadata, if any.
fn extension_name<'schema>(schema: &'schema Schema, field: &str) -> Option<&'schema str> {
    schema.field_with_name(field).unwrap().extension_type_name()
}

/// Serialized parameters of a field's extension type, the second reserved key, if any.
fn extension_metadata<'schema>(schema: &'schema Schema, field: &str) -> Option<&'schema str> {
    schema
        .field_with_name(field)
        .unwrap()
        .extension_type_metadata()
}
