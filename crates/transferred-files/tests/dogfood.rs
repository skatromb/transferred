//! Dogfood: in-memory batches → `FilesDestination` → `FilesSource` → in-memory collector,
//! both legs orchestrated by `Transfer`. Wide schema of round-trip-safe Arrow types.

#![cfg(test)]
#![expect(clippy::arithmetic_side_effects, reason = "tests code")]

use std::iter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float64Array,
    Int32Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray, UInt16Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow_schema::extension::{ExtensionType as _, Json, Opaque, Uuid};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use futures::{StreamExt as _, stream};
use tempfile::tempdir;
use transferred_core::{BatchStream, Destination, Result, RunReport, Source, Transfer};
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

    // Act
    let write_report = write_parquet(input.clone(), &path).await;
    let (read_report, read) = read_parquet(&write_report).await;

    // Assert
    assert!(path.is_dir());
    assert_eq!(write_report.written_objects.len(), 1);
    assert_eq!(write_report.rows, total_rows);
    assert!(write_report.bytes_written > 0);
    assert_eq!(read_report.rows, total_rows);

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

    let concat_in = compute::concat_batches(&schema, &input).unwrap();
    let concat_read = compute::concat_batches(&read_schema, read.iter()).unwrap();
    assert_eq!(concat_in, concat_read);
}

/// Dumps in-memory batches to zstd Parquet parts under `path`.
async fn write_parquet(input: Vec<RecordBatch>, path: &Path) -> RunReport {
    Transfer::new(
        Box::new(MemorySource(input)),
        Box::new(FilesDestination::new(
            path.to_path_buf(),
            Arc::new(Parquet::new(Compression::Zstd)),
            false,
        )),
    )
    .run()
    .await
    .unwrap()
}

/// Reads the parts a write reported back into memory, alongside the read's own report.
async fn read_parquet(written: &RunReport) -> (RunReport, Vec<RecordBatch>) {
    let parts = written.written_objects.iter().map(PathBuf::from).collect();
    let (sender, receiver) = mpsc::channel();

    let report = Transfer::new(
        Box::new(FilesSource::new(
            GlobOrPaths::Paths(parts),
            Arc::new(Parquet::default()),
        )),
        Box::new(MemoryDestination(sender)),
    )
    .run()
    .await
    .unwrap();

    (report, receiver.iter().collect())
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

fn input_batch(schema: &Arc<Schema>, rows: u8, offset: u8) -> RecordBatch {
    let columns = [plain_columns(rows, offset), extension_columns(rows, offset)].concat();
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

/// Columns of the schema's built-in Arrow types, nulls sprinkled at different strides.
fn plain_columns(rows: u8, offset: u8) -> Vec<ArrayRef> {
    let i32_arr = Int32Array::from_iter_values((0..rows).map(|i| i32::from(i + offset)));
    let i64_arr: Int64Array = (0..rows)
        .map(|i| (i % 3 != 0).then(|| i64::from(i + offset)))
        .collect();
    let u16_arr = UInt16Array::from_iter_values((0..rows).map(u16::from));
    let f64_arr: Float64Array = (0..rows)
        .map(|i| (i % 2 == 0).then(|| f64::from(i) * 1.25))
        .collect();
    let bool_arr: BooleanArray = (0..rows)
        .map(|i| (i % 3 != 2).then_some(i % 3 == 0))
        .collect();
    let utf8_arr: StringArray = (0..rows)
        .map(|i| (i % 4 != 0).then(|| format!("s{}", i + offset)))
        .collect();
    let bin_arr: BinaryArray = (0..rows)
        .map(|i| (i % 2 == 0).then_some([i, i + 1, i + 2]))
        .collect();
    let date_arr: Date32Array = (0..rows)
        .map(|i| (i % 5 != 0).then(|| 19_000 + i32::from(i)))
        .collect();
    let ts_arr = TimestampMicrosecondArray::from_iter_values(
        (0..rows).map(|i| 1_700_000_000_000_000 + i64::from(i) * 1_000_000),
    )
    .with_timezone("UTC");
    let list_arr = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::from_lengths(iter::repeat_n(2, rows.into())),
        Arc::new(Int32Array::from_iter_values(0..i32::from(rows) * 2)),
        None,
    );

    vec![
        Arc::new(i32_arr),
        Arc::new(i64_arr),
        Arc::new(u16_arr),
        Arc::new(f64_arr),
        Arc::new(bool_arr),
        Arc::new(utf8_arr),
        Arc::new(bin_arr),
        Arc::new(date_arr),
        Arc::new(ts_arr),
        Arc::new(list_arr),
    ]
}

/// Columns whose type lives in field metadata, which the Parquet round-trip must carry through.
fn extension_columns(rows: u8, offset: u8) -> Vec<ArrayRef> {
    let uuid_arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        (0..rows).map(|i| (i % 3 != 0).then_some([i; 16])),
        16,
    )
    .unwrap();
    let json_arr: StringArray = (0..rows)
        .map(|i| (i % 2 == 0).then(|| format!(r#"{{"i": {}}}"#, i + offset)))
        .collect();
    // Six macaddr bytes, as an unmapped Postgres type reaches Arrow.
    let opaque_arr: BinaryArray = (0..rows)
        .map(|i| (i % 2 == 0).then_some(b"\x08\x00\x2b\x01\x02\x03"))
        .collect();

    vec![Arc::new(uuid_arr), Arc::new(json_arr), Arc::new(opaque_arr)]
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

/// Yields fixed batches as a single partition.
struct MemorySource(Vec<RecordBatch>);

#[async_trait]
impl Source for MemorySource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let stream = stream::iter(self.0.into_iter().map(Ok));
        Ok(vec![Box::pin(stream)])
    }
}

/// Sends every batch down a channel; the run drops the sender, which ends the receiver's iterator.
struct MemoryDestination(mpsc::Sender<RecordBatch>);

#[async_trait]
impl Destination for MemoryDestination {
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport> {
        let mut rows: u64 = 0;
        for mut partition in partitions {
            while let Some(batch) = partition.next().await {
                let batch = batch?;
                rows += u64::try_from(batch.num_rows()).expect("row count fits u64");
                self.0.send(batch).expect("receiver outlives the run");
            }
        }
        Ok(RunReport {
            rows,
            bytes_written: 0,
            written_objects: vec![],
            duration: Duration::ZERO,
            coercions: vec![],
        })
    }
}
