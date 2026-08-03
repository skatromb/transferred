//! Postgres → Arrow type mapping. One table row per supported type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::Date32Type;
use arrow_schema::{DataType as ArrowType, Field, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tokio_postgres::Column as PgColumn;
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::types::{FromSql, Type as PgType};
use transferred_core::{Result, TransferredError};

/// Builds one Arrow column from column `i` of a chunk of PG binary rows.
type PgToArrowFn = fn(&[BinaryCopyOutRow], usize) -> ArrayRef;

/// PG stores `timestamptz` as UTC; the original client offset is not retained.
const UTC: &str = "UTC";

/// Arrow schema + per-column builders, derived once from PG column metadata.
pub struct PgToArrow {
    schema: Arc<Schema>,
    pg_to_arrows: Vec<PgToArrowFn>,
}

impl PgToArrow {
    /// Derive schema and builders from a prepared statement's columns. All fields nullable.
    pub fn derive(columns: &[PgColumn]) -> Result<Self> {
        let (fields, pg_to_arrows) = columns
            .iter()
            .map(|column| {
                let (arrow, pg_to_arrow) = pg_arrow_type_and_builder(column.type_())?;
                Ok((Field::new(column.name(), arrow, true), pg_to_arrow))
            })
            .collect::<Result<(Vec<_>, Vec<_>)>>()?;

        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            pg_to_arrows,
        })
    }

    /// Build a `RecordBatch` from a chunk of PG binary rows.
    pub fn batch(&self, chunk: &[BinaryCopyOutRow]) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .pg_to_arrows
            .iter()
            .enumerate()
            .map(|(i, build)| build(chunk, i))
            .collect();

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// Defines supported Postgres types: one row per type,
/// mapping it to an Arrow type and the builder for that column.
fn pg_arrow_type_and_builder(pg: &PgType) -> Result<(ArrowType, PgToArrowFn)> {
    Ok(match *pg {
        PgType::BOOL => (ArrowType::Boolean, |rows, i| {
            Arc::new(BooleanArray::from(col::<bool>(rows, i)))
        }),
        PgType::INT2 => (ArrowType::Int16, |rows, i| {
            Arc::new(Int16Array::from(col::<i16>(rows, i)))
        }),
        PgType::INT4 => (ArrowType::Int32, |rows, i| {
            Arc::new(Int32Array::from(col::<i32>(rows, i)))
        }),
        PgType::INT8 => (ArrowType::Int64, |rows, i| {
            Arc::new(Int64Array::from(col::<i64>(rows, i)))
        }),
        PgType::FLOAT4 => (ArrowType::Float32, |rows, i| {
            Arc::new(Float32Array::from(col::<f32>(rows, i)))
        }),
        PgType::FLOAT8 => (ArrowType::Float64, |rows, i| {
            Arc::new(Float64Array::from(col::<f64>(rows, i)))
        }),
        PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME => {
            (ArrowType::Utf8, |rows, i| {
                Arc::new(StringArray::from(col::<&str>(rows, i)))
            })
        }
        PgType::BYTEA => (ArrowType::Binary, |rows, i| {
            Arc::new(BinaryArray::from(col::<&[u8]>(rows, i)))
        }),
        PgType::DATE => (ArrowType::Date32, |rows, i| {
            let days = col::<NaiveDate>(rows, i)
                .into_iter()
                .map(|date| date.map(Date32Type::from_naive_date));
            Arc::new(days.collect::<Date32Array>())
        }),
        PgType::TIMESTAMP => (
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            |rows, i| {
                let micros = col::<NaiveDateTime>(rows, i)
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.and_utc().timestamp_micros()));
                Arc::new(micros.collect::<TimestampMicrosecondArray>())
            },
        ),
        PgType::TIMESTAMPTZ => (
            ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            |rows, i| {
                let micros = col::<DateTime<Utc>>(rows, i)
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.timestamp_micros()));
                Arc::new(
                    micros
                        .collect::<TimestampMicrosecondArray>()
                        .with_timezone(UTC),
                )
            },
        ),
        ref other => {
            return Err(TransferredError::source(format!(
                "Postgres type `{}` (oid {}) not supported in 0.1",
                other.name(),
                other.oid()
            )));
        }
    })
}

/// Collect column `i` from every row, `None` for SQL NULL.
fn col<'a, T>(rows: &'a [BinaryCopyOutRow], i: usize) -> Vec<Option<T>>
where
    Option<T>: FromSql<'a>,
{
    rows.iter().map(|row| row.get(i)).collect()
}
