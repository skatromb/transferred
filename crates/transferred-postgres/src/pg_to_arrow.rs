//! Postgres → Arrow type mapping. One table row per supported type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::Date32Type;
use arrow_schema::extension::{ExtensionType, Json, Uuid};
use arrow_schema::{DataType as ArrowType, Field, IntervalUnit, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use pg_interval::Interval as PgInterval;
use rust_decimal::Decimal;
use serde_json::value::RawValue;
use tokio_postgres::Column as PgColumn;
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::types::{FromSql, Json as PgJson, Type as PgType};
use tracing::warn;
use transferred_core::{Result, TransferredError};

use crate::convert::{BARE_NUMERIC_TYPMOD, decimal_units, month_day_nano, numeric_precision_scale};

/// Builds one Arrow column from column `i` of a chunk of PG binary rows.
type PgToArrowFn = Box<dyn Fn(&[BinaryCopyOutRow], usize) -> Result<ArrayRef> + Send + Sync>;

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
            .map(pg_arrow_field_and_builder)
            .collect::<Result<(Vec<_>, Vec<_>)>>()?;

        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            pg_to_arrows,
        })
    }

    /// Build a `RecordBatch` from a chunk of PG binary rows.
    pub fn batch(&self, chunk: &[BinaryCopyOutRow]) -> Result<RecordBatch> {
        let arrays = self
            .pg_to_arrows
            .iter()
            .enumerate()
            .map(|(i, build)| build(chunk, i))
            .collect::<Result<Vec<ArrayRef>>>()?;

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// Returns Arrow field and array builder for a given PG type.
#[allow(clippy::too_many_lines)]
fn pg_arrow_field_and_builder(column: &PgColumn) -> Result<(Field, PgToArrowFn)> {
    let name = column.name();
    Ok(match *column.type_() {
        PgType::BOOL => (
            Field::new(name, ArrowType::Boolean, true),
            Box::new(|rows, i| Ok(Arc::new(BooleanArray::from(col::<bool>(rows, i))))),
        ),
        PgType::INT2 => (
            Field::new(name, ArrowType::Int16, true),
            Box::new(|rows, i| Ok(Arc::new(Int16Array::from(col::<i16>(rows, i))))),
        ),
        PgType::INT4 => (
            Field::new(name, ArrowType::Int32, true),
            Box::new(|rows, i| Ok(Arc::new(Int32Array::from(col::<i32>(rows, i))))),
        ),
        PgType::INT8 => (
            Field::new(name, ArrowType::Int64, true),
            Box::new(|rows, i| Ok(Arc::new(Int64Array::from(col::<i64>(rows, i))))),
        ),
        PgType::FLOAT4 => (
            Field::new(name, ArrowType::Float32, true),
            Box::new(|rows, i| Ok(Arc::new(Float32Array::from(col::<f32>(rows, i))))),
        ),
        PgType::FLOAT8 => (
            Field::new(name, ArrowType::Float64, true),
            Box::new(|rows, i| Ok(Arc::new(Float64Array::from(col::<f64>(rows, i))))),
        ),
        PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME => (
            Field::new(name, ArrowType::Utf8, true),
            Box::new(|rows, i| Ok(Arc::new(StringArray::from(col::<&str>(rows, i))))),
        ),
        PgType::BYTEA => (
            Field::new(name, ArrowType::Binary, true),
            Box::new(|rows, i| Ok(Arc::new(BinaryArray::from(col::<&[u8]>(rows, i))))),
        ),
        PgType::DATE => (
            Field::new(name, ArrowType::Date32, true),
            Box::new(|rows, i| {
                let days = col::<NaiveDate>(rows, i)
                    .into_iter()
                    .map(|date| date.map(Date32Type::from_naive_date));
                Ok(Arc::new(days.collect::<Date32Array>()))
            }),
        ),
        PgType::TIMESTAMP => (
            Field::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Box::new(|rows, i| {
                let micros = col::<NaiveDateTime>(rows, i)
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.and_utc().timestamp_micros()));
                Ok(Arc::new(micros.collect::<TimestampMicrosecondArray>()))
            }),
        ),
        PgType::TIMESTAMPTZ => (
            Field::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                true,
            ),
            Box::new(|rows, i| {
                let micros = col::<DateTime<Utc>>(rows, i)
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.timestamp_micros()));
                Ok(Arc::new(
                    micros
                        .collect::<TimestampMicrosecondArray>()
                        .with_timezone(UTC),
                ))
            }),
        ),
        PgType::INTERVAL => (
            Field::new(name, ArrowType::Interval(IntervalUnit::MonthDayNano), true),
            Box::new(|rows, i| {
                let intervals = col::<PgInterval>(rows, i)
                    .into_iter()
                    .map(|interval| interval.map(month_day_nano).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(IntervalMonthDayNanoArray::from(intervals)))
            }),
        ),
        PgType::NUMERIC => {
            let (precision, scale) = numeric_precision_scale(column.type_modifier())?;
            if column.type_modifier() == BARE_NUMERIC_TYPMOD {
                warn!(
                    column = name,
                    "`numeric` without declared precision; mapping to \
                     Decimal128({precision}, {scale}) and rounding beyond {scale} decimals"
                );
            }
            (
                Field::new(name, ArrowType::Decimal128(precision, scale), true),
                Box::new(move |rows, i| {
                    let units = col::<Decimal>(rows, i)
                        .into_iter()
                        .map(|decimal| decimal.map(|decimal| decimal_units(decimal, scale)))
                        .map(Option::transpose)
                        .collect::<Result<Vec<_>>>()?;

                    Ok(Arc::new(
                        Decimal128Array::from(units).with_precision_and_scale(precision, scale)?,
                    ))
                }),
            )
        }
        PgType::UUID => (
            extended_field(name, ArrowType::FixedSizeBinary(16), Uuid)?,
            Box::new(|rows, i| {
                let bytes = col::<uuid::Uuid>(rows, i)
                    .into_iter()
                    .map(|uuid| uuid.map(uuid::Uuid::into_bytes));
                Ok(Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes, 16)?,
                ))
            }),
        ),
        PgType::JSON | PgType::JSONB => (
            extended_field(name, ArrowType::Utf8, Json::default())?,
            Box::new(|rows, i| {
                let text = col::<PgJson<&RawValue>>(rows, i)
                    .into_iter()
                    .map(|json| json.map(|json| json.0.get()));
                Ok(Arc::new(text.collect::<StringArray>()))
            }),
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

/// Nullable Arrow field carrying a canonical Arrow extension type in its metadata.
fn extended_field<E: ExtensionType>(name: &str, arrow: ArrowType, extension: E) -> Result<Field> {
    let mut field = Field::new(name, arrow, true);
    field.try_with_extension_type(extension)?;
    Ok(field)
}

/// Collect column `i` from every row, `None` for SQL NULL.
fn col<'a, T>(rows: &'a [BinaryCopyOutRow], i: usize) -> Vec<Option<T>>
where
    Option<T>: FromSql<'a>,
{
    rows.iter().map(|row| row.get(i)).collect()
}
