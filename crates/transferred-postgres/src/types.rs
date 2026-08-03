//! Postgres → Arrow type mapping. One table row per supported type.

use std::error::Error;
use std::str;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::Date32Type;
use arrow_schema::extension::{ExtensionType, Json, Uuid};
use arrow_schema::{DataType as ArrowType, Field, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tokio_postgres::Column as PgColumn;
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::types::{FromSql, Type as PgType};
use transferred_core::{Result, TransferredError};

/// Builds one Arrow column from column `i` of a chunk of PG binary rows.
type PgToArrowFn = fn(&[BinaryCopyOutRow], usize) -> Result<ArrayRef>;

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

/// Defines supported Postgres types: one row per type,
/// mapping it to an Arrow field and the builder for that column.
fn pg_arrow_field_and_builder(column: &PgColumn) -> Result<(Field, PgToArrowFn)> {
    let name = column.name();
    Ok(match *column.type_() {
        PgType::BOOL => (Field::new(name, ArrowType::Boolean, true), |rows, i| {
            Ok(Arc::new(BooleanArray::from(col::<bool>(rows, i))))
        }),
        PgType::INT2 => (Field::new(name, ArrowType::Int16, true), |rows, i| {
            Ok(Arc::new(Int16Array::from(col::<i16>(rows, i))))
        }),
        PgType::INT4 => (Field::new(name, ArrowType::Int32, true), |rows, i| {
            Ok(Arc::new(Int32Array::from(col::<i32>(rows, i))))
        }),
        PgType::INT8 => (Field::new(name, ArrowType::Int64, true), |rows, i| {
            Ok(Arc::new(Int64Array::from(col::<i64>(rows, i))))
        }),
        PgType::FLOAT4 => (Field::new(name, ArrowType::Float32, true), |rows, i| {
            Ok(Arc::new(Float32Array::from(col::<f32>(rows, i))))
        }),
        PgType::FLOAT8 => (Field::new(name, ArrowType::Float64, true), |rows, i| {
            Ok(Arc::new(Float64Array::from(col::<f64>(rows, i))))
        }),
        PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME => {
            (Field::new(name, ArrowType::Utf8, true), |rows, i| {
                Ok(Arc::new(StringArray::from(col::<&str>(rows, i))))
            })
        }
        PgType::BYTEA => (Field::new(name, ArrowType::Binary, true), |rows, i| {
            Ok(Arc::new(BinaryArray::from(col::<&[u8]>(rows, i))))
        }),
        PgType::DATE => (Field::new(name, ArrowType::Date32, true), |rows, i| {
            let days = col::<NaiveDate>(rows, i)
                .into_iter()
                .map(|date| date.map(Date32Type::from_naive_date));
            Ok(Arc::new(days.collect::<Date32Array>()))
        }),
        PgType::TIMESTAMP => (
            Field::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            |rows, i| {
                let micros = col::<NaiveDateTime>(rows, i)
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.and_utc().timestamp_micros()));
                Ok(Arc::new(micros.collect::<TimestampMicrosecondArray>()))
            },
        ),
        PgType::TIMESTAMPTZ => (
            Field::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                true,
            ),
            |rows, i| {
                let micros = col::<DateTime<Utc>>(rows, i)
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.timestamp_micros()));
                Ok(Arc::new(
                    micros
                        .collect::<TimestampMicrosecondArray>()
                        .with_timezone(UTC),
                ))
            },
        ),
        PgType::UUID => (
            extended_field(name, ArrowType::FixedSizeBinary(16), Uuid)?,
            |rows, i| {
                let bytes = col::<uuid::Uuid>(rows, i)
                    .into_iter()
                    .map(|uuid| uuid.map(uuid::Uuid::into_bytes));
                Ok(Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes, 16)?,
                ))
            },
        ),
        PgType::JSON | PgType::JSONB => (
            extended_field(name, ArrowType::Utf8, Json::default())?,
            |rows, i| {
                let text = col::<JsonText>(rows, i)
                    .into_iter()
                    .map(|json| json.map(|json| json.0));
                Ok(Arc::new(text.collect::<StringArray>()))
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

/// Borrowed JSON document; both `json` and `jsonb` ship it as text on the wire.
struct JsonText<'a>(&'a str);

impl<'a> FromSql<'a> for JsonText<'a> {
    fn from_sql(
        pg: &PgType,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn Error + Sync + Send>> {
        let text = match (*pg == PgType::JSONB).then(|| raw.split_first()) {
            None => raw,
            Some(Some((1, rest))) => rest,
            Some(_) => return Err("unsupported jsonb encoding version".into()),
        };
        Ok(Self(str::from_utf8(text)?))
    }

    fn accepts(pg: &PgType) -> bool {
        matches!(*pg, PgType::JSON | PgType::JSONB)
    }
}
