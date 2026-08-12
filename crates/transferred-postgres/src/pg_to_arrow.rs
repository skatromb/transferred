//! Postgres → Arrow type mapping. One table row per supported type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::Date32Type;
use arrow_schema::extension::{ExtensionType, Json, Opaque, Uuid};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, IntervalUnit, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use pg_interval::Interval as PgInterval;
use rust_decimal::Decimal;
use serde_json::value::RawValue;
use tokio_postgres::Column as PgColumn;
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::types::{FromSql, Json as PgJson, Kind, Type as PgType};
use tracing::warn;
use transferred_core::{Result, TransferredError};

use crate::convert::{
    BARE_NUMERIC_TYPMOD, CITEXT, GEOGRAPHY, GEOMETRY, decimal_units, geo_srid, month_day_nano,
    numeric_precision_scale,
};
use crate::geoarrow::Wkb;

/// Builds one Arrow column from column `i` of a chunk of PG binary rows.
type PgToArrowFn = Box<dyn Fn(&[BinaryCopyOutRow], usize) -> Result<ArrayRef> + Send + Sync>;

/// PG stores `timestamptz` as UTC; the original client offset is not retained.
const UTC: &str = "UTC";

/// The `arrow.opaque` fallback's `vendor_name`: the system an unmapped type came from.
const VENDOR: &str = "PostgreSQL";

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
fn pg_arrow_field_and_builder(column: &PgColumn) -> Result<(ArrowField, PgToArrowFn)> {
    let name = column.name();
    Ok(match *column.type_() {
        PgType::BOOL => (
            ArrowField::new(name, ArrowType::Boolean, true),
            Box::new(|rows, i| Ok(Arc::new(BooleanArray::from(col::<bool>(rows, i)?)))),
        ),
        PgType::INT2 => (
            ArrowField::new(name, ArrowType::Int16, true),
            Box::new(|rows, i| Ok(Arc::new(Int16Array::from(col::<i16>(rows, i)?)))),
        ),
        PgType::INT4 => (
            ArrowField::new(name, ArrowType::Int32, true),
            Box::new(|rows, i| Ok(Arc::new(Int32Array::from(col::<i32>(rows, i)?)))),
        ),
        PgType::INT8 => (
            ArrowField::new(name, ArrowType::Int64, true),
            Box::new(|rows, i| Ok(Arc::new(Int64Array::from(col::<i64>(rows, i)?)))),
        ),
        PgType::FLOAT4 => (
            ArrowField::new(name, ArrowType::Float32, true),
            Box::new(|rows, i| Ok(Arc::new(Float32Array::from(col::<f32>(rows, i)?)))),
        ),
        PgType::FLOAT8 => (
            ArrowField::new(name, ArrowType::Float64, true),
            Box::new(|rows, i| Ok(Arc::new(Float64Array::from(col::<f64>(rows, i)?)))),
        ),
        PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME => (
            ArrowField::new(name, ArrowType::Utf8, true),
            Box::new(|rows, i| Ok(Arc::new(StringArray::from(col::<&str>(rows, i)?)))),
        ),
        PgType::BYTEA => (
            ArrowField::new(name, ArrowType::Binary, true),
            Box::new(|rows, i| Ok(Arc::new(BinaryArray::from(col::<&[u8]>(rows, i)?)))),
        ),
        PgType::DATE => (
            ArrowField::new(name, ArrowType::Date32, true),
            Box::new(|rows, i| {
                let days = col::<NaiveDate>(rows, i)?
                    .into_iter()
                    .map(|date| date.map(Date32Type::from_naive_date));
                Ok(Arc::new(days.collect::<Date32Array>()))
            }),
        ),
        PgType::TIMESTAMP => (
            ArrowField::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Box::new(|rows, i| {
                let micros = col::<NaiveDateTime>(rows, i)?
                    .into_iter()
                    .map(|ts| ts.map(|ts| ts.and_utc().timestamp_micros()));
                Ok(Arc::new(micros.collect::<TimestampMicrosecondArray>()))
            }),
        ),
        PgType::TIMESTAMPTZ => (
            ArrowField::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                true,
            ),
            Box::new(|rows, i| {
                let micros = col::<DateTime<Utc>>(rows, i)?
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
            ArrowField::new(name, ArrowType::Interval(IntervalUnit::MonthDayNano), true),
            Box::new(|rows, i| {
                let intervals = col::<PgInterval>(rows, i)?
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
                    target: "postgres::source",
                    column = name,
                    "`numeric` without declared precision; mapping to \
                     Decimal128({precision}, {scale}) and rounding beyond {scale} decimals"
                );
            }
            (
                ArrowField::new(name, ArrowType::Decimal128(precision, scale), true),
                Box::new(move |rows, i| {
                    let units = col::<Decimal>(rows, i)?
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
                let bytes = col::<uuid::Uuid>(rows, i)?
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
                let text = col::<PgJson<&RawValue>>(rows, i)?
                    .into_iter()
                    .map(|json| json.map(|json| json.0.get()));
                Ok(Arc::new(text.collect::<StringArray>()))
            }),
        ),
        // Both go on the wire as their own UTF-8 text. `citext` has no fixed OID, so goes by name.
        ref text if matches!(text.kind(), Kind::Enum(_)) || text.name() == CITEXT => (
            ArrowField::new(name, ArrowType::Utf8, true),
            Box::new(|rows, i| {
                let strings = col::<RawText>(rows, i)?
                    .into_iter()
                    .map(|raw| raw.map(|raw| raw.text));
                Ok(Arc::new(strings.collect::<StringArray>()))
            }),
        ),
        // `PostGIS` carries no fixed OID, so its types answer to a name. Their wire form is EWKB,
        // which `geoarrow.wkb` accepts as-is, SRID per value and all.
        ref geo if geo.name() == GEOMETRY => (
            extended_field(
                name,
                ArrowType::Binary,
                Wkb::planar(geo_srid(column.type_modifier())),
            )?,
            Box::new(wire_bytes),
        ),
        // The same bytes, but `geography` bends its edges around the globe.
        ref geo if geo.name() == GEOGRAPHY => (
            extended_field(
                name,
                ArrowType::Binary,
                Wkb::spherical(geo_srid(column.type_modifier())),
            )?,
            Box::new(wire_bytes),
        ),
        // No mapping: keep the column transferable as self-describing bytes.
        ref other => {
            warn!(
                target: "postgres::source",
                column = name,
                "no Arrow mapping for Postgres type `{}` (oid {}); \
                 passing its wire bytes through as opaque binary",
                other.name(),
                other.oid()
            );
            (
                extended_field(name, ArrowType::Binary, Opaque::new(other.name(), VENDOR))?,
                Box::new(wire_bytes),
            )
        }
    })
}

/// Column `i` exactly as PG sent it, for the types we pass through rather than decode.
fn wire_bytes(rows: &[BinaryCopyOutRow], i: usize) -> Result<ArrayRef> {
    let bytes = col::<RawBytes>(rows, i)?
        .into_iter()
        .map(|raw| raw.map(|raw| raw.bytes));

    Ok(Arc::new(bytes.collect::<BinaryArray>()))
}

/// Nullable Arrow field carrying a canonical Arrow extension type in its metadata.
fn extended_field<E: ExtensionType>(
    name: &str,
    arrow: ArrowType,
    extension: E,
) -> Result<ArrowField> {
    let mut field = ArrowField::new(name, arrow, true);
    field.try_with_extension_type(extension)?;
    Ok(field)
}

/// A column's bytes exactly as PG sent them, accepting any type so unmapped OIDs still decode.
struct RawBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> FromSql<'a> for RawBytes<'a> {
    fn from_sql(
        _: &PgType,
        bytes: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self { bytes })
    }

    fn accepts(_: &PgType) -> bool {
        true
    }
}

/// A column's text exactly as PG sent it; `&str` declines the enum OIDs whose wire form it is.
struct RawText<'a> {
    text: &'a str,
}

impl<'a> FromSql<'a> for RawText<'a> {
    fn from_sql(
        _: &PgType,
        bytes: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self {
            text: str::from_utf8(bytes)?,
        })
    }

    fn accepts(_: &PgType) -> bool {
        true
    }
}

/// Collect column `i` from every row, `None` for SQL NULL. The one place row bytes are decoded, so
/// `try_get` here is what keeps a malformed value an error rather than a panic.
fn col<'a, T>(rows: &'a [BinaryCopyOutRow], i: usize) -> Result<Vec<Option<T>>>
where
    Option<T>: FromSql<'a>,
{
    rows.iter()
        .map(|row| row.try_get(i).map_err(TransferredError::source))
        .collect()
}
