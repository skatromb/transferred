//! Postgres → Arrow type mapping. One table row per supported type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::NullBuffer;
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
use crate::pg_range::{Bounds, PgRange};

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
    /// Derives schema and builders from a prepared statement's columns. All fields nullable.
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

    /// Builds a `RecordBatch` from a chunk of PG binary rows.
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
            Box::new(|rows, i| Ok(int32_array(col(rows, i)?))),
        ),
        PgType::INT8 => (
            ArrowField::new(name, ArrowType::Int64, true),
            Box::new(|rows, i| Ok(int64_array(col(rows, i)?))),
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
            Box::new(|rows, i| Ok(date32_array(col(rows, i)?))),
        ),
        PgType::TIMESTAMP => (
            ArrowField::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Box::new(|rows, i| Ok(timestamp_array(col(rows, i)?))),
        ),
        PgType::TIMESTAMPTZ => (
            ArrowField::new(
                name,
                ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
                true,
            ),
            Box::new(|rows, i| Ok(timestamptz_array(col(rows, i)?))),
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
                Box::new(move |rows, i| decimal128_array(col(rows, i)?, precision, scale)),
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
        // The six built-in ranges, each a pair of bounds over one of the scalars above. Postgres
        // canonicalises a discrete range to `[)`, so their inclusivity flags never vary.
        PgType::INT4_RANGE => range_column(name, PgType::INT4, ArrowType::Int32, |bounds| {
            Ok(int32_array(bounds))
        })?,
        PgType::INT8_RANGE => range_column(name, PgType::INT8, ArrowType::Int64, |bounds| {
            Ok(int64_array(bounds))
        })?,
        PgType::DATE_RANGE => range_column(name, PgType::DATE, ArrowType::Date32, |bounds| {
            Ok(date32_array(bounds))
        })?,
        PgType::TS_RANGE => range_column(
            name,
            PgType::TIMESTAMP,
            ArrowType::Timestamp(TimeUnit::Microsecond, None),
            |bounds| Ok(timestamp_array(bounds)),
        )?,
        PgType::TSTZ_RANGE => range_column(
            name,
            PgType::TIMESTAMPTZ,
            ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            |bounds| Ok(timestamptz_array(bounds)),
        )?,
        PgType::NUM_RANGE => {
            let (precision, scale) = numeric_precision_scale(BARE_NUMERIC_TYPMOD)?;
            // A range constrains no precision on its bounds, so they can only be bare `numeric`s.
            warn!(
                target: "postgres::source",
                column = name,
                "`numrange` bounds carry no declared precision; mapping to \
                 Decimal128({precision}, {scale}) and rounding beyond {scale} decimals"
            );
            range_column(
                name,
                PgType::NUMERIC,
                ArrowType::Decimal128(precision, scale),
                move |bounds| decimal128_array(bounds, precision, scale),
            )?
        }
        // PG sends both as their own UTF-8 text. `citext` has no fixed OID, so goes by name.
        ref text if matches!(text.kind(), Kind::Enum(_)) || text.name() == CITEXT => (
            ArrowField::new(name, ArrowType::Utf8, true),
            Box::new(|rows, i| {
                let strings = col::<RawText>(rows, i)?
                    .into_iter()
                    .map(|raw| raw.map(|raw| raw.text));
                Ok(Arc::new(strings.collect::<StringArray>()))
            }),
        ),
        // `PostGIS` carries no fixed OID, so its types answer to a name. Their binary form is
        // EWKB, which `geoarrow.wkb` accepts as-is, SRID per value and all.
        ref geo if geo.name() == GEOMETRY => (
            extended_field(
                name,
                ArrowType::Binary,
                Wkb::planar(geo_srid(column.type_modifier())),
            )?,
            Box::new(raw_bytes),
        ),
        // The same bytes, but `geography` bends its edges around the globe.
        ref geo if geo.name() == GEOGRAPHY => (
            extended_field(
                name,
                ArrowType::Binary,
                Wkb::spherical(geo_srid(column.type_modifier())),
            )?,
            Box::new(raw_bytes),
        ),
        // No mapping: keep the column transferable as self-describing bytes.
        ref other => {
            warn!(
                target: "postgres::source",
                column = name,
                "no Arrow mapping for Postgres type `{}` (oid {}); \
                 passing its bytes through as opaque binary",
                other.name(),
                other.oid()
            );
            (
                extended_field(name, ArrowType::Binary, Opaque::new(other.name(), VENDOR))?,
                Box::new(raw_bytes),
            )
        }
    })
}

/// The array builders a scalar column shares with the range over it, one per range Postgres has.
fn int32_array(values: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}

fn int64_array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn date32_array(dates: Vec<Option<NaiveDate>>) -> ArrayRef {
    let days = dates
        .into_iter()
        .map(|date| date.map(Date32Type::from_naive_date));
    Arc::new(days.collect::<Date32Array>())
}

fn timestamp_array(timestamps: Vec<Option<NaiveDateTime>>) -> ArrayRef {
    let micros = timestamps
        .into_iter()
        .map(|ts| ts.map(|ts| ts.and_utc().timestamp_micros()));
    Arc::new(micros.collect::<TimestampMicrosecondArray>())
}

fn timestamptz_array(timestamps: Vec<Option<DateTime<Utc>>>) -> ArrayRef {
    let micros = timestamps
        .into_iter()
        .map(|ts| ts.map(|ts| ts.timestamp_micros()));
    Arc::new(
        micros
            .collect::<TimestampMicrosecondArray>()
            .with_timezone(UTC),
    )
}

/// Each value carries its own scale on the wire, so all of them are restated at the column's.
fn decimal128_array(decimals: Vec<Option<Decimal>>, precision: u8, scale: i8) -> Result<ArrayRef> {
    let units = decimals
        .into_iter()
        .map(|decimal| decimal.map(|decimal| decimal_units(decimal, scale)))
        .map(Option::transpose)
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(
        Decimal128Array::from(units).with_precision_and_scale(precision, scale)?,
    ))
}

/// Declares a Postgres range column: `PgRange::fields` shapes it, `range_array` fills it.
fn range_column<T: for<'a> FromSql<'a>>(
    name: &str,
    bound_type: PgType,
    arrow: ArrowType,
    bounds_to_array: impl Fn(Vec<Option<T>>) -> Result<ArrayRef> + Send + Sync + 'static,
) -> Result<(ArrowField, PgToArrowFn)> {
    let field = extended_field(name, ArrowType::Struct(PgRange::fields(arrow)), PgRange)?;
    let builder = move |rows: &[BinaryCopyOutRow], i| {
        let ranges = col::<RawBytes>(rows, i)?
            .into_iter()
            .map(|raw| {
                raw.map(|raw| Bounds::from_binary(&bound_type, raw.bytes))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        range_array(ranges, &bounds_to_array)
    };

    Ok((field, Box::new(builder)))
}

/// Builds the five arrays behind a range's struct, clearing a SQL NULL row in every one of them.
/// Totally non-optimal, makes 7 iterations, but whatever
fn range_array<T>(
    ranges: Vec<Option<Bounds<T>>>,
    bounds_to_array: impl Fn(Vec<Option<T>>) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    let nulls = ranges.iter().map(Option::is_some).collect::<NullBuffer>();
    let lower_inc = flags(&ranges, |bounds| bounds.lower_inc);
    let upper_inc = flags(&ranges, |bounds| bounds.upper_inc);
    let empty = flags(&ranges, |bounds| bounds.empty);

    let (lower, upper): (Vec<_>, Vec<_>) = ranges
        .into_iter()
        .map(|bounds| match bounds {
            Some(bounds) => (bounds.lower, bounds.upper),
            None => (None, None),
        })
        .unzip();

    let (lower, upper) = (bounds_to_array(lower)?, bounds_to_array(upper)?);
    let fields = PgRange::fields(lower.data_type().clone());
    let columns: Vec<ArrayRef> = vec![
        lower,
        upper,
        Arc::new(lower_inc),
        Arc::new(upper_inc),
        Arc::new(empty),
    ];

    Ok(Arc::new(StructArray::try_new(
        fields,
        columns,
        Some(nulls),
    )?))
}

/// Collects one tag bit as a column; a SQL NULL row carries no tag, so it reads as false.
fn flags<T>(ranges: &[Option<Bounds<T>>], is_set: impl Fn(&Bounds<T>) -> bool) -> BooleanArray {
    ranges
        .iter()
        .map(|bounds| bounds.as_ref().is_some_and(&is_set))
        .collect()
}

/// Column `i` exactly as PG sent it, for the types we pass through rather than decode.
fn raw_bytes(rows: &[BinaryCopyOutRow], i: usize) -> Result<ArrayRef> {
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

/// A column's text exactly as PG sent it; `&str` declines the enum OIDs whose binary form it is.
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

/// Collects column `i` from every row, `None` for SQL NULL. The one place row bytes are decoded, so
/// `try_get` here is what keeps a malformed value an error rather than a panic.
fn col<'a, T>(rows: &'a [BinaryCopyOutRow], i: usize) -> Result<Vec<Option<T>>>
where
    Option<T>: FromSql<'a>,
{
    rows.iter()
        .map(|row| row.try_get(i).map_err(TransferredError::source))
        .collect()
}
