//! Postgres → Arrow type mapping. One table row per supported type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{Date32Type, IntervalMonthDayNano};
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

/// Builds one Arrow column from column `i` of a chunk of PG binary rows.
type PgToArrowFn = Box<dyn Fn(&[BinaryCopyOutRow], usize) -> Result<ArrayRef> + Send + Sync>;

/// PG stores `timestamptz` as UTC; the original client offset is not retained.
const UTC: &str = "UTC";

/// Arrow `Decimal128` holds at most 38 digits; PG `numeric` allows up to 1000.
const DECIMAL128_MAX_PRECISION: i32 = 38;

/// Precision and scale for bare `numeric`, matching BQ `NUMERIC` so it lands there uncoerced.
const BARE_NUMERIC: (i32, i32) = (DECIMAL128_MAX_PRECISION, 9);

/// Typmod PG reports for a `numeric` declared without precision.
const BARE_NUMERIC_TYPMOD: i32 = -1;

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
/// A flat lookup table is one logical unit; splitting it by length would only obscure it.
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

/// Decode a `numeric` typmod, defaulting bare `numeric` (`-1`) to the BQ `NUMERIC` shape.
fn numeric_precision_scale(typmod: i32) -> Result<(u8, i8)> {
    let (precision, scale) = match typmod {
        BARE_NUMERIC_TYPMOD => BARE_NUMERIC,
        // `numeric_typmod_precision`/`numeric_typmod_scale`, minus `VARHDRSZ`; the XOR sign-extends
        // the 11-bit scale, which PG 15+ allows to be negative.
        // https://github.com/postgres/postgres/blob/REL_17_10/src/backend/utils/adt/numeric.c#L925
        _ => (
            ((typmod - 4) >> 16) & 0xffff,
            (((typmod - 4) & 0x7ff) ^ 0x400) - 0x400,
        ),
    };

    // PG 15+ decouples scale from precision, so a narrow column may still carry an i8-busting scale.
    match (u8::try_from(precision), i8::try_from(scale)) {
        (Ok(precision), Ok(scale)) if i32::from(precision) <= DECIMAL128_MAX_PRECISION => {
            Ok((precision, scale))
        }
        _ => Err(TransferredError::source(format!(
            "`numeric({precision},{scale})` is outside Arrow `Decimal128`, \
             which holds {DECIMAL128_MAX_PRECISION} digits"
        ))),
    }
}

/// Restate a decimal as an integer count of `10^-scale` units, as Arrow `Decimal128` stores it.
fn decimal_units(mut decimal: Decimal, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).map_err(|_| {
        TransferredError::source("`numeric` with negative scale is not supported in 0.1")
    })?;

    decimal.rescale(scale);
    if decimal.scale() != scale {
        return Err(TransferredError::source(format!(
            "`numeric` value {decimal} does not fit scale {scale}"
        )));
    }

    Ok(decimal.mantissa())
}

/// PG counts interval time in microseconds; Arrow wants nanoseconds, which overflow past ~292 years.
fn month_day_nano(interval: PgInterval) -> Result<IntervalMonthDayNano> {
    let nanos = interval.microseconds.checked_mul(1_000).ok_or_else(|| {
        TransferredError::source(
            "`interval` exceeds the nanosecond range of Arrow `Interval(MonthDayNano)`",
        )
    })?;

    Ok(IntervalMonthDayNano::new(
        interval.months,
        interval.days,
        nanos,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// Typmods as PG 17 stores them in `pg_attribute.atttypmod`, pinned here rather than reused
    /// from the constants above, so a wrong constant fails a test instead of agreeing with it.
    const NUMERIC_BARE: i32 = -1;
    const NUMERIC_18_4: i32 = 1_179_656;
    const NUMERIC_38_9: i32 = 2_490_381;
    const NUMERIC_5_NEG2: i32 = 329_730;
    const NUMERIC_1000_500: i32 = 65_536_504;
    const NUMERIC_5_200: i32 = 327_884;

    /// Largest mantissa `rust_decimal` can hold: 2^96 - 1, with no room to zero-pad.
    const U96_MAX: Decimal = Decimal::from_parts(u32::MAX, u32::MAX, u32::MAX, false, 0);

    #[test]
    fn typmod_decodes_declared_precision_and_scale() {
        assert_eq!(numeric_precision_scale(NUMERIC_18_4).unwrap(), (18, 4));
        assert_eq!(numeric_precision_scale(NUMERIC_38_9).unwrap(), (38, 9));
    }

    #[test]
    fn bare_numeric_defaults_to_bq_numeric_shape() {
        assert_eq!(numeric_precision_scale(NUMERIC_BARE).unwrap(), (38, 9));
    }

    /// PG 15+ allows negative scale; the decode must not read it as a large positive one.
    #[test]
    fn typmod_keeps_negative_scale_negative() {
        assert_eq!(numeric_precision_scale(NUMERIC_5_NEG2).unwrap(), (5, -2));
    }

    #[test]
    fn typmod_rejects_precision_past_decimal128() {
        assert!(numeric_precision_scale(NUMERIC_1000_500).is_err());
    }

    /// PG 15+ decouples scale from precision, so a narrow column can still carry an i8-busting scale.
    #[test]
    fn typmod_rejects_scale_past_i8() {
        assert!(numeric_precision_scale(NUMERIC_5_200).is_err());
    }

    #[test]
    fn decimal_units_scales_to_target() {
        assert_eq!(
            decimal_units(Decimal::new(15, 1), 9).unwrap(),
            1_500_000_000
        );
        assert_eq!(
            decimal_units(Decimal::new(-25, 2), 9).unwrap(),
            -250_000_000
        );
        assert_eq!(decimal_units(Decimal::new(15, 1), 4).unwrap(), 15_000);
        assert_eq!(decimal_units(Decimal::ZERO, 4).unwrap(), 0);
    }

    /// `rescale` is infallible and silently keeps the old scale when padding would overflow the
    /// mantissa, so without the scale check we would emit this value 10^9 times too small.
    #[test]
    fn decimal_units_rejects_value_too_wide_to_rescale() {
        assert_eq!(U96_MAX.scale(), 0);
        assert!(decimal_units(U96_MAX, 9).is_err());
    }

    /// Bare `numeric` carries the value's own scale, so the (38,9) default rounds. Lossy by design.
    #[test]
    fn decimal_units_rounds_excess_fraction_digits_half_away_from_zero() {
        // 0.1234567885 sits exactly on the midpoint: half-to-even would keep ...788.
        assert_eq!(
            decimal_units(Decimal::new(1_234_567_885, 10), 9).unwrap(),
            123_456_789
        );
        assert_eq!(
            decimal_units(Decimal::new(-1_234_567_885, 10), 9).unwrap(),
            -123_456_789
        );
    }

    #[test]
    fn decimal_units_rejects_negative_scale() {
        assert!(decimal_units(Decimal::new(15, 1), -2).is_err());
    }

    #[test]
    fn interval_keeps_months_days_and_micros_separate() {
        let interval = PgInterval::new(14, 3, 14_706_789_000);
        assert_eq!(
            month_day_nano(interval).unwrap(),
            IntervalMonthDayNano::new(14, 3, 14_706_789_000_000)
        );
    }

    #[test]
    fn interval_carries_each_part_signed() {
        let interval = PgInterval::new(-1, -2, -10_800_000_000);
        assert_eq!(
            month_day_nano(interval).unwrap(),
            IntervalMonthDayNano::new(-1, -2, -10_800_000_000_000)
        );
    }

    #[test]
    fn interval_rejects_micros_past_nanosecond_range() {
        assert!(month_day_nano(PgInterval::new(0, 0, i64::MAX)).is_err());
    }
}
