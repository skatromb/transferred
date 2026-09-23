//! Postgres → Arrow type mapping. One `Decoding` variant per supported type; mirror of `arrow_to_pg`.
//!
//! A statement's columns map once into `ColumnDecoder`s; every value decodes itself through its
//! `Decoding`, straight into the Arrow builder its column's array comes out of.

use std::any::type_name;
use std::error::Error as StdError;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, IntervalMonthDayNanoBuilder, RecordBatch, StringBuilder, StructBuilder,
    TimestampMicrosecondBuilder, make_builder,
};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, Date32Type, IntervalMonthDayNano};
use arrow_schema::extension::{Json, Opaque, Uuid};
use arrow_schema::{
    DataType as ArrowType, Field as ArrowField, IntervalUnit, Schema, SchemaRef, TimeUnit,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use geoarrow_schema::WkbType;
use pg_interval::Interval as PgInterval;
use postgres_protocol::types::{Range, RangeBound, range_from_sql};
use rust_decimal::Decimal;
use tokio_postgres::Column as PgColumn;
use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::types::{FromSql, Kind, Type as PgType};
use tracing::warn;
use transferred_core::{Result, TransferredError};

use crate::geoarrow::{self, GEOGRAPHY, GEOMETRY};
use crate::pg_range::PgRange;

/// PG stores `timestamptz` as UTC; the original client offset is not retained.
const UTC: &str = "UTC";

/// Case-insensitive text, an extension type answering to a name rather than a fixed OID.
const CITEXT: &str = "citext";

/// Precision and scale for bare `numeric`, matching BQ `NUMERIC` so it lands there uncoerced.
const BARE_NUMERIC: (u8, i8) = (DECIMAL128_MAX_PRECISION, 9);

/// Typmod PG reports for a `numeric` declared without precision.
const BARE_NUMERIC_TYPMOD: i32 = -1;

/// PG counts sub-second time in microseconds; Arrow intervals count nanoseconds.
const NANOS_PER_MICRO: i64 = 1_000;

/// The `arrow.opaque` fallback's `vendor_name`: the system an unmapped type came from.
const VENDOR: &str = "PostgreSQL";

/// Bytes an Arrow `uuid` holds, which is also what PG sends.
const UUID_BYTES: i32 = 16;

/// Arrow schema + per-column decoders, mapped once from PG column metadata.
pub struct Decoder {
    schema: SchemaRef,
    columns: Vec<ColumnDecoder>,
}

impl Decoder {
    /// Maps a prepared statement's columns onto Arrow columns. All fields nullable.
    pub fn derive(columns: &[PgColumn]) -> Result<Self> {
        let columns = columns
            .iter()
            .map(ColumnDecoder::new)
            .collect::<Result<Vec<_>>>()?;
        let fields: Vec<_> = columns.iter().map(|column| column.field.clone()).collect();

        Ok(Self {
            schema: Schema::new(fields).into(),
            columns,
        })
    }

    /// Appends one row, each field still exactly as Postgres sent it.
    pub fn append_row(&mut self, row: &BinaryCopyOutRow) -> Result<()> {
        for (at, column) in self.columns.iter_mut().enumerate() {
            let raw: Option<Raw> = row.try_get(at).map_err(TransferredError::source)?;
            column.append(raw.map(|raw| raw.0))?;
        }

        Ok(())
    }

    /// Takes the rows appended so far as a `RecordBatch`, leaving the builders empty again.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let arrays = self.columns.iter_mut().map(ColumnDecoder::finish).collect();

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// A field's bytes untouched, for any type — `&[u8]`'s own `FromSql` accepts `bytea` alone.
struct Raw<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _: &PgType,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(Self(raw))
    }

    fn accepts(_: &PgType) -> bool {
        true
    }
}

/// One column of the source: the Arrow field it becomes, how its values are read, where they land.
struct ColumnDecoder {
    field: ArrowField,
    decoding: Decoding,
    builder: Box<dyn ArrayBuilder>,
}

impl ColumnDecoder {
    fn new(column: &PgColumn) -> Result<Self> {
        let decoding = Decoding::new(column)?;
        let field = decoding.field(column.name())?;
        // The builder comes off the field the batch is checked against, so the two cannot disagree.
        let builder = make_builder(field.data_type(), 0);

        Ok(Self {
            field,
            decoding,
            builder,
        })
    }

    /// Appends one value of this column, or a null where Postgres sent no bytes.
    fn append(&mut self, bytes: Option<&[u8]>) -> Result<()> {
        self.decoding
            .append(&mut *self.builder, bytes)
            .map_err(|error| {
                TransferredError::source(format!("column {}: {error}", self.field.name()))
            })
    }

    fn finish(&mut self) -> ArrayRef {
        self.builder.finish()
    }
}

/// One decision per column, answering for both its Arrow field and its values' binary form.
enum Decoding {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    /// `versioned` is `jsonb`, which leads with a format version byte `json` has no room for.
    Json {
        versioned: bool,
    },
    Bytea,
    Uuid,
    Date,
    Timestamp,
    Timestamptz,
    Interval,
    Numeric {
        precision: u8,
        scale: i8,
    },
    /// `PostGIS` sends EWKB, which `geoarrow.wkb` takes verbatim; only the field names the geo type.
    Geo(WkbType),
    /// A range arrives as a tag byte plus bounds, each bound through the element's own decoding.
    Range(Box<Decoding>),
    /// No mapping: the bytes pass through, tagged with the Postgres type they came from.
    Opaque(Opaque),
}

impl Decoding {
    /// Decides what Arrow column a Postgres column becomes.
    fn new(column: &PgColumn) -> Result<Self> {
        let (name, typmod) = (column.name(), column.type_modifier());
        Ok(match *column.type_() {
            PgType::BOOL => Self::Bool,
            PgType::INT2 => Self::Int2,
            PgType::INT4 => Self::Int4,
            PgType::INT8 => Self::Int8,
            PgType::FLOAT4 => Self::Float4,
            PgType::FLOAT8 => Self::Float8,
            PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME => Self::Text,
            PgType::BYTEA => Self::Bytea,
            PgType::DATE => Self::Date,
            PgType::TIMESTAMP => Self::Timestamp,
            PgType::TIMESTAMPTZ => Self::Timestamptz,
            PgType::INTERVAL => Self::Interval,
            PgType::UUID => Self::Uuid,
            PgType::JSON => Self::Json { versioned: false },
            PgType::JSONB => Self::Json { versioned: true },
            PgType::NUMERIC => Self::numeric(typmod, name)?,
            PgType::INT4_RANGE => Self::Range(Box::new(Self::Int4)),
            PgType::INT8_RANGE => Self::Range(Box::new(Self::Int8)),
            PgType::DATE_RANGE => Self::Range(Box::new(Self::Date)),
            PgType::TS_RANGE => Self::Range(Box::new(Self::Timestamp)),
            PgType::TSTZ_RANGE => Self::Range(Box::new(Self::Timestamptz)),
            // A range constrains no precision on its bounds, so they can only be bare.
            PgType::NUM_RANGE => Self::Range(Box::new(Self::numeric(BARE_NUMERIC_TYPMOD, name)?)),
            // Extension-type OIDs differ per database, so `citext` and `PostGIS` match on a name.
            ref text if matches!(text.kind(), Kind::Enum(_)) || text.name() == CITEXT => Self::Text,
            // `geoarrow.wkb` holds EWKB, so the bytes pass through untouched, SRID per value and all.
            ref geo if geo.name() == GEOMETRY => {
                Self::Geo(geoarrow::planar(geoarrow::srid(typmod)))
            }
            ref geo if geo.name() == GEOGRAPHY => {
                Self::Geo(geoarrow::spherical(geoarrow::srid(typmod)))
            }
            ref other => {
                warn!(
                    target: "postgres::source",
                    column = name,
                    "no Arrow mapping for Postgres type `{}` (oid {}); \
                     passing its bytes through as opaque binary",
                    other.name(),
                    other.oid()
                );
                Self::Opaque(Opaque::new(other.name(), VENDOR))
            }
        })
    }

    /// Decodes a `numeric` typmod into the `Decimal128` its values are restated at.
    fn numeric(typmod: i32, name: &str) -> Result<Self> {
        let (precision, scale) = numeric_precision_scale(typmod)?;
        if typmod == BARE_NUMERIC_TYPMOD {
            warn!(
                target: "postgres::source",
                column = name,
                "`numeric` without declared precision; mapping to \
                 Decimal128({precision}, {scale}) and rounding beyond {scale} decimals"
            );
        }

        Ok(Self::Numeric { precision, scale })
    }

    /// Arrow type the column's values land in; the test suite pins every one.
    fn arrow_type(&self) -> ArrowType {
        match self {
            Self::Bool => ArrowType::Boolean,
            Self::Int2 => ArrowType::Int16,
            Self::Int4 => ArrowType::Int32,
            Self::Int8 => ArrowType::Int64,
            Self::Float4 => ArrowType::Float32,
            Self::Float8 => ArrowType::Float64,
            Self::Text | Self::Json { .. } => ArrowType::Utf8,
            Self::Bytea | Self::Geo(_) | Self::Opaque(_) => ArrowType::Binary,
            Self::Uuid => ArrowType::FixedSizeBinary(UUID_BYTES),
            Self::Date => ArrowType::Date32,
            Self::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, None),
            Self::Timestamptz => ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            Self::Interval => ArrowType::Interval(IntervalUnit::MonthDayNano),
            &Self::Numeric { precision, scale } => ArrowType::Decimal128(precision, scale),
            Self::Range(bounds) => ArrowType::Struct(PgRange::fields(bounds.arrow_type())),
        }
    }

    /// Nullable Arrow field for the column, carrying whatever extension type tags its values.
    fn field(&self, name: &str) -> Result<ArrowField> {
        let mut field = ArrowField::new(name, self.arrow_type(), true);

        match self {
            Self::Uuid => field.try_with_extension_type(Uuid)?,
            Self::Json { .. } => field.try_with_extension_type(Json::default())?,
            Self::Geo(wkb) => field.try_with_extension_type(wkb.clone())?,
            Self::Opaque(opaque) => field.try_with_extension_type(opaque.clone())?,
            Self::Range(_) => field.try_with_extension_type(PgRange)?,
            _ => {}
        }

        Ok(field)
    }

    /// Appends one value in Postgres binary form into `builder`, or a null where PG sent no bytes.
    fn append(&self, builder: &mut dyn ArrayBuilder, bytes: Option<&[u8]>) -> Result<()> {
        match self {
            Self::Bool => {
                cast::<BooleanBuilder>(builder)?.append_option(decode(&PgType::BOOL, bytes)?);
            }
            Self::Int2 => {
                cast::<Int16Builder>(builder)?.append_option(decode(&PgType::INT2, bytes)?);
            }
            Self::Int4 => {
                cast::<Int32Builder>(builder)?.append_option(decode(&PgType::INT4, bytes)?);
            }
            Self::Int8 => {
                cast::<Int64Builder>(builder)?.append_option(decode(&PgType::INT8, bytes)?);
            }
            Self::Float4 => {
                cast::<Float32Builder>(builder)?.append_option(decode(&PgType::FLOAT4, bytes)?);
            }
            Self::Float8 => {
                cast::<Float64Builder>(builder)?.append_option(decode(&PgType::FLOAT8, bytes)?);
            }
            Self::Text => cast::<StringBuilder>(builder)?.append_option(text(bytes)?),
            &Self::Json { versioned } => {
                let json = bytes.map(|bytes| json(bytes, versioned)).transpose()?;
                cast::<StringBuilder>(builder)?.append_option(text(json)?);
            }
            Self::Bytea | Self::Geo(_) | Self::Opaque(_) => {
                cast::<BinaryBuilder>(builder)?.append_option(bytes);
            }
            Self::Uuid => {
                let builder = cast::<FixedSizeBinaryBuilder>(builder)?;
                match decode::<uuid::Uuid>(&PgType::UUID, bytes)? {
                    Some(uuid) => builder.append_value(uuid.into_bytes())?,
                    None => builder.append_null(),
                }
            }
            Self::Date => cast::<Date32Builder>(builder)?.append_option(
                decode::<NaiveDate>(&PgType::DATE, bytes)?.map(Date32Type::from_naive_date),
            ),
            Self::Timestamp => cast::<TimestampMicrosecondBuilder>(builder)?.append_option(
                decode::<NaiveDateTime>(&PgType::TIMESTAMP, bytes)?
                    .map(|ts| ts.and_utc().timestamp_micros()),
            ),
            Self::Timestamptz => cast::<TimestampMicrosecondBuilder>(builder)?.append_option(
                decode::<DateTime<Utc>>(&PgType::TIMESTAMPTZ, bytes)?
                    .map(|ts| ts.timestamp_micros()),
            ),
            Self::Interval => cast::<IntervalMonthDayNanoBuilder>(builder)?.append_option(
                decode::<PgInterval>(&PgType::INTERVAL, bytes)?
                    .map(month_day_nano)
                    .transpose()?,
            ),
            Self::Numeric { scale, .. } => cast::<Decimal128Builder>(builder)?.append_option(
                decode::<Decimal>(&PgType::NUMERIC, bytes)?
                    .map(|decimal| decimal_units(decimal, *scale))
                    .transpose()?,
            ),
            Self::Range(bounds) => append_range(bounds, cast(builder)?, bytes)?,
        }

        Ok(())
    }
}

/// Appends one range: the tag byte, then whichever bounds it says are there.
fn append_range(bounds: &Decoding, range: &mut StructBuilder, bytes: Option<&[u8]>) -> Result<()> {
    let parsed = bytes
        .map(range_from_sql)
        .transpose()
        .map_err(TransferredError::source)?;

    let [lower, upper, lower_inc, upper_inc, empty] = range.field_builders_mut() else {
        return Err(TransferredError::source(
            "a `transferred.pg_range` column does not build the five fields it declares",
        ));
    };

    match parsed {
        Some(Range::Nonempty(low, high)) => {
            bounds.append(&mut **lower, bound(&low))?;
            bounds.append(&mut **upper, bound(&high))?;
            tag(lower_inc, matches!(low, RangeBound::Inclusive(_)))?;
            tag(upper_inc, matches!(high, RangeBound::Inclusive(_)))?;
            tag(empty, false)?;
        }
        // An empty range and a SQL NULL both leave every bound null. `empty` separates them, and
        // it is `Some` for exactly the range that carried a tag saying so.
        boundless => {
            bounds.append(&mut **lower, None)?;
            bounds.append(&mut **upper, None)?;
            tag(lower_inc, false)?;
            tag(upper_inc, false)?;
            tag(empty, boundless.is_some())?;
        }
    }

    // The struct's own validity is the only thing that says a whole range was NULL.
    range.append(bytes.is_some());

    Ok(())
}

/// Appends one of a range's three tag bits, none of which is ever null.
fn tag(builder: &mut Box<dyn ArrayBuilder>, set: bool) -> Result<()> {
    cast::<BooleanBuilder>(&mut **builder)?.append_value(set);

    Ok(())
}

/// Decodes one value from its Postgres binary form; `None` is a NULL, which PG sends no bytes for.
fn decode<'a, T: FromSql<'a>>(pg_type: &PgType, bytes: Option<&'a [u8]>) -> Result<Option<T>> {
    bytes
        .map(|bytes| T::from_sql(pg_type, bytes))
        .transpose()
        .map_err(TransferredError::source)
}

/// Strips the format version byte `jsonb` leads with; `json` sends the document text as it is.
fn json(bytes: &[u8], versioned: bool) -> Result<&[u8]> {
    if !versioned {
        return Ok(bytes);
    }

    match bytes.split_first() {
        Some((1, text)) => Ok(text),
        _ => Err(TransferredError::source(
            "`jsonb` arrived in an encoding version other than 1",
        )),
    }
}

/// Reads a text value's bytes, which every Postgres text type sends as its own UTF-8.
fn text(bytes: Option<&[u8]>) -> Result<Option<&str>> {
    bytes
        .map(str::from_utf8)
        .transpose()
        .map_err(TransferredError::source)
}

/// A bound's bytes; `None` is an infinite bound, the only kind Postgres sends no value for.
fn bound<'a>(bound: &RangeBound<Option<&'a [u8]>>) -> Option<&'a [u8]> {
    match bound {
        RangeBound::Inclusive(value) | RangeBound::Exclusive(value) => *value,
        RangeBound::Unbounded => None,
    }
}

/// Decodes a `numeric` typmod, defaulting bare `numeric` `-1` to (38,9).
fn numeric_precision_scale(typmod: i32) -> Result<(u8, i8)> {
    if typmod == BARE_NUMERIC_TYPMOD {
        return Ok(BARE_NUMERIC);
    }

    // `numeric_typmod_precision`/`numeric_typmod_scale`, minus `VARHDRSZ`; the XOR sign-extends the
    // 11-bit scale, which PG 15+ allows to be negative.
    // https://github.com/postgres/postgres/blob/REL_17_10/src/backend/utils/adt/numeric.c#L925
    let precision = ((typmod - 4) >> 16) & 0xffff;
    let scale = (((typmod - 4) & 0x7ff) ^ 0x400) - 0x400;

    // PG holds 1000 digits to Arrow's 38, and PG 15+ decouples scale from precision, so a narrow
    // column may still carry a scale that busts an i8 or outruns its own precision.
    match (u8::try_from(precision), i8::try_from(scale)) {
        (Ok(precision), Ok(scale))
            if precision <= DECIMAL128_MAX_PRECISION
                && i32::from(scale) <= i32::from(precision) =>
        {
            Ok((precision, scale))
        }
        _ => Err(TransferredError::source(format!(
            "`numeric({precision},{scale})` is outside Arrow `Decimal128`, which holds \
             {DECIMAL128_MAX_PRECISION} digits and no more scale than precision"
        ))),
    }
}

/// Restates a decimal as an integer count of `10^-scale` units, as Arrow `Decimal128` stores it.
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
    let nanos = interval
        .microseconds
        .checked_mul(NANOS_PER_MICRO)
        .ok_or_else(|| {
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

/// Downcasts an Arrow builder; a mismatch is unreachable, as `make_builder` took the same decoding.
fn cast<B: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> Result<&mut B> {
    builder
        .as_any_mut()
        .downcast_mut::<B>()
        .ok_or_else(|| TransferredError::source(format!("column is not a {}", type_name::<B>())))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use arrow::array::{Array, BooleanArray, Int32Array, StructArray};
    use arrow_schema::extension::ExtensionType;

    use super::*;

    /// `[1,5]` over a discrete type reaches us canonicalised to `[1,6)`, tag bits and all.
    const BOUNDED: [u8; 17] = [0b0000_0010, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 6];

    /// Both bounds infinite: no value follows the tag.
    const UNBOUNDED: [u8; 1] = [0b0001_1000];

    const EMPTY_RANGE: [u8; 1] = [0b0000_0001];

    fn int4_range() -> Decoding {
        Decoding::Range(Box::new(Decoding::Int4))
    }

    /// Decodes one `int4range` value into the one-row struct its column lands in.
    fn decode_range(bytes: Option<&[u8]>) -> Result<StructArray> {
        let decoding = int4_range();
        let mut builder = make_builder(&decoding.arrow_type(), 0);
        decoding.append(&mut *builder, bytes)?;

        Ok(builder
            .finish()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone())
    }

    /// The five fields of a one-row range struct: both bounds, then the three tag bits.
    fn parts(range: &StructArray) -> (Option<i32>, Option<i32>, bool, bool, bool) {
        let bound = |i: usize| {
            let column = range
                .column(i)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            column.is_valid(0).then(|| column.value(0))
        };
        let flag = |i: usize| {
            range
                .column(i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        };

        (bound(0), bound(1), flag(2), flag(3), flag(4))
    }

    #[test]
    fn decodes_a_bounded_range() {
        let range = decode_range(Some(&BOUNDED)).unwrap();

        assert!(range.is_valid(0));
        assert_eq!(parts(&range), (Some(1), Some(6), true, false, false));
    }

    /// An infinite bound is a null bound, and neither infinite bound counts as inclusive.
    #[test]
    fn decodes_an_unbounded_range() {
        let range = decode_range(Some(&UNBOUNDED)).unwrap();

        assert!(range.is_valid(0));
        assert_eq!(parts(&range), (None, None, false, false, false));
    }

    /// Empty is the one state the bounds cannot express, which is why it gets a field of its own.
    #[test]
    fn decodes_an_empty_range() {
        let range = decode_range(Some(&EMPTY_RANGE)).unwrap();

        assert!(range.is_valid(0));
        assert_eq!(parts(&range), (None, None, false, false, true));
    }

    /// A SQL NULL range carries no tag at all, so it must not arrive looking `empty`.
    #[test]
    fn separates_a_null_range_from_an_empty_one() {
        let range = decode_range(None).unwrap();

        assert!(range.is_null(0));
        assert_eq!(parts(&range), (None, None, false, false, false));
    }

    #[test]
    fn rejects_bytes_that_are_not_a_range() {
        assert!(decode_range(Some(&[])).is_err());
    }

    #[test]
    fn tags_a_range_field_with_the_type_of_its_bounds() {
        let field = int4_range().field("valid").unwrap();

        assert_eq!(field.extension_type_name(), Some(PgRange::NAME));
        assert_eq!(
            field.data_type(),
            &ArrowType::Struct(PgRange::fields(ArrowType::Int32))
        );
    }

    /// Typmods as PG 17 stores them in `pg_attribute.atttypmod`, pinned here rather than reused
    /// from the constants above, so a wrong constant fails a test instead of agreeing with it.
    const NUMERIC_BARE: i32 = -1;
    const NUMERIC_18_4: i32 = 1_179_656;
    const NUMERIC_38_9: i32 = 2_490_381;
    const NUMERIC_5_NEG2: i32 = 329_730;
    const NUMERIC_1000_500: i32 = 65_536_504;
    const NUMERIC_5_200: i32 = 327_884;
    const NUMERIC_5_10: i32 = 327_694;

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

    /// PG takes `numeric(5,10)`; Arrow `Decimal128` does not, and would build a broken array from it.
    #[test]
    fn typmod_rejects_scale_wider_than_precision() {
        assert!(numeric_precision_scale(NUMERIC_5_10).is_err());
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
