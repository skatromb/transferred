//! Postgres → Arrow type mapping. One `Decoding` variant per supported type; mirror of `arrow_to_pg`.
//!
//! A statement's columns map once into `ColumnDecoder`s; every value decodes itself through its
//! `Decoding`, straight into the Arrow builder its column's array comes out of.

use std::result;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::NullBuffer;
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
use transferred_core::{AnyError, Result, TransferredError};

use crate::geoarrow::{self, GEOGRAPHY, GEOMETRY};
use crate::pg_range::PgRange;

/// PG stores `timestamptz` as UTC; the original client offset is not retained.
const UTC: &str = "UTC";

/// Case-insensitive text, an extension type answering to a name rather than a fixed OID.
const CITEXT: &str = "citext";

/// Precision and scale for bare `numeric`, matching BQ `NUMERIC` so it lands there uncoerced.
const BARE_NUMERIC: (u8, u8) = (DECIMAL128_MAX_PRECISION, 9);

/// Typmod PG reports for a `numeric` declared without precision.
const BARE_NUMERIC_TYPMOD: i32 = -1;

/// Varlena header PG adds to every typmod it encodes.
const VARHDRSZ: i32 = 4;

/// PG counts sub-second time in microseconds; Arrow intervals count nanoseconds.
const NANOS_PER_MICRO: i64 = 1_000;

/// The `arrow.opaque` fallback's `vendor_name`: the system an unmapped type came from.
const VENDOR: &str = "PostgreSQL";

/// Bytes an Arrow `uuid` holds, which is also what PG sends.
const UUID_BYTES: i32 = 16;

/// Arrow schema + per-column decodings, mapped once from PG column metadata.
pub(crate) struct Decoder {
    schema: SchemaRef,
    decodings: Vec<Decoding>,
}

impl Decoder {
    /// Maps a prepared statement's columns onto Arrow columns. All fields nullable.
    pub(crate) fn derive(columns: &[PgColumn]) -> Result<Self> {
        let (fields, decodings): (Vec<_>, Vec<_>) = columns
            .iter()
            .map(|column| {
                let decoding = Decoding::new(column)?;
                Ok((decoding.field(column.name())?, decoding))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            schema: Schema::new(fields).into(),
            decodings,
        })
    }

    /// Decodes a batch of rows, each column straight from the bytes Postgres sent for it.
    pub(crate) fn decode(&self, rows: &[BinaryCopyOutRow]) -> Result<RecordBatch> {
        let arrays = self
            .decodings
            .iter()
            .zip(self.schema.fields().iter())
            .enumerate()
            .map(|(index, (decoding, field))| {
                decoding.array(&column(rows, index)?).map_err(|error| {
                    TransferredError::in_source(format!("column {}: {error}", field.name()))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// A value's bytes exactly as Postgres sent them; `None` where it sent none.
type Cell<'buf> = Option<&'buf [u8]>;

/// One column's cells, each exactly as Postgres sent it; `None` where it sent a NULL.
fn column(rows: &[BinaryCopyOutRow], index: usize) -> Result<Vec<Cell<'_>>> {
    rows.iter()
        .map(|row| {
            row.try_get::<Option<Raw<'_>>>(index)
                .map(|cell| cell.map(|raw| raw.0))
        })
        .collect::<result::Result<_, _>>()
        .map_err(TransferredError::in_source)
}

/// A field's bytes untouched, for any type — `&[u8]`'s own `FromSql` accepts `bytea` alone.
struct Raw<'buf>(&'buf [u8]);

impl<'buf> FromSql<'buf> for Raw<'buf> {
    fn from_sql(_: &PgType, raw: &'buf [u8]) -> result::Result<Self, AnyError> {
        Ok(Self(raw))
    }

    fn accepts(_: &PgType) -> bool {
        true
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
    Json,
    Jsonb,
    Bytea,
    Uuid,
    Date,
    Timestamp,
    Timestamptz,
    Interval,
    Numeric {
        precision: u8,
        scale: u8,
    },
    /// `PostGIS` sends EWKB, which `geoarrow.wkb` takes verbatim; only the field names the geo type.
    Geo(WkbType),
    /// A range arrives as a tag byte plus bounds, each bound through the element's own decoding.
    Range(Box<Self>),
    /// No mapping: the bytes pass through, tagged with the Postgres type they came from.
    Opaque(Opaque),
}

impl Decoding {
    /// Decides what Arrow column a Postgres column becomes.
    #[expect(clippy::too_many_lines, reason = "handles many types")]
    fn new(column: &PgColumn) -> Result<Self> {
        let (name, typmod, pg_type) = (column.name(), column.type_modifier(), column.type_());
        Ok(match *pg_type {
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
            PgType::JSON => Self::Json,
            PgType::JSONB => Self::Jsonb,
            PgType::NUMERIC => Self::numeric(typmod, name)?,
            PgType::INT4_RANGE => Self::Range(Box::new(Self::Int4)),
            PgType::INT8_RANGE => Self::Range(Box::new(Self::Int8)),
            PgType::DATE_RANGE => Self::Range(Box::new(Self::Date)),
            PgType::TS_RANGE => Self::Range(Box::new(Self::Timestamp)),
            PgType::TSTZ_RANGE => Self::Range(Box::new(Self::Timestamptz)),
            // A range constrains no precision on its bounds, so they can only be bare.
            PgType::NUM_RANGE => Self::Range(Box::new(Self::numeric(BARE_NUMERIC_TYPMOD, name)?)),
            _ => Self::extension(pg_type, typmod, name),
        })
    }

    /// Decides for types whose OID differs per database, so they match on kind or name instead.
    fn extension(pg_type: &PgType, typmod: i32, name: &str) -> Self {
        match (pg_type.kind(), pg_type.name()) {
            (Kind::Enum(_), _) | (_, CITEXT) => Self::Text,
            // `geoarrow.wkb` holds EWKB, so the bytes pass through untouched, SRID per value and all.
            (_, GEOMETRY) => Self::Geo(geoarrow::planar(geoarrow::srid(typmod))),
            (_, GEOGRAPHY) => Self::Geo(geoarrow::spherical(geoarrow::srid(typmod))),
            _ => {
                warn!(
                    target: "postgres::source",
                    column = name,
                    "no Arrow mapping for Postgres type `{}` (oid {}); \
                     passing its bytes through as opaque binary",
                    pg_type.name(),
                    pg_type.oid()
                );
                Self::Opaque(Opaque::new(pg_type.name(), VENDOR))
            }
        }
    }

    /// Decodes a `numeric` typmod into the `Decimal128` its values are restated at.
    fn numeric(typmod: i32, name: &str) -> Result<Self> {
        if typmod == BARE_NUMERIC_TYPMOD {
            let (precision, scale) = BARE_NUMERIC;
            warn!(
                target: "postgres::source",
                column = name,
                "`numeric` without declared precision; mapping to \
                 Decimal128({precision}, {scale}) and rounding beyond {scale} decimals"
            );
            return Ok(Self::Numeric { precision, scale });
        }

        Self::declared_numeric(typmod)
    }

    /// Decodes the precision and scale a `numeric(p,s)` typmod packs, if `Decimal128` can hold them.
    fn declared_numeric(typmod: i32) -> Result<Self> {
        // `numeric_typmod_precision`/`numeric_typmod_scale`, minus `VARHDRSZ`; the XOR sign-extends the
        // 11-bit scale, which PG 15+ allows to be negative.
        // https://github.com/postgres/postgres/blob/REL_17_10/src/backend/utils/adt/numeric.c#L925
        let packed = typmod.wrapping_sub(VARHDRSZ);
        let precision = (packed >> 16) & 0xffff;
        let scale = ((packed & 0x7ff) ^ 0x400).wrapping_sub(0x400);

        // PG holds 1000 digits to Arrow's 38, and PG 15+ lets scale go negative or past precision.
        match (u8::try_from(precision), u8::try_from(scale)) {
            (Ok(digits), Ok(decimals))
                if digits <= DECIMAL128_MAX_PRECISION && decimals <= digits =>
            {
                Ok(Self::Numeric {
                    precision: digits,
                    scale: decimals,
                })
            }
            _ => Err(TransferredError::in_source(format!(
                "`numeric({precision},{scale})` is not supported: it needs at most \
                 {DECIMAL128_MAX_PRECISION} digits and a scale from 0 to its precision"
            ))),
        }
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
            Self::Text | Self::Json | Self::Jsonb => ArrowType::Utf8,
            Self::Bytea | Self::Geo(_) | Self::Opaque(_) => ArrowType::Binary,
            Self::Uuid => ArrowType::FixedSizeBinary(UUID_BYTES),
            Self::Date => ArrowType::Date32,
            Self::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, None),
            Self::Timestamptz => ArrowType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            Self::Interval => ArrowType::Interval(IntervalUnit::MonthDayNano),
            &Self::Numeric { precision, scale } => {
                ArrowType::Decimal128(precision, scale.cast_signed())
            }
            Self::Range(bounds) => ArrowType::Struct(PgRange::fields(bounds.arrow_type())),
        }
    }

    /// Nullable Arrow field for the column, carrying whatever extension type tags its values.
    fn field(&self, name: &str) -> Result<ArrowField> {
        let mut field = ArrowField::new(name, self.arrow_type(), true);

        match self {
            Self::Uuid => field.try_with_extension_type(Uuid)?,
            Self::Json | Self::Jsonb => field.try_with_extension_type(Json::default())?,
            Self::Geo(wkb) => field.try_with_extension_type(wkb.clone())?,
            Self::Opaque(opaque) => field.try_with_extension_type(opaque.clone())?,
            Self::Range(_) => field.try_with_extension_type(PgRange)?,
            Self::Bool
            | Self::Int2
            | Self::Int4
            | Self::Int8
            | Self::Float4
            | Self::Float8
            | Self::Text
            | Self::Bytea
            | Self::Date
            | Self::Timestamp
            | Self::Timestamptz
            | Self::Interval
            | Self::Numeric { .. } => {}
        }

        Ok(field)
    }

    /// Builds the column's array from its cells, each in Postgres binary form; `None` is a NULL.
    #[expect(clippy::too_many_lines, reason = "handles many types")]
    fn array(&self, cells: &[Cell<'_>]) -> Result<ArrayRef> {
        Ok(match self {
            Self::Bool => Arc::new(decoded::<BooleanArray, bool, _>(&PgType::BOOL, cells, Ok)?),
            Self::Int2 => Arc::new(decoded::<Int16Array, i16, _>(&PgType::INT2, cells, Ok)?),
            Self::Int4 => Arc::new(decoded::<Int32Array, i32, _>(&PgType::INT4, cells, Ok)?),
            Self::Int8 => Arc::new(decoded::<Int64Array, i64, _>(&PgType::INT8, cells, Ok)?),
            Self::Float4 => Arc::new(decoded::<Float32Array, f32, _>(&PgType::FLOAT4, cells, Ok)?),
            Self::Float8 => Arc::new(decoded::<Float64Array, f64, _>(&PgType::FLOAT8, cells, Ok)?),
            // `json` is its text, and every Postgres text type sends its own UTF-8.
            Self::Text | Self::Json => {
                Arc::new(decoded::<StringArray, &str, _>(&PgType::TEXT, cells, Ok)?)
            }
            Self::Jsonb => Arc::new(decoded::<StringArray, &[u8], _>(
                &PgType::JSONB,
                cells,
                |document| str::from_utf8(jsonb(document)?).map_err(TransferredError::in_source),
            )?),
            Self::Bytea | Self::Geo(_) | Self::Opaque(_) => {
                Arc::new(cells.iter().copied().collect::<BinaryArray>())
            }
            Self::Uuid => {
                let uuids = decoded::<Vec<_>, uuid::Uuid, _>(&PgType::UUID, cells, |uuid| {
                    Ok(uuid.into_bytes())
                })?;
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    uuids.into_iter(),
                    UUID_BYTES,
                )?)
            }
            Self::Date => Arc::new(decoded::<Date32Array, NaiveDate, _>(
                &PgType::DATE,
                cells,
                |date| Ok(Date32Type::from_naive_date(date)),
            )?),
            Self::Timestamp => Arc::new(decoded::<TimestampMicrosecondArray, NaiveDateTime, _>(
                &PgType::TIMESTAMP,
                cells,
                |timestamp| Ok(timestamp.and_utc().timestamp_micros()),
            )?),
            Self::Timestamptz => Arc::new(
                decoded::<TimestampMicrosecondArray, DateTime<Utc>, _>(
                    &PgType::TIMESTAMPTZ,
                    cells,
                    |timestamp| Ok(timestamp.timestamp_micros()),
                )?
                .with_timezone(UTC),
            ),
            Self::Interval => Arc::new(decoded::<IntervalMonthDayNanoArray, PgInterval, _>(
                &PgType::INTERVAL,
                cells,
                month_day_nano,
            )?),
            &Self::Numeric { precision, scale } => Arc::new(
                decoded::<Decimal128Array, Decimal, _>(&PgType::NUMERIC, cells, |decimal| {
                    decimal_units(decimal, scale)
                })?
                .with_precision_and_scale(precision, scale.cast_signed())?,
            ),
            Self::Range(bounds) => range_array(bounds, cells)?,
        })
    }
}

/// Builds a range column: both bounds through the element's own decoding, then the three tag bits.
fn range_array(bounds: &Decoding, cells: &[Cell<'_>]) -> Result<ArrayRef> {
    let ranges = cells
        .iter()
        .map(|cell| cell.map(range_from_sql).transpose())
        .collect::<result::Result<Vec<_>, _>>()
        .map_err(TransferredError::in_source)?;

    let (lower, upper): (Vec<_>, Vec<_>) = ranges
        .iter()
        .map(|range| bound_bytes(range.as_ref()))
        .unzip();
    let (lower_inc, upper_inc): (Vec<_>, Vec<_>) =
        ranges.iter().map(|range| inclusive(range.as_ref())).unzip();
    // An empty range and a SQL NULL both leave every bound null. `empty` separates them, and
    // the struct's own validity is the only thing that says a whole range was NULL.
    let empty = ranges
        .iter()
        .map(|range| matches!(range, Some(Range::Empty)));
    let nulls = NullBuffer::from_iter(ranges.iter().map(Option::is_some));

    let arrays: Vec<ArrayRef> = vec![
        bounds.array(&lower)?,
        bounds.array(&upper)?,
        Arc::new(BooleanArray::from(lower_inc)),
        Arc::new(BooleanArray::from(upper_inc)),
        Arc::new(empty.collect::<BooleanArray>()),
    ];
    let fields = PgRange::fields(bounds.arrow_type());

    Ok(Arc::new(StructArray::try_new(fields, arrays, Some(nulls))?))
}

/// Both bounds' bytes; an infinite bound sends none, and an empty range or a SQL NULL has no bounds.
const fn bound_bytes<'buf>(range: Option<&Range<'buf>>) -> (Cell<'buf>, Cell<'buf>) {
    match range {
        Some(Range::Nonempty(low, high)) => (bound(low), bound(high)),
        _ => (None, None),
    }
}

/// Which bounds are inclusive; an infinite one never is, and an empty range or a SQL NULL has none.
const fn inclusive(range: Option<&Range<'_>>) -> (bool, bool) {
    match range {
        Some(Range::Nonempty(low, high)) => (
            matches!(low, RangeBound::Inclusive(_)),
            matches!(high, RangeBound::Inclusive(_)),
        ),
        _ => (false, false),
    }
}

/// Decodes every cell from Postgres binary form and restates it with `convert`; `None` is a NULL.
fn decoded<'buf, Column, Postgres, Native>(
    pg_type: &PgType,
    cells: &[Cell<'buf>],
    convert: impl Fn(Postgres) -> Result<Native>,
) -> Result<Column>
where
    Column: FromIterator<Option<Native>>,
    Postgres: FromSql<'buf>,
{
    cells
        .iter()
        .map(|cell| {
            let parsed = cell
                .map(|bytes| Postgres::from_sql(pg_type, bytes))
                .transpose()
                .map_err(TransferredError::in_source)?;
            parsed.map(&convert).transpose()
        })
        .collect()
}

/// Strips the format version byte `jsonb` leads with, leaving the document text.
fn jsonb(bytes: &[u8]) -> Result<&[u8]> {
    match bytes.split_first() {
        Some((1, text)) => Ok(text),
        _ => Err(TransferredError::in_source(
            "`jsonb` arrived in an encoding version other than 1",
        )),
    }
}

/// A bound's bytes; `None` is an infinite bound, the only kind Postgres sends no value for.
const fn bound<'buf>(bound: &RangeBound<Cell<'buf>>) -> Cell<'buf> {
    match bound {
        RangeBound::Inclusive(bytes) | RangeBound::Exclusive(bytes) => *bytes,
        RangeBound::Unbounded => None,
    }
}

/// Restates a decimal as an integer count of `10^-scale` units, as Arrow `Decimal128` stores it.
fn decimal_units(mut decimal: Decimal, scale: u8) -> Result<i128> {
    let places = u32::from(scale);
    decimal.rescale(places);
    if decimal.scale() != places {
        return Err(TransferredError::in_source(format!(
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
            TransferredError::in_source(
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
    use arrow::array::AsArray as _;
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    use arrow_schema::extension::ExtensionType as _;

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
    fn decode_range(bytes: Cell<'_>) -> Result<StructArray> {
        Ok(int4_range().array(&[bytes])?.as_struct().clone())
    }

    /// A one-row range struct as Arrow prints it, every field by name.
    fn text(range: &StructArray) -> String {
        let options = FormatOptions::default().with_null("NULL");
        ArrayFormatter::try_new(range, &options)
            .unwrap()
            .value(0)
            .to_string()
    }

    #[test]
    fn decodes_a_bounded_range() {
        let range = decode_range(Some(&BOUNDED)).unwrap();

        assert_eq!(
            text(&range),
            "{lower: 1, upper: 6, lower_inc: true, upper_inc: false, empty: false}"
        );
    }

    /// An infinite bound is a null bound, and neither infinite bound counts as inclusive.
    #[test]
    fn decodes_an_unbounded_range() {
        let range = decode_range(Some(&UNBOUNDED)).unwrap();

        assert_eq!(
            text(&range),
            "{lower: NULL, upper: NULL, lower_inc: false, upper_inc: false, empty: false}"
        );
    }

    /// Empty is the one state the bounds cannot express, which is why it gets a field of its own.
    #[test]
    fn decodes_an_empty_range() {
        let range = decode_range(Some(&EMPTY_RANGE)).unwrap();

        assert_eq!(
            text(&range),
            "{lower: NULL, upper: NULL, lower_inc: false, upper_inc: false, empty: true}"
        );
    }

    /// A SQL NULL range carries no tag at all, so it must not arrive looking `empty`.
    #[test]
    fn separates_a_null_range_from_an_empty_one() {
        let range = decode_range(None).unwrap();

        assert_eq!(text(&range), "NULL");
        assert!(!range.column_by_name("empty").unwrap().as_boolean().value(0));
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
    const NUMERIC_5_10: i32 = 327_694;

    /// Largest mantissa `rust_decimal` can hold: 2^96 - 1, with no room to zero-pad.
    const U96_MAX: Decimal = Decimal::from_parts(u32::MAX, u32::MAX, u32::MAX, false, 0);

    /// Arrow type a `numeric` column declared with `typmod` lands in.
    fn numeric_type(typmod: i32) -> Result<ArrowType> {
        Decoding::numeric(typmod, "amount").map(|decoding| decoding.arrow_type())
    }

    #[test]
    fn typmod_decodes_declared_precision_and_scale() {
        assert_eq!(
            numeric_type(NUMERIC_18_4).unwrap(),
            ArrowType::Decimal128(18, 4)
        );
        assert_eq!(
            numeric_type(NUMERIC_38_9).unwrap(),
            ArrowType::Decimal128(38, 9)
        );
    }

    #[test]
    fn bare_numeric_defaults_to_bq_numeric_shape() {
        assert_eq!(
            numeric_type(NUMERIC_BARE).unwrap(),
            ArrowType::Decimal128(38, 9)
        );
    }

    /// PG 15+ allows negative scale; the decode must not read it as a large positive one.
    #[test]
    fn typmod_rejects_negative_scale_by_its_value() {
        let error = numeric_type(NUMERIC_5_NEG2).unwrap_err();
        assert!(format!("{error:?}").contains("numeric(5,-2)"));
    }

    #[test]
    fn typmod_rejects_precision_past_decimal128() {
        assert!(numeric_type(NUMERIC_1000_500).is_err());
    }

    /// PG takes `numeric(5,10)`; Arrow `Decimal128` does not, and would build a broken array from it.
    #[test]
    fn typmod_rejects_scale_wider_than_precision() {
        assert!(numeric_type(NUMERIC_5_10).is_err());
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
