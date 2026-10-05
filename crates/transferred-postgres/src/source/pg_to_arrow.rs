//! Postgres → Arrow type mapping. One `Decoding` variant per supported type; mirror of `arrow_to_pg`.
//!
//! A statement's columns map once into `ColumnDecoder`s; every value decodes itself through its
//! `Decoding`, straight into the Arrow builder its column's array comes out of.

use std::any::type_name;
use std::mem;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, IntervalMonthDayNanoBuilder, RecordBatch, StringBuilder, StructBuilder,
    TimestampMicrosecondBuilder, make_builder,
};
use arrow::datatypes::{DECIMAL128_MAX_PRECISION, Date32Type, IntervalMonthDayNano};
use arrow_schema::extension::{Json, Opaque, Uuid};
use arrow_schema::{
    DataType as ArrowType, Field as ArrowField, IntervalUnit, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use geoarrow_schema::WkbType;
use pg_interval::Interval as PgInterval;
use postgres_protocol::types::{Range, RangeBound, range_from_sql};
use rust_decimal::Decimal;
use tokio_postgres::Column as PgColumn;
use tokio_postgres::types::{FromSql, Kind, Type as PgType};
use tracing::warn;
use transferred_core::{Result, TransferredError};

use super::BATCH_ROWS;
use super::copy_out::{Cell, Cells};
use crate::geoarrow::{self, GEOGRAPHY, GEOMETRY};
use crate::pg_range::PgRange;
use crate::{NANOS_PER_MICRO, UUID_BYTES};

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

/// The only `jsonb` binary format version PG sends.
const JSONB_VERSION: u8 = 1;

/// The `arrow.opaque` fallback's `vendor_name`: the system an unmapped type came from.
const VENDOR: &str = "PostgreSQL";

/// Whole Arrow batch decoder with schema + `ColumnDecoder` for each column.
pub(crate) struct Decoder {
    schema: ArrowSchemaRef,
    decoders: Vec<ColumnDecoder>,
}

impl Decoder {
    /// Maps a prepared statement's columns onto Arrow columns. All fields nullable.
    pub(crate) fn derive(columns: &[PgColumn]) -> Result<Self> {
        let decoders = columns
            .iter()
            .map(ColumnDecoder::new)
            .collect::<Result<Vec<_>>>()?;

        let fields: Vec<ArrowField> = decoders
            .iter()
            .map(|decoder| decoder.decoding.field(&decoder.name))
            .collect::<Result<_>>()?;

        let schema = ArrowSchema::new(fields).into();

        Ok(Self { schema, decoders })
    }

    /// Decodes a batch row by row, so each row's bytes are read once, while still in cache.
    pub(crate) fn decode(&mut self, rows: &[Bytes]) -> Result<RecordBatch> {
        for row in rows {
            let mut cells = Cells::new(row, self.decoders.len())?;
            for decoder in &mut self.decoders {
                decoder.append(cells.take()?)?;
            }
        }

        let arrays = self
            .decoders
            .iter_mut()
            .map(ColumnDecoder::finish)
            .collect();

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// One column: its name, its `Decoding`, and the Arrow builder its values go into.
struct ColumnDecoder {
    name: String,
    decoding: Decoding,
    builder: Box<dyn ArrayBuilder>,
}

impl ColumnDecoder {
    /// Starts a column and its builder with corresponding `Decoding` for the schema field.
    fn new(column: &PgColumn) -> Result<Self> {
        let decoding = Decoding::new(column)?;
        let builder = decoding.builder();

        Ok(Self {
            name: column.name().to_owned(),
            decoding,
            builder,
        })
    }

    /// Decodes one cell onto the end of the column.
    fn append(&mut self, cell: Cell) -> Result<()> {
        self.decoding
            .append(&mut *self.builder, cell)
            .map_err(|error| TransferredError::in_source(format!("column {}: {error}", self.name)))
    }

    /// Takes the values appended so far as an array, swapping in a fresh builder for the next batch.
    fn finish(&mut self) -> ArrayRef {
        mem::replace(&mut self.builder, self.decoding.builder()).finish()
    }
}

/// Decoding for a given type of a column, answering for both its Arrow field and its values' binary form.
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

    /// Empty builder for the column's values, sized for a full batch of rows.
    fn builder(&self) -> Box<dyn ArrayBuilder> {
        make_builder(&self.arrow_type(), BATCH_ROWS)
    }

    /// Arrow schema field for the column: nullable, tagged with its extension type if it has one.
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

    /// Decodes one cell from Postgres binary form onto `builder`; `None` is a NULL.
    #[expect(clippy::too_many_lines, reason = "handles many types")]
    fn append(&self, builder: &mut dyn ArrayBuilder, cell: Cell) -> Result<()> {
        match self {
            Self::Bool => {
                cast::<BooleanBuilder>(builder)?.append_option(decode(&PgType::BOOL, cell)?);
            }
            Self::Int2 => {
                cast::<Int16Builder>(builder)?.append_option(decode(&PgType::INT2, cell)?);
            }
            Self::Int4 => {
                cast::<Int32Builder>(builder)?.append_option(decode(&PgType::INT4, cell)?);
            }
            Self::Int8 => {
                cast::<Int64Builder>(builder)?.append_option(decode(&PgType::INT8, cell)?);
            }
            Self::Float4 => {
                cast::<Float32Builder>(builder)?.append_option(decode(&PgType::FLOAT4, cell)?);
            }
            Self::Float8 => {
                cast::<Float64Builder>(builder)?.append_option(decode(&PgType::FLOAT8, cell)?);
            }
            Self::Text | Self::Json => {
                cast::<StringBuilder>(builder)?.append_option(cell.map(utf8).transpose()?);
            }
            Self::Jsonb => {
                let text = cell.map(|document| utf8(jsonb(document)?)).transpose()?;
                cast::<StringBuilder>(builder)?.append_option(text);
            }
            Self::Bytea | Self::Geo(_) | Self::Opaque(_) => {
                cast::<BinaryBuilder>(builder)?.append_option(cell);
            }
            Self::Uuid => {
                let uuids = cast::<FixedSizeBinaryBuilder>(builder)?;
                match decode::<uuid::Uuid>(&PgType::UUID, cell)? {
                    Some(uuid) => uuids.append_value(uuid.into_bytes())?,
                    None => uuids.append_null(),
                }
            }
            Self::Date => cast::<Date32Builder>(builder)?.append_option(
                decode::<NaiveDate>(&PgType::DATE, cell)?.map(Date32Type::from_naive_date),
            ),
            Self::Timestamp => cast::<TimestampMicrosecondBuilder>(builder)?.append_option(
                decode::<NaiveDateTime>(&PgType::TIMESTAMP, cell)?
                    .map(|timestamp| timestamp.and_utc().timestamp_micros()),
            ),
            Self::Timestamptz => cast::<TimestampMicrosecondBuilder>(builder)?.append_option(
                decode::<DateTime<Utc>>(&PgType::TIMESTAMPTZ, cell)?
                    .map(|timestamp| timestamp.timestamp_micros()),
            ),
            Self::Interval => cast::<IntervalMonthDayNanoBuilder>(builder)?.append_option(
                decode(&PgType::INTERVAL, cell)?
                    .map(month_day_nano)
                    .transpose()?,
            ),
            &Self::Numeric { scale, .. } => cast::<Decimal128Builder>(builder)?.append_option(
                decode(&PgType::NUMERIC, cell)?
                    .map(|decimal| decimal_units(decimal, scale))
                    .transpose()?,
            ),
            Self::Range(bounds) => append_range(bounds, cast(builder)?, cell)?,
        }

        Ok(())
    }
}

/// Appends a range: both bounds through the element's own decoding, then the three tag bits.
fn append_range(bounds: &Decoding, range: &mut StructBuilder, cell: Cell) -> Result<()> {
    let parsed = cell
        .map(range_from_sql)
        .transpose()
        .map_err(TransferredError::in_source)?;
    let [lower, upper, lower_inc, upper_inc, empty] = range.field_builders_mut() else {
        return Err(TransferredError::in_source(
            "a `transferred.pg_range` column does not build the five fields it declares",
        ));
    };

    let (low, high) = bound_bytes(parsed.as_ref());
    let (low_inclusive, high_inclusive) = inclusive(parsed.as_ref());
    bounds.append(&mut **lower, low)?;
    bounds.append(&mut **upper, high)?;
    tag(lower_inc, low_inclusive)?;
    tag(upper_inc, high_inclusive)?;
    // An empty range and a SQL NULL both leave every bound null. `empty` separates them, and
    // the struct's own validity is the only thing that says a whole range was NULL.
    tag(empty, matches!(parsed, Some(Range::Empty)))?;
    range.append(parsed.is_some());

    Ok(())
}

/// Both bounds' bytes; an infinite bound sends none, and an empty range or a SQL NULL has no bounds.
const fn bound_bytes<'buf>(range: Option<&Range<'buf>>) -> (Cell<'buf>, Cell<'buf>) {
    match range {
        Some(Range::Nonempty(low, high)) => (bound(low), bound(high)),
        _ => (None, None),
    }
}

/// A bound's bytes; `None` is an infinite bound, the only kind Postgres sends no value for.
const fn bound<'buf>(bound: &RangeBound<Cell<'buf>>) -> Cell<'buf> {
    match bound {
        RangeBound::Inclusive(bytes) | RangeBound::Exclusive(bytes) => *bytes,
        RangeBound::Unbounded => None,
    }
}

/// Which bounds are inclusive; an infinite one never is, and an empty range or a SQL NULL has none.
const fn inclusive(range: Option<&Range>) -> (bool, bool) {
    match range {
        Some(Range::Nonempty(low, high)) => (
            matches!(low, RangeBound::Inclusive(_)),
            matches!(high, RangeBound::Inclusive(_)),
        ),
        _ => (false, false),
    }
}

/// Appends one of a range's tag bits.
fn tag(builder: &mut Box<dyn ArrayBuilder>, set: bool) -> Result<()> {
    cast::<BooleanBuilder>(&mut **builder)?.append_value(set);

    Ok(())
}

/// Text as Postgres sends it: every text type, and `json`, arrives as its own UTF-8.
fn utf8(bytes: &[u8]) -> Result<&str> {
    str::from_utf8(bytes).map_err(TransferredError::in_source)
}

/// Strips the format version byte `jsonb` leads with, leaving the document text.
fn jsonb(bytes: &[u8]) -> Result<&[u8]> {
    match bytes.split_first() {
        Some((&JSONB_VERSION, text)) => Ok(text),
        _ => Err(TransferredError::in_source(format!(
            "`jsonb` arrived in an encoding version other than {JSONB_VERSION}"
        ))),
    }
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

/// Decodes a cell from Postgres binary form; `None` is a NULL.
fn decode<'buf, Postgres: FromSql<'buf>>(
    pg_type: &PgType,
    cell: Cell<'buf>,
) -> Result<Option<Postgres>> {
    cell.map(|bytes| Postgres::from_sql(pg_type, bytes))
        .transpose()
        .map_err(TransferredError::in_source)
}

/// The concrete builder behind `builder`, which came off the column's own field.
fn cast<Builder: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> Result<&mut Builder> {
    builder
        .as_any_mut()
        .downcast_mut::<Builder>()
        .ok_or_else(|| {
            TransferredError::in_source(format!("column is not a {}", type_name::<Builder>()))
        })
}

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray as _, StructArray};
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
    fn decode_range(bytes: Cell) -> Result<StructArray> {
        let decoding = int4_range();
        let mut builder = make_builder(&decoding.arrow_type(), 1);
        decoding.append(&mut *builder, bytes)?;
        Ok(builder.finish().as_struct().clone())
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
