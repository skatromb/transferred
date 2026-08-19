//! Postgres → Arrow type mapping. One `Decoding` variant per supported type; mirror of `arrow_to_pg`.
//!
//! A statement's columns map once into `ColumnDecoder`s; every value decodes itself through its
//! `Decoding`, straight into the Arrow builder its column's array comes out of.

use std::any::type_name;
use std::error::Error as StdError;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, IntervalMonthDayNanoBuilder, RecordBatch, StringBuilder, StructBuilder,
    TimestampMicrosecondBuilder, make_builder,
};
use arrow::datatypes::Date32Type;
use arrow_schema::extension::{Json, Opaque, Uuid};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, IntervalUnit, Schema, TimeUnit};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use pg_interval::Interval as PgInterval;
use postgres_protocol::types::{Range, RangeBound, range_from_sql};
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
use crate::pg_range::PgRange;

/// PG stores `timestamptz` as UTC; the original client offset is not retained.
const UTC: &str = "UTC";

/// The `arrow.opaque` fallback's `vendor_name`: the system an unmapped type came from.
const VENDOR: &str = "PostgreSQL";

/// Bytes an Arrow `uuid` holds, which is also what PG sends.
const UUID_BYTES: i32 = 16;

/// Arrow schema + per-column decoders, mapped once from PG column metadata.
pub struct Decoder {
    schema: Arc<Schema>,
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
            schema: Arc::new(Schema::new(fields)),
            columns,
        })
    }

    /// Appends one row, each field still exactly as Postgres sent it.
    pub fn append_row(&mut self, row: &BinaryCopyOutRow) -> Result<()> {
        for (at, column) in self.columns.iter_mut().enumerate() {
            let raw: Raw = row.try_get(at).map_err(TransferredError::source)?;
            column.append(raw.0)?;
        }

        Ok(())
    }

    /// Takes the rows appended so far as a `RecordBatch`, leaving the builders empty again.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let arrays = self.columns.iter_mut().map(ColumnDecoder::finish).collect();

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// A field's bytes exactly as Postgres sent them, whatever its type; `None` is a NULL. Every
/// `Decoding` reads its own binary form, so the row must hand them over untouched.
struct Raw<'a>(Option<&'a [u8]>);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _: &PgType,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(Self(Some(raw)))
    }

    fn from_sql_null(_: &PgType) -> std::result::Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(Self(None))
    }

    fn accepts(_: &PgType) -> bool {
        true
    }
}

/// One column of the source: the Arrow field it becomes, what its values are, where they land.
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

/// What a column is in Arrow terms; one decision per column, answering for both its Arrow field and
/// the binary form of its values, so the two can never disagree.
enum Decoding {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    /// `text`, `varchar`, `enum` and `citext` alike: PG sends every one of them as its own UTF-8.
    Text,
    /// `json` and `jsonb` differ on the wire — `jsonb` leads with a version byte — so the decode
    /// needs the type it came from, not just the shape it lands in.
    Json(PgType),
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
    Geo(Wkb),
    /// No mapping: the bytes pass through, tagged with the Postgres type they came from.
    Opaque(Opaque),
    /// A range arrives as a tag byte plus bounds, each bound through the element's own decoding.
    Range(Box<Decoding>),
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
            ref json @ (PgType::JSON | PgType::JSONB) => Self::Json(json.clone()),
            PgType::NUMERIC => Self::numeric(typmod, name)?,
            // The six built-in ranges, each a pair of bounds over one of the scalars above. A range
            // constrains no precision on its bounds, so a `numrange` can only carry bare ones.
            PgType::INT4_RANGE => Self::Range(Box::new(Self::Int4)),
            PgType::INT8_RANGE => Self::Range(Box::new(Self::Int8)),
            PgType::DATE_RANGE => Self::Range(Box::new(Self::Date)),
            PgType::TS_RANGE => Self::Range(Box::new(Self::Timestamp)),
            PgType::TSTZ_RANGE => Self::Range(Box::new(Self::Timestamptz)),
            PgType::NUM_RANGE => Self::Range(Box::new(Self::numeric(BARE_NUMERIC_TYPMOD, name)?)),
            // PG sends both as their own UTF-8 text. `citext` has no fixed OID, so goes by name.
            ref text if matches!(text.kind(), Kind::Enum(_)) || text.name() == CITEXT => Self::Text,
            // `PostGIS` carries no fixed OID, so its types answer to a name. Their binary form is
            // EWKB, which `geoarrow.wkb` accepts as-is, SRID per value and all.
            ref geo if geo.name() == GEOMETRY => Self::Geo(Wkb::planar(geo_srid(typmod))),
            // The same bytes, but `geography` bends its edges around the globe.
            ref geo if geo.name() == GEOGRAPHY => Self::Geo(Wkb::spherical(geo_srid(typmod))),
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
            Self::Text | Self::Json(_) => ArrowType::Utf8,
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
            Self::Json(_) => field.try_with_extension_type(Json::default())?,
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
            Self::Json(pg_type) => cast::<StringBuilder>(builder)?.append_option(
                decode::<PgJson<&RawValue>>(pg_type, bytes)?.map(|json| json.0.get()),
            ),
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

/// Reads a text value's bytes, which every Postgres text type sends as its own UTF-8.
fn text(bytes: Option<&[u8]>) -> Result<Option<&str>> {
    bytes
        .map(str::from_utf8)
        .transpose()
        .map_err(TransferredError::source)
}

/// A bound's bytes, `None` when the bound is infinite. Postgres rejects a NULL bound, so a bound
/// with no value is one it did not send.
fn bound<'a>(bound: &RangeBound<Option<&'a [u8]>>) -> Option<&'a [u8]> {
    match bound {
        RangeBound::Inclusive(value) | RangeBound::Exclusive(value) => *value,
        RangeBound::Unbounded => None,
    }
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
}
