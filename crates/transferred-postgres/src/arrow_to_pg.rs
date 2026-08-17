//! Arrow → Postgres type mapping. One `Encoding` variant per supported type; mirror of `pg_to_arrow`.
//!
//! A schema maps once into `ColumnEncoder`s; every value writes itself through its `Encoding`.

use std::any::type_name;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::extension::{ExtensionType, Json, Uuid};
use arrow_schema::{DataType as ArrowType, Field as ArrowField, IntervalUnit, Schema, TimeUnit};
use bytes::BytesMut;
use postgres_protocol::IsNull as ProtocolIsNull;
use postgres_protocol::escape::escape_identifier;
use postgres_protocol::types::{RangeBound, empty_range_to_sql, range_to_sql};
use tokio_postgres::types::{IsNull, ToSql, Type as PgType};
use transferred_core::{Result, TransferredError};

use crate::convert::{
    GEOGRAPHY, GEOMETRY, pg_date, pg_interval, pg_json, pg_numeric, pg_timestamp, pg_uuid,
};
use crate::geoarrow::Wkb;
use crate::pg_range::{LOWER, PgRange};

/// Postgres column definitions + value encoders, mapped once from an Arrow schema.
pub struct Encoder {
    schema: Schema,
    columns: Vec<ColumnEncoder>,
}

impl Encoder {
    /// Maps an Arrow schema onto Postgres columns. All columns nullable.
    pub fn new(schema: &Schema) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                Ok(ColumnEncoder {
                    name: escape_identifier(field.name()),
                    encoding: Encoding::new(field)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema: schema.clone(), // todo: if we clone, maybe pass by value then?
            columns,
        })
    }

    /// Column-type list for `CREATE TABLE`, quoted and comma-separated.
    /// E.g. `"id" int4, "total" numeric(38,9)`.
    pub fn declarations(&self) -> String {
        self.columns
            .iter()
            .map(|column| format!("{} {}", column.name, column.encoding.sql_type()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Checks a batch against the mapped schema, then hands back the column encoders.
    pub fn columns(&self, batch: &RecordBatch) -> Result<&[ColumnEncoder]> {
        // The table was created from the first batch, so a later partition may not fit it.
        if batch.schema().fields() != self.schema.fields() {
            return Err(TransferredError::destination(format!(
                "batch schema `{}` does not match the target table's `{}`",
                batch.schema(),
                self.schema
            )));
        }

        Ok(&self.columns)
    }
}

/// One column of the target table: its quoted name and the encoding of its values.
pub struct ColumnEncoder {
    name: String,
    encoding: Encoding,
}

impl ColumnEncoder {
    /// Writes value `row` of `array` into the COPY buffer, or reports it null.
    pub fn write(&self, array: &dyn Array, row_num: usize, buf: &mut BytesMut) -> Result<IsNull> {
        self.encoding.write(array, row_num, buf).map_err(|error| {
            TransferredError::destination(format!("column {}: {error}", self.name))
        })
    }
}

/// What a column is in Postgres terms; one decision per column, answering for both its
/// `CREATE TABLE` type and the binary form of its values, so the two can never disagree.
enum Encoding {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Json,
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
    /// `PostGIS` values write as `bytea`-framed WKB; only the DDL names the geo type.
    Geo {
        sql: String,
    },
    /// A range writes as a tag byte plus bounds, each bound through the element's own encoding.
    Range {
        sql: String,
        element: Box<Encoding>,
    },
}

impl Encoding {
    /// Decides what a Postgres column an Arrow field becomes.
    fn new(field: &ArrowField) -> Result<Self> {
        let extension = field.extension_type_name();
        Ok(match field.data_type() {
            ArrowType::Boolean => Self::Bool,
            ArrowType::Int16 => Self::Int2,
            ArrowType::Int32 => Self::Int4,
            ArrowType::Int64 => Self::Int8,
            ArrowType::Float32 => Self::Float4,
            ArrowType::Float64 => Self::Float8,
            // `json` stores the document verbatim; `jsonb` would reorder keys and drop whitespace.
            ArrowType::Utf8 if extension == Some(Json::NAME) => Self::Json,
            ArrowType::Utf8 => Self::Text,
            // `PostGIS` gets its OIDs per database, so no `PgType` names it and only the DDL can.
            ArrowType::Binary if extension == Some(Wkb::NAME) => Self::Geo {
                sql: geo_sql_type(field)?,
            },
            // Plain bytes, and `arrow.opaque`, whose type name the destination deliberately drops.
            ArrowType::Binary => Self::Bytea,
            ArrowType::FixedSizeBinary(16) if extension == Some(Uuid::NAME) => Self::Uuid,
            ArrowType::Date32 => Self::Date,
            ArrowType::Timestamp(TimeUnit::Microsecond, None) => Self::Timestamp,
            // Arrow timestamps are UTC instants whatever the zone name, so the zone needs no lookup.
            ArrowType::Timestamp(TimeUnit::Microsecond, Some(_)) => Self::Timestamptz,
            ArrowType::Interval(IntervalUnit::MonthDayNano) => Self::Interval,
            &ArrowType::Decimal128(precision, scale) => Self::Numeric { precision, scale },
            ArrowType::Struct(_) if extension == Some(PgRange::NAME) => {
                let bounds_type =
                    PgRange::type_of(field.data_type()).map_err(TransferredError::destination)?;
                let element = Self::new(&ArrowField::new(LOWER, bounds_type.clone(), true))?;

                Self::Range {
                    sql: range_sql(&element)?.to_owned(),
                    element: Box::new(element),
                }
            }
            other => {
                return Err(TransferredError::destination(format!(
                    "Arrow type `{other}` is not supported by the Postgres destination in 0.1"
                )));
            }
        })
    }

    /// Type half of the column's DDL, e.g. `numeric(38,9)`; the test suite pins every name.
    fn sql_type(&self) -> String {
        let name = match self {
            Self::Bool => "bool",
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Float4 => "float4",
            Self::Float8 => "float8",
            Self::Text => "text",
            Self::Json => "json",
            Self::Bytea => "bytea",
            Self::Uuid => "uuid",
            Self::Date => "date",
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamptz",
            Self::Interval => "interval",
            Self::Numeric { precision, scale } => return format!("numeric({precision},{scale})"),
            Self::Geo { sql } | Self::Range { sql, .. } => sql.as_str(),
        };

        name.to_owned()
    }

    /// Writes one value in Postgres binary form; nulls stop here, before any downcast.
    fn write(&self, array: &dyn Array, row_num: usize, buf: &mut BytesMut) -> Result<IsNull> {
        if array.is_null(row_num) {
            return Ok(IsNull::Yes);
        }

        match self {
            Self::Bool => write_sql(
                &cast::<BooleanArray>(array)?.value(row_num),
                &PgType::BOOL,
                buf,
            ),
            Self::Int2 => write_sql(
                &cast::<Int16Array>(array)?.value(row_num),
                &PgType::INT2,
                buf,
            ),
            Self::Int4 => write_sql(
                &cast::<Int32Array>(array)?.value(row_num),
                &PgType::INT4,
                buf,
            ),
            Self::Int8 => write_sql(
                &cast::<Int64Array>(array)?.value(row_num),
                &PgType::INT8,
                buf,
            ),
            Self::Float4 => write_sql(
                &cast::<Float32Array>(array)?.value(row_num),
                &PgType::FLOAT4,
                buf,
            ),
            Self::Float8 => write_sql(
                &cast::<Float64Array>(array)?.value(row_num),
                &PgType::FLOAT8,
                buf,
            ),
            Self::Text => write_sql(
                &cast::<StringArray>(array)?.value(row_num),
                &PgType::TEXT,
                buf,
            ),
            Self::Json => write_sql(
                &pg_json(cast::<StringArray>(array)?.value(row_num))?,
                &PgType::JSON,
                buf,
            ),
            // Binary COPY sends no types of its own, so `bytea` framing reaches `geometry_recv`.
            Self::Bytea | Self::Geo { .. } => write_sql(
                &cast::<BinaryArray>(array)?.value(row_num),
                &PgType::BYTEA,
                buf,
            ),
            Self::Uuid => write_sql(
                &pg_uuid(cast::<FixedSizeBinaryArray>(array)?.value(row_num))?,
                &PgType::UUID,
                buf,
            ),
            Self::Date => write_sql(
                &pg_date(cast::<Date32Array>(array)?.value(row_num))?,
                &PgType::DATE,
                buf,
            ),
            Self::Timestamp => write_sql(
                &pg_timestamp(cast::<TimestampMicrosecondArray>(array)?.value(row_num))?
                    .naive_utc(),
                &PgType::TIMESTAMP,
                buf,
            ),
            Self::Timestamptz => write_sql(
                &pg_timestamp(cast::<TimestampMicrosecondArray>(array)?.value(row_num))?,
                &PgType::TIMESTAMPTZ,
                buf,
            ),
            Self::Interval => write_sql(
                &pg_interval(cast::<IntervalMonthDayNanoArray>(array)?.value(row_num))?,
                &PgType::INTERVAL,
                buf,
            ),
            Self::Numeric { scale, .. } => write_sql(
                &pg_numeric(cast::<Decimal128Array>(array)?.value(row_num), *scale)?,
                &PgType::NUMERIC,
                buf,
            ),
            Self::Range { element, .. } => {
                write_range(element, cast::<StructArray>(array)?, row_num, buf)
            }
        }
    }
}

/// Writes one value in Postgres binary form, turning a `ToSql` failure into a destination error.
fn write_sql(value: &impl ToSql, pg_type: &PgType, buf: &mut BytesMut) -> Result<IsNull> {
    value
        .to_sql(pg_type, buf)
        .map_err(TransferredError::destination)
}

/// Downcasts an Arrow column; a mismatch is unreachable, as the encoding came from the same field.
fn cast<A: 'static>(array: &dyn Array) -> Result<&A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        TransferredError::destination(format!("column is not a {}", type_name::<A>()))
    })
}

/// Names the Postgres range over `element`; a range outside these six is defined per database.
fn range_sql(element: &Encoding) -> Result<&'static str> {
    Ok(match element {
        Encoding::Int4 => "int4range",
        Encoding::Int8 => "int8range",
        Encoding::Numeric { .. } => "numrange",
        Encoding::Date => "daterange",
        Encoding::Timestamp => "tsrange",
        Encoding::Timestamptz => "tstzrange",
        other => {
            return Err(TransferredError::destination(format!(
                "Postgres has no built-in range over `{}`",
                other.sql_type()
            )));
        }
    })
}

/// Writes one range: empty ones as their tag alone, the rest as a tag plus the finite bounds.
fn write_range(
    element: &Encoding,
    ranges: &StructArray,
    row_num: usize,
    buf: &mut BytesMut,
) -> Result<IsNull> {
    // In the order `PgRange::fields` declares them, as `PgRange::type_of` has already checked.
    let [lower, upper, lower_inc, upper_inc, empty] = ranges.columns() else {
        return Err(TransferredError::destination(format!(
            "a range column holds five children, not {}",
            ranges.num_columns()
        )));
    };

    // `PgRange::fields` declares the flags non-nullable, so they read straight off.
    if cast::<BooleanArray>(empty.as_ref())?.value(row_num) {
        empty_range_to_sql(buf);
        return Ok(IsNull::No);
    }

    let lower_inc = cast::<BooleanArray>(lower_inc.as_ref())?.value(row_num);
    let upper_inc = cast::<BooleanArray>(upper_inc.as_ref())?.value(row_num);

    range_to_sql(
        |buf| write_bound(element, lower.as_ref(), row_num, lower_inc, buf),
        |buf| write_bound(element, upper.as_ref(), row_num, upper_inc, buf),
        buf,
    )
    .map_err(TransferredError::destination)?;

    Ok(IsNull::No)
}

/// Writes a bound, reporting it infinite when its value is null: Postgres allows no NULL bound.
fn write_bound(
    element: &Encoding,
    array: &dyn Array,
    row_num: usize,
    inclusive: bool,
    buf: &mut BytesMut,
) -> std::result::Result<RangeBound<ProtocolIsNull>, Box<dyn std::error::Error + Sync + Send>> {
    // The two `IsNull`s belong to different crates; only a bound we did write reaches the protocol's.
    Ok(match element.write(array, row_num, buf)? {
        IsNull::Yes => RangeBound::Unbounded,
        IsNull::No if inclusive => RangeBound::Inclusive(ProtocolIsNull::No),
        IsNull::No => RangeBound::Exclusive(ProtocolIsNull::No),
    })
}

/// SQL type for a `geoarrow.wkb` field, constrained to its coordinate system but not to a geometry
/// subtype, which the tag says nothing about. E.g. `geography(Geometry,4326)`, or bare `geometry`.
fn geo_sql_type(field: &ArrowField) -> Result<String> {
    let wkb = field
        .try_extension_type::<Wkb>()
        .map_err(TransferredError::destination)?;

    // `geography` bends its edges around the globe; `geometry` keeps them straight.
    let name = if wkb.is_spherical() {
        GEOGRAPHY
    } else {
        GEOMETRY
    };

    Ok(match wkb.epsg() {
        Some(epsg) => format!("{name}(Geometry,{epsg})"),
        None => name.to_owned(),
    })
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::ArrayRef;

    use super::*;

    /// Every type `pg_to_arrow` produces, in fixture order, so the pair stays symmetric.
    fn source_schema() -> Schema {
        Schema::new(vec![
            ArrowField::new("b", ArrowType::Boolean, true),
            ArrowField::new("i2", ArrowType::Int16, true),
            ArrowField::new("i4", ArrowType::Int32, true),
            ArrowField::new("i8", ArrowType::Int64, true),
            ArrowField::new("f4", ArrowType::Float32, true),
            ArrowField::new("f8", ArrowType::Float64, true),
            ArrowField::new("t", ArrowType::Utf8, true),
            ArrowField::new("bin", ArrowType::Binary, true),
            ArrowField::new("d", ArrowType::Date32, true),
            ArrowField::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            ArrowField::new(
                "tstz",
                ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            ArrowField::new("iv", ArrowType::Interval(IntervalUnit::MonthDayNano), true),
            ArrowField::new("n", ArrowType::Decimal128(38, 9), true),
            ArrowField::new("u", ArrowType::FixedSizeBinary(16), true).with_extension_type(Uuid),
            ArrowField::new("j", ArrowType::Utf8, true).with_extension_type(Json::default()),
        ])
    }

    #[test]
    fn maps_every_source_type_back_to_a_pg_declaration() {
        assert_eq!(
            Encoder::new(&source_schema()).unwrap().declarations(),
            r#""b" bool, "i2" int2, "i4" int4, "i8" int8, "f4" float4, "f8" float8, "t" text, "#
                .to_owned()
                + r#""bin" bytea, "d" date, "ts" timestamp, "tstz" timestamptz, "iv" interval, "#
                + r#""n" numeric(38,9), "u" uuid, "j" json"#
        );
    }

    /// Writes the first row of a one-column batch, as the COPY stream would.
    fn write_first(field: ArrowField, array: ArrayRef) -> Result<BytesMut> {
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap();
        let mut buf = BytesMut::new();

        let encoder = Encoder::new(&schema)?;
        let column = encoder.columns(&batch)?.first().unwrap();
        column.write(batch.column(0).as_ref(), 0, &mut buf)?;
        Ok(buf)
    }

    /// An Arrow null writes no bytes: the field length alone says NULL.
    #[test]
    fn writes_nothing_for_an_arrow_null() {
        let field = ArrowField::new("i4", ArrowType::Int32, true);
        let nulls = Arc::new(Int32Array::from(vec![None::<i32>]));
        assert!(write_first(field, nulls).unwrap().is_empty());
    }

    fn range(name: &str, arrow_type: ArrowType) -> ArrowField {
        ArrowField::new(name, ArrowType::Struct(PgRange::fields(arrow_type)), true)
            .with_extension_type(PgRange)
    }

    /// Six ranges told apart by the type of their bounds alone, the tag itself carrying nothing.
    #[test]
    fn declares_range_columns_from_the_type_of_their_bounds() {
        let schema = Schema::new(vec![
            range("i4", ArrowType::Int32),
            range("i8", ArrowType::Int64),
            range("n", ArrowType::Decimal128(38, 9)),
            range("d", ArrowType::Date32),
            range("ts", ArrowType::Timestamp(TimeUnit::Microsecond, None)),
            range(
                "tstz",
                ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
        ]);

        assert_eq!(
            Encoder::new(&schema).unwrap().declarations(),
            r#""i4" int4range, "i8" int8range, "n" numrange, "d" daterange, "#.to_owned()
                + r#""ts" tsrange, "tstz" tstzrange"#
        );
    }

    /// A range over anything else is defined per database, so no fixed OID could announce it.
    #[test]
    fn rejects_a_range_postgres_has_no_built_in_for() {
        let schema = Schema::new(vec![range("t", ArrowType::Utf8)]);
        assert!(Encoder::new(&schema).is_err());
    }

    /// Extension metadata is the only thing separating `json` from `text`; without it, plain wins.
    #[test]
    fn extension_metadata_picks_the_semantic_pg_type() {
        let plain = Schema::new(vec![ArrowField::new("j", ArrowType::Utf8, true)]);
        assert_eq!(Encoder::new(&plain).unwrap().declarations(), r#""j" text"#);
    }

    /// `PostGIS` gets its OIDs per database, so only the DDL can name it: edges pick the type and
    /// the tag's coordinate system constrains it, while the geometry subtype stays free.
    #[test]
    fn declares_postgis_columns_from_the_wkb_tag() {
        let schema = Schema::new(vec![
            ArrowField::new("geom", ArrowType::Binary, true).with_extension_type(Wkb::planar(None)),
            ArrowField::new("pt", ArrowType::Binary, true)
                .with_extension_type(Wkb::planar(Some(4326))),
            ArrowField::new("geog", ArrowType::Binary, true)
                .with_extension_type(Wkb::spherical(Some(4326))),
            // Bare `geography` is not implicitly 4326: PG takes any SRID into such a column.
            ArrowField::new("bare", ArrowType::Binary, true)
                .with_extension_type(Wkb::spherical(None)),
        ]);

        assert_eq!(
            Encoder::new(&schema).unwrap().declarations(),
            r#""geom" geometry, "pt" geometry(Geometry,4326), "geog" geography(Geometry,4326), "bare" geography"#
        );
    }

    /// Binary COPY names no types itself, so `bytea` framing carries the WKB to `geography_recv`.
    #[test]
    fn writes_wkb_verbatim() {
        let wkb: &[u8] = &[1, 2, 3, 4];
        let field = ArrowField::new("geog", ArrowType::Binary, true)
            .with_extension_type(Wkb::spherical(Some(4326)));

        assert_eq!(
            write_first(field, Arc::new(BinaryArray::from(vec![wkb]))).unwrap(),
            wkb
        );
    }

    /// PG has no fixed-width binary, so 16 bytes only mean a uuid when the field says so.
    #[test]
    fn rejects_fixed_size_binary_without_the_uuid_extension() {
        let schema = Schema::new(vec![ArrowField::new(
            "u",
            ArrowType::FixedSizeBinary(16),
            true,
        )]);
        assert!(Encoder::new(&schema).is_err());
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        let schema = Schema::new(vec![ArrowField::new("u16", ArrowType::UInt16, true)]);
        assert!(Encoder::new(&schema).is_err());
    }

    /// The table is created from the first batch, so a later partition may not fit it.
    #[test]
    fn rejects_a_batch_that_does_not_match_the_mapped_schema() {
        let encoder = Encoder::new(&Schema::new(vec![ArrowField::new(
            "a",
            ArrowType::Int32,
            true,
        )]))
        .unwrap();

        let widened = Arc::new(Schema::new(vec![ArrowField::new(
            "a",
            ArrowType::Int64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(widened, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        assert!(encoder.columns(&batch).is_err());
    }

    /// Only fields drive the mapping, so writer metadata on the schema must not reject a batch.
    #[test]
    fn accepts_a_batch_differing_only_in_schema_metadata() {
        let schema = Schema::new(vec![ArrowField::new("a", ArrowType::Int32, true)]);
        let encoder = Encoder::new(&schema).unwrap();

        let tagged = Arc::new(
            schema.with_metadata(HashMap::from([("writer".to_owned(), "test".to_owned())])),
        );
        let batch =
            RecordBatch::try_new(tagged, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        assert_eq!(encoder.columns(&batch).unwrap().len(), 1);
    }

    /// Names go straight into DDL, and a Parquet field or dict key need not be a bare identifier.
    #[test]
    fn quotes_column_names_in_declarations() {
        let awkward = Schema::new(vec![
            ArrowField::new("Total Sales", ArrowType::Int32, true),
            ArrowField::new("user.id", ArrowType::Utf8, true),
        ]);
        assert_eq!(
            Encoder::new(&awkward).unwrap().declarations(),
            r#""Total Sales" int4, "user.id" text"#
        );
    }
}
