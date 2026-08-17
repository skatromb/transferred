//! Arrow → Postgres type mapping. One table row per supported type; mirror of `pg_to_arrow`.
//!
//! A schema resolves once into `PgColumn`s, each holding a `BindColumn`; every batch runs those
//! into one `WriteValue` per column; every row calls a `WriteValue` to write itself.

use std::any::type_name;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
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

/// Postgres column definitions + per-column encoders, derived once from an Arrow schema.
pub struct ArrowToPg {
    schema: Schema,
    columns: Vec<PgColumn>,
}

impl ArrowToPg {
    /// Derives Postgres columns and encoders from an Arrow schema. All columns nullable.
    pub fn derive(schema: &Schema) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| field.to_pg_column())
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema: schema.clone(),
            columns,
        })
    }

    /// Column-type list for `CREATE TABLE`, quoted and comma-separated.
    /// E.g. `"id" int4, "total" numeric(38,9)`.
    pub fn declarations(&self) -> String {
        self.columns
            .iter()
            .map(|column| column.sql.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Binds every column to a batch, ready to write any of its rows into the COPY buffer.
    pub fn bind(&self, batch: &RecordBatch) -> Result<Vec<WriteValue>> {
        // The table was created from the first batch, so a later partition may not fit it.
        if batch.schema().fields() != self.schema.fields() {
            return Err(TransferredError::destination(format!(
                "batch schema `{}` does not match the target table's `{}`",
                batch.schema(),
                self.schema
            )));
        }

        self.columns
            .iter()
            .zip(batch.columns())
            .map(|(column, array)| {
                (column.bind)(array, &column.pg_type).map_err(|error| {
                    TransferredError::destination(format!(
                        "encoding column {}: {error}",
                        column.sql
                    ))
                })
            })
            .collect()
    }
}

/// Everything the load needs about one column: how to declare it, and how to write it.
/// Both come of one decision, so neither can contradict the other.
struct PgColumn {
    /// SQL declaration of the column for the `CREATE TABLE` statement.
    sql: String,
    /// Type the values are written as. For `PostGIS` that is `bytea`, not the column's own type.
    pg_type: PgType,
    /// Encoder turning the Arrow column into values of `pg_type`.
    bind: BindColumn,
}

impl PgColumn {
    /// Builder for a column whose DDL names the bare type, like `int4`, as most types do.
    fn bare(field: &ArrowField, pg_type: PgType, bind: BindColumn) -> Self {
        let name = pg_type.name().to_owned();
        Self::typed(field, &name, pg_type, bind)
    }

    /// Builder for a column whose DDL either includes a modifier, like `numeric(38,9)`,
    /// or names a type with its own OID in every database, like `geometry(Geometry,4326)`.
    fn typed(field: &ArrowField, sql_type: &str, pg_type: PgType, bind: BindColumn) -> Self {
        Self {
            pg_type,
            // Names go straight into DDL, and a Parquet field or dict key need not be an identifier.
            sql: format!("{} {sql_type}", escape_identifier(field.name())),
            bind,
        }
    }
}

/// Binds a column's encoder to one batch's array, so the downcast happens once per batch.
type BindColumn = Box<dyn Fn(&ArrayRef, &PgType) -> Result<WriteValue> + Send + Sync>;

/// Writes one row of a bound column into the COPY buffer, or reports it null.
pub type WriteValue = Box<dyn Fn(usize, &mut BytesMut) -> Result<IsNull> + Send + Sync>;

/// The Postgres column an Arrow field maps to.
trait ToPgColumn {
    /// Resolves the `CREATE TABLE` type name, binary COPY type and value encoder together, so no
    /// two of the three can disagree about what a column is.
    fn to_pg_column(&self) -> Result<PgColumn>;
}

impl ToPgColumn for ArrowField {
    #[allow(clippy::too_many_lines)]
    fn to_pg_column(&self) -> Result<PgColumn> {
        let extension = self.extension_type_name();
        Ok(match self.data_type() {
            ArrowType::Boolean => PgColumn::bare(
                self,
                PgType::BOOL,
                binder::<BooleanArray, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            ArrowType::Int16 => PgColumn::bare(
                self,
                PgType::INT2,
                binder::<Int16Array, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            ArrowType::Int32 => PgColumn::bare(
                self,
                PgType::INT4,
                binder::<Int32Array, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            ArrowType::Int64 => PgColumn::bare(
                self,
                PgType::INT8,
                binder::<Int64Array, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            ArrowType::Float32 => PgColumn::bare(
                self,
                PgType::FLOAT4,
                binder::<Float32Array, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            ArrowType::Float64 => PgColumn::bare(
                self,
                PgType::FLOAT8,
                binder::<Float64Array, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            // `json` stores the document verbatim; `jsonb` would reorder keys and drop whitespace.
            ArrowType::Utf8 if extension == Some(Json::NAME) => PgColumn::bare(
                self,
                PgType::JSON,
                binder::<StringArray, _>(|array, row, ty, buf| {
                    write_sql(&pg_json(array.value(row))?, ty, buf)
                }),
            ),
            ArrowType::Utf8 => PgColumn::bare(
                self,
                PgType::TEXT,
                binder::<StringArray, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            // `PostGIS` gets its OIDs per database, so no `PgType` names it and only the DDL can.
            // Binary COPY sends no types of its own, so `bytea` framing reaches `geometry_recv`.
            ArrowType::Binary if extension == Some(Wkb::NAME) => PgColumn::typed(
                self,
                &geo_sql_type(self)?,
                PgType::BYTEA,
                binder::<BinaryArray, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            // Plain bytes, and `arrow.opaque`, whose type name the destination deliberately drops.
            ArrowType::Binary => PgColumn::bare(
                self,
                PgType::BYTEA,
                binder::<BinaryArray, _>(|array, row, ty, buf| {
                    write_sql(&array.value(row), ty, buf)
                }),
            ),
            ArrowType::FixedSizeBinary(16) if extension == Some(Uuid::NAME) => PgColumn::bare(
                self,
                PgType::UUID,
                binder::<FixedSizeBinaryArray, _>(|array, row, ty, buf| {
                    write_sql(&pg_uuid(array.value(row))?, ty, buf)
                }),
            ),
            ArrowType::Date32 => PgColumn::bare(
                self,
                PgType::DATE,
                binder::<Date32Array, _>(|array, row, ty, buf| {
                    write_sql(&pg_date(array.value(row))?, ty, buf)
                }),
            ),
            ArrowType::Timestamp(TimeUnit::Microsecond, None) => PgColumn::bare(
                self,
                PgType::TIMESTAMP,
                binder::<TimestampMicrosecondArray, _>(|array, row, ty, buf| {
                    write_sql(&pg_timestamp(array.value(row))?.naive_utc(), ty, buf)
                }),
            ),
            // Arrow timestamps are UTC instants whatever the zone name, so the zone needs no lookup.
            ArrowType::Timestamp(TimeUnit::Microsecond, Some(_)) => PgColumn::bare(
                self,
                PgType::TIMESTAMPTZ,
                binder::<TimestampMicrosecondArray, _>(|array, row, ty, buf| {
                    write_sql(&pg_timestamp(array.value(row))?, ty, buf)
                }),
            ),
            ArrowType::Interval(IntervalUnit::MonthDayNano) => PgColumn::bare(
                self,
                PgType::INTERVAL,
                binder::<IntervalMonthDayNanoArray, _>(|array, row, ty, buf| {
                    write_sql(&pg_interval(array.value(row))?, ty, buf)
                }),
            ),
            // The one type here that takes a modifier, which a bare OID doesn't carry.
            &ArrowType::Decimal128(precision, scale) => PgColumn::typed(
                self,
                &format!("{}({precision},{scale})", PgType::NUMERIC.name()),
                PgType::NUMERIC,
                binder::<Decimal128Array, _>(move |array, row, ty, buf| {
                    write_sql(&pg_numeric(array.value(row), scale)?, ty, buf)
                }),
            ),
            // A range is its bounds plus a tag byte, so each bound reuses its own type's encoder.
            ArrowType::Struct(_) if extension == Some(PgRange::NAME) => {
                let bounds_type =
                    PgRange::type_of(self.data_type()).map_err(TransferredError::destination)?;
                let bound = ArrowField::new(LOWER, bounds_type.clone(), true).to_pg_column()?;
                let pg_type = to_range_type(&bound.pg_type)?;

                PgColumn::bare(
                    self,
                    pg_type,
                    Box::new(move |array, _| bind_range(array, &bound)),
                )
            }
            other => {
                return Err(TransferredError::destination(format!(
                    "Arrow type `{other}` is not supported by the Postgres destination in 0.1"
                )));
            }
        })
    }
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

/// Converts `pg_type` to the Postgres range over it; a range outside these six is per database.
fn to_range_type(pg_type: &PgType) -> Result<PgType> {
    Ok(match *pg_type {
        PgType::INT4 => PgType::INT4_RANGE,
        PgType::INT8 => PgType::INT8_RANGE,
        PgType::NUMERIC => PgType::NUM_RANGE,
        PgType::DATE => PgType::DATE_RANGE,
        PgType::TIMESTAMP => PgType::TS_RANGE,
        PgType::TIMESTAMPTZ => PgType::TSTZ_RANGE,
        ref other => {
            return Err(TransferredError::destination(format!(
                "Postgres has no built-in range over `{}`",
                other.name()
            )));
        }
    })
}

/// Binds a range column: a tag byte, then the bounds that are not infinite, each written by the
/// encoder of its own type.
fn bind_range(array: &ArrayRef, bound: &PgColumn) -> Result<WriteValue> {
    let ranges = cast::<StructArray>(array)?;
    // In the order `PgRange::fields` declares them, as `PgRange::type_of` has already checked.
    let [lower, upper, lower_inc, upper_inc, empty] = ranges.columns() else {
        return Err(TransferredError::destination(format!(
            "a `{}` column holds five children, not {}",
            PgRange::NAME,
            ranges.num_columns()
        )));
    };

    let write_lower = (bound.bind)(lower, &bound.pg_type)?;
    let write_upper = (bound.bind)(upper, &bound.pg_type)?;
    // `PgRange::fields` declares these non-nullable, so they read straight off.
    let (lower_inc, upper_inc, empty) = (
        cast::<BooleanArray>(lower_inc)?.clone(),
        cast::<BooleanArray>(upper_inc)?.clone(),
        cast::<BooleanArray>(empty)?.clone(),
    );

    Ok(null_checked(ranges, move |_ranges, row, buf| {
        if empty.value(row) {
            empty_range_to_sql(buf);
        } else {
            range_to_sql(
                |buf| write_bound(&write_lower, row, lower_inc.value(row), buf),
                |buf| write_bound(&write_upper, row, upper_inc.value(row), buf),
                buf,
            )
            .map_err(TransferredError::destination)?;
        }

        Ok(IsNull::No)
    }))
}

/// Writes a bound, reporting it infinite when its value is null: Postgres allows no NULL bound.
fn write_bound(
    write: &WriteValue,
    row: usize,
    inclusive: bool,
    buf: &mut BytesMut,
) -> std::result::Result<RangeBound<ProtocolIsNull>, Box<dyn std::error::Error + Sync + Send>> {
    // The two `IsNull`s belong to different crates; only a bound we did write reaches the protocol's.
    Ok(match write(row, buf)? {
        IsNull::Yes => RangeBound::Unbounded,
        IsNull::No if inclusive => RangeBound::Inclusive(ProtocolIsNull::No),
        IsNull::No => RangeBound::Exclusive(ProtocolIsNull::No),
    })
}

/// Builds a column binder from the way one of its values is written. Downcasts once per batch.
fn binder<A, F>(write: F) -> BindColumn
where
    A: Array + Clone + 'static,
    F: Fn(&A, usize, &PgType, &mut BytesMut) -> Result<IsNull> + Copy + Send + Sync + 'static,
{
    Box::new(move |array, pg_type| {
        let pg_type = pg_type.clone();

        Ok(null_checked(cast::<A>(array)?, move |array, row, buf| {
            write(array, row, &pg_type, buf)
        }))
    })
}

/// Checks the validity bitmap first, so a null value never reaches the writer.
/// Keeps the array by clone, which for Arrow bumps refcounts and copies no data.
fn null_checked<A: Array + Clone + 'static>(
    array: &A,
    write: impl Fn(&A, usize, &mut BytesMut) -> Result<IsNull> + Send + Sync + 'static,
) -> WriteValue {
    let array = array.clone();

    Box::new(move |row, buf| {
        if array.is_null(row) {
            Ok(IsNull::Yes)
        } else {
            write(&array, row, buf)
        }
    })
}

/// Writes one value in Postgres binary form, turning a `ToSql` failure into a destination error.
fn write_sql(value: &impl ToSql, pg_type: &PgType, buf: &mut BytesMut) -> Result<IsNull> {
    value
        .to_sql(pg_type, buf)
        .map_err(TransferredError::destination)
}

/// Downcasts an Arrow column; a mismatch is unreachable, as the encoder came from the same field.
fn cast<A: 'static>(array: &ArrayRef) -> Result<&A> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        TransferredError::destination(format!("column is not a {}", type_name::<A>()))
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::Arc;

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
            ArrowToPg::derive(&source_schema()).unwrap().declarations(),
            r#""b" bool, "i2" int2, "i4" int4, "i8" int8, "f4" float4, "f8" float8, "t" text, "#
                .to_owned()
                + r#""bin" bytea, "d" date, "ts" timestamp, "tstz" timestamptz, "iv" interval, "#
                + r#""n" numeric(38,9), "u" uuid, "j" json"#
        );
    }

    /// Binds a one-column batch and writes its first row, as the COPY stream would.
    fn write_first(field: ArrowField, array: ArrayRef) -> Result<BytesMut> {
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap();
        let mut buf = BytesMut::new();
        let columns = ArrowToPg::derive(&schema)?.bind(&batch)?;

        (columns.first().unwrap())(0, &mut buf)?;
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
            ArrowToPg::derive(&schema).unwrap().declarations(),
            r#""i4" int4range, "i8" int8range, "n" numrange, "d" daterange, "#.to_owned()
                + r#""ts" tsrange, "tstz" tstzrange"#
        );
    }

    /// A range over anything else is defined per database, so no fixed OID could announce it.
    #[test]
    fn rejects_a_range_postgres_has_no_built_in_for() {
        let schema = Schema::new(vec![range("t", ArrowType::Utf8)]);
        assert!(ArrowToPg::derive(&schema).is_err());
    }

    /// Extension metadata is the only thing separating `json` from `text`; without it, plain wins.
    #[test]
    fn extension_metadata_picks_the_semantic_pg_type() {
        let plain = Schema::new(vec![ArrowField::new("j", ArrowType::Utf8, true)]);
        assert_eq!(
            ArrowToPg::derive(&plain).unwrap().declarations(),
            r#""j" text"#
        );
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
            ArrowToPg::derive(&schema).unwrap().declarations(),
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
        assert!(ArrowToPg::derive(&schema).is_err());
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        let schema = Schema::new(vec![ArrowField::new("u16", ArrowType::UInt16, true)]);
        assert!(ArrowToPg::derive(&schema).is_err());
    }

    /// The table is created from the first batch, so a later partition may not fit it.
    #[test]
    fn rejects_a_batch_that_does_not_match_the_derived_schema() {
        let arrow_to_pg = ArrowToPg::derive(&Schema::new(vec![ArrowField::new(
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

        assert!(arrow_to_pg.bind(&batch).is_err());
    }

    /// Only fields drive the mapping, so writer metadata on the schema must not reject a batch.
    #[test]
    fn accepts_a_batch_differing_only_in_schema_metadata() {
        let schema = Schema::new(vec![ArrowField::new("a", ArrowType::Int32, true)]);
        let arrow_to_pg = ArrowToPg::derive(&schema).unwrap();

        let tagged = Arc::new(
            schema.with_metadata(HashMap::from([("writer".to_owned(), "test".to_owned())])),
        );
        let batch =
            RecordBatch::try_new(tagged, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        assert_eq!(arrow_to_pg.bind(&batch).unwrap().len(), 1);
    }

    /// Names go straight into DDL, and a Parquet field or dict key need not be a bare identifier.
    #[test]
    fn quotes_column_names_in_declarations() {
        let awkward = Schema::new(vec![
            ArrowField::new("Total Sales", ArrowType::Int32, true),
            ArrowField::new("user.id", ArrowType::Utf8, true),
        ]);
        assert_eq!(
            ArrowToPg::derive(&awkward).unwrap().declarations(),
            r#""Total Sales" int4, "user.id" text"#
        );
    }
}
