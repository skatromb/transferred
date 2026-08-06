//! Arrow → Postgres type mapping. One table row per supported type; mirror of `pg_to_arrow`.

use std::any::type_name;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::extension::{ExtensionType, Json, Uuid};
use arrow_schema::{DataType as ArrowType, Field, IntervalUnit, Schema, TimeUnit};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::types::{ToSql, Type as PgType};
use transferred_core::{Result, TransferredError};

use crate::convert::{pg_date, pg_interval, pg_json, pg_numeric, pg_timestamp, pg_uuid};

/// One Postgres value, borrowed from the Arrow column it came from.
pub type PgValue<'a> = Box<dyn ToSql + Sync + Send + 'a>;

/// Turns an Arrow column into Postgres values, one per row.
type ArrowToPgFn = Box<dyn for<'a> Fn(&'a ArrayRef) -> Result<Vec<PgValue<'a>>> + Send + Sync>;

/// A Postgres column and the encoder for the Arrow column feeding it.
struct PgColumn {
    /// Quoted name and type for the `CREATE TABLE` statement.
    declaration: String,
    /// Type for the Postgres binary COPY.
    pg_type: PgType,
    /// Encoder turning the Arrow column into values matching `pg_type`.
    arrow_to_pg: ArrowToPgFn,
}

/// Postgres column definitions + per-column encoders, derived once from an Arrow schema.
pub struct ArrowToPg {
    schema: Schema,
    columns: Vec<PgColumn>,
}

impl ArrowToPg {
    /// Derive Postgres columns and encoders from an Arrow schema. All columns nullable.
    pub fn derive(schema: &Schema) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| arrow_pg_column(field))
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
            .map(|column| column.declaration.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Column types in batch order for binary COPY.
    pub fn pg_types(&self) -> Vec<PgType> {
        self.columns
            .iter()
            .map(|column| column.pg_type.clone())
            .collect()
    }

    /// Encode a batch into Postgres values, one vector per column.
    pub fn encode<'a>(&self, batch: &'a RecordBatch) -> Result<Vec<Vec<PgValue<'a>>>> {
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
                let arrow_to_pg = &column.arrow_to_pg;
                arrow_to_pg(array).map_err(|error| {
                    TransferredError::destination(format!(
                        "encoding column {}: {error}",
                        column.declaration
                    ))
                })
            })
            .collect()
    }
}

/// Returns Postgres wire type and value encoder for a given Arrow field.
#[allow(clippy::too_many_lines)]
fn arrow_pg_column(field: &Field) -> Result<PgColumn> {
    let extension = field.extension_type_name();
    let (pg_type, arrow_to_pg): (PgType, ArrowToPgFn) = match field.data_type() {
        ArrowType::Boolean => (
            PgType::BOOL,
            Box::new(|array| Ok(values(cast::<BooleanArray>(array)?.iter()))),
        ),
        ArrowType::Int16 => (
            PgType::INT2,
            Box::new(|array| Ok(values(cast::<Int16Array>(array)?.iter()))),
        ),
        ArrowType::Int32 => (
            PgType::INT4,
            Box::new(|array| Ok(values(cast::<Int32Array>(array)?.iter()))),
        ),
        ArrowType::Int64 => (
            PgType::INT8,
            Box::new(|array| Ok(values(cast::<Int64Array>(array)?.iter()))),
        ),
        ArrowType::Float32 => (
            PgType::FLOAT4,
            Box::new(|array| Ok(values(cast::<Float32Array>(array)?.iter()))),
        ),
        ArrowType::Float64 => (
            PgType::FLOAT8,
            Box::new(|array| Ok(values(cast::<Float64Array>(array)?.iter()))),
        ),
        ArrowType::Utf8 if extension == Some(Json::NAME) => (
            // `json` stores the document verbatim; `jsonb` would reorder keys and drop whitespace.
            PgType::JSON,
            Box::new(|array| {
                let docs = cast::<StringArray>(array)?
                    .iter()
                    .map(|text| text.map(pg_json).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(docs.into_iter()))
            }),
        ),
        ArrowType::Utf8 => (
            PgType::TEXT,
            Box::new(|array| Ok(values(cast::<StringArray>(array)?.iter()))),
        ),
        ArrowType::Binary => (
            PgType::BYTEA,
            Box::new(|array| Ok(values(cast::<BinaryArray>(array)?.iter()))),
        ),
        ArrowType::FixedSizeBinary(16) if extension == Some(Uuid::NAME) => (
            PgType::UUID,
            Box::new(|array| {
                let uuids = cast::<FixedSizeBinaryArray>(array)?
                    .iter()
                    .map(|bytes| bytes.map(pg_uuid).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(uuids.into_iter()))
            }),
        ),
        ArrowType::Date32 => (
            PgType::DATE,
            Box::new(|array| {
                let dates = cast::<Date32Array>(array)?
                    .iter()
                    .map(|days| days.map(pg_date).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(dates.into_iter()))
            }),
        ),
        ArrowType::Timestamp(TimeUnit::Microsecond, None) => (
            PgType::TIMESTAMP,
            Box::new(|array| {
                let timestamps = cast::<TimestampMicrosecondArray>(array)?
                    .iter()
                    .map(|micros| {
                        micros.map(|micros| pg_timestamp(micros).map(|utc| utc.naive_utc()))
                    })
                    .map(Option::transpose)
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(timestamps.into_iter()))
            }),
        ),
        // Arrow timestamps are UTC instants whatever the zone name, so the zone needs no lookup.
        ArrowType::Timestamp(TimeUnit::Microsecond, Some(_)) => (
            PgType::TIMESTAMPTZ,
            Box::new(|array| {
                let timestamps = cast::<TimestampMicrosecondArray>(array)?
                    .iter()
                    .map(|micros| micros.map(pg_timestamp))
                    .map(Option::transpose)
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(timestamps.into_iter()))
            }),
        ),
        ArrowType::Interval(IntervalUnit::MonthDayNano) => (
            PgType::INTERVAL,
            Box::new(|array| {
                let intervals = cast::<IntervalMonthDayNanoArray>(array)?
                    .iter()
                    .map(|interval| interval.map(pg_interval).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(intervals.into_iter()))
            }),
        ),
        &ArrowType::Decimal128(_, scale) => (
            PgType::NUMERIC,
            Box::new(move |array| {
                let decimals = cast::<Decimal128Array>(array)?
                    .iter()
                    .map(|units| units.map(|units| pg_numeric(units, scale)).transpose())
                    .collect::<Result<Vec<_>>>()?;
                Ok(values(decimals.into_iter()))
            }),
        ),
        other => {
            return Err(TransferredError::destination(format!(
                "Arrow type `{other}` is not supported by the Postgres destination in 0.1"
            )));
        }
    };

    Ok(PgColumn {
        declaration: declaration(field, &pg_type),
        pg_type,
        arrow_to_pg,
    })
}

/// One `CREATE TABLE` column, e.g. `"total" numeric(38,9)`.
fn declaration(field: &Field, pg_type: &PgType) -> String {
    // `PgType` is a bare OID, so the one type here that takes a modifier spells out its own.
    let modifier = match *field.data_type() {
        ArrowType::Decimal128(precision, scale) => format!("({precision},{scale})"),
        _ => String::new(),
    };

    format!(
        "{} {}{modifier}",
        escape_identifier(field.name()),
        pg_type.name()
    )
}

/// Box one Postgres value per row, `None` for Arrow null.
fn values<'a, T: ToSql + Sync + Send + 'a>(
    column: impl Iterator<Item = Option<T>>,
) -> Vec<PgValue<'a>> {
    column.map(|value| Box::new(value) as PgValue).collect()
}

/// Downcast an Arrow column; a mismatch is unreachable, as the encoder came from the same field.
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
            Field::new("b", ArrowType::Boolean, true),
            Field::new("i2", ArrowType::Int16, true),
            Field::new("i4", ArrowType::Int32, true),
            Field::new("i8", ArrowType::Int64, true),
            Field::new("f4", ArrowType::Float32, true),
            Field::new("f8", ArrowType::Float64, true),
            Field::new("t", ArrowType::Utf8, true),
            Field::new("bin", ArrowType::Binary, true),
            Field::new("d", ArrowType::Date32, true),
            Field::new(
                "ts",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "tstz",
                ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("iv", ArrowType::Interval(IntervalUnit::MonthDayNano), true),
            Field::new("n", ArrowType::Decimal128(38, 9), true),
            Field::new("u", ArrowType::FixedSizeBinary(16), true).with_extension_type(Uuid),
            Field::new("j", ArrowType::Utf8, true).with_extension_type(Json::default()),
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

    #[test]
    fn pg_types_follow_batch_order() {
        let types = ArrowToPg::derive(&source_schema()).unwrap().pg_types();
        assert_eq!(types.first(), Some(&PgType::BOOL));
        assert_eq!(types.last(), Some(&PgType::JSON));
        assert_eq!(types.len(), source_schema().fields().len());
    }

    /// Extension metadata is the only thing separating `json` from `text`; without it, plain wins.
    #[test]
    fn extension_metadata_picks_the_semantic_pg_type() {
        let plain = Schema::new(vec![Field::new("j", ArrowType::Utf8, true)]);
        assert_eq!(
            ArrowToPg::derive(&plain).unwrap().declarations(),
            r#""j" text"#
        );
    }

    /// PG has no fixed-width binary, so 16 bytes only mean a uuid when the field says so.
    #[test]
    fn rejects_fixed_size_binary_without_the_uuid_extension() {
        let schema = Schema::new(vec![Field::new("u", ArrowType::FixedSizeBinary(16), true)]);
        assert!(ArrowToPg::derive(&schema).is_err());
    }

    #[test]
    fn rejects_unsupported_arrow_type() {
        let schema = Schema::new(vec![Field::new("u16", ArrowType::UInt16, true)]);
        assert!(ArrowToPg::derive(&schema).is_err());
    }

    /// The table is created from the first batch, so a later partition may not fit it.
    #[test]
    fn rejects_a_batch_that_does_not_match_the_derived_schema() {
        let arrow_to_pg =
            ArrowToPg::derive(&Schema::new(vec![Field::new("a", ArrowType::Int32, true)])).unwrap();

        let widened = Arc::new(Schema::new(vec![Field::new("a", ArrowType::Int64, true)]));
        let batch =
            RecordBatch::try_new(widened, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        assert!(arrow_to_pg.encode(&batch).is_err());
    }

    /// Only fields drive the mapping, so writer metadata on the schema must not reject a batch.
    #[test]
    fn accepts_a_batch_differing_only_in_schema_metadata() {
        let schema = Schema::new(vec![Field::new("a", ArrowType::Int32, true)]);
        let arrow_to_pg = ArrowToPg::derive(&schema).unwrap();

        let tagged = Arc::new(
            schema.with_metadata(HashMap::from([("writer".to_owned(), "test".to_owned())])),
        );
        let batch =
            RecordBatch::try_new(tagged, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        let columns = arrow_to_pg.encode(&batch).unwrap();
        assert_eq!(columns.first().unwrap().len(), 1);
    }

    /// Names go straight into DDL, and a Parquet field or dict key need not be a bare identifier.
    #[test]
    fn quotes_column_names_in_declarations() {
        let awkward = Schema::new(vec![
            Field::new("Total Sales", ArrowType::Int32, true),
            Field::new("user.id", ArrowType::Utf8, true),
        ]);
        assert_eq!(
            ArrowToPg::derive(&awkward).unwrap().declarations(),
            r#""Total Sales" int4, "user.id" text"#
        );
    }
}
