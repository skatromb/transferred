//! One Arrow column's MySQL type and parameter encoding.

use crate::error::Result;
use arrow::array::{Array, FixedSizeBinaryArray};
use arrow_schema::extension::{Json, Uuid};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use mysql_async::Value;
use serde_json::value::RawValue;

use super::values;
use crate::column::{JSON_MARKER, UUID_MARKER};
use crate::table::quote;

pub(crate) enum Encoding {
    Plain(DataType),
    Json,
    Uuid,
}

impl Encoding {
    pub(crate) fn new(field: &Field) -> Result<Self> {
        match field.extension_type_name() {
            None => Ok(Self::Plain(field.data_type().clone())),
            Some("arrow.json") => {
                _ = field.try_extension_type::<Json>()?;
                Ok(Self::Json)
            }
            Some("arrow.uuid") => {
                _ = field.try_extension_type::<Uuid>()?;
                Ok(Self::Uuid)
            }
            Some(extension) => Err(format!("unsupported extension {extension}").into()),
        }
    }

    pub(crate) fn declaration(&self, name: &str) -> Result<String> {
        match self {
            Self::Json => Ok(format!(
                "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin COMMENT '{JSON_MARKER}' CHECK(JSON_VALID({}))",
                quote(name)
            )),
            Self::Uuid => Ok(format!("BINARY(16) COMMENT '{UUID_MARKER}'")),
            Self::Plain(data_type) => sql_type(data_type),
        }
    }

    pub(crate) fn parameter(&self, array: &dyn Array, row: usize) -> Result<Value> {
        if array.is_null(row) {
            return Ok(Value::NULL);
        }
        match self {
            Self::Plain(data_type) => values::plain(data_type, array, row),
            Self::Json => {
                let document = values::text(array, row)?;
                _ = serde_json::from_str::<&RawValue>(document)?;
                values::bytes(document.as_bytes())
            }
            Self::Uuid => values::bytes(values::cast::<FixedSizeBinaryArray>(array)?.value(row)),
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps all accepted Arrow-to-SQL type mappings in one match"
)]
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "all unlisted Arrow types must be rejected"
)]
fn sql_type(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Boolean => "TINYINT(1)".to_owned(),
        DataType::Int8 => "TINYINT".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INT".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_owned(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_owned(),
        DataType::UInt32 => "INT UNSIGNED".to_owned(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_owned(),
        DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin".to_owned()
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "LONGBLOB".to_owned(),
        DataType::Date32 => "DATE".to_owned(),
        DataType::Timestamp(_, None) => "DATETIME(6)".to_owned(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP(6)".to_owned(),
        DataType::Duration(_)
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Interval(IntervalUnit::MonthDayNano) => "TIME(6)".to_owned(),
        &DataType::Decimal128(precision, scale) | &DataType::Decimal256(precision, scale) => {
            decimal_type(precision, scale)?
        }
        _ => return Err(format!("unsupported Arrow type {data_type}").into()),
    })
}

fn decimal_type(precision: u8, scale: i8) -> Result<String> {
    if precision == 0
        || precision > 65
        || !(0..=30).contains(&scale)
        || scale.cast_unsigned() > precision
    {
        return Err(
            format!("DECIMAL({precision},{scale}) exceeds MySQL precision 65 or scale 30").into(),
        );
    }
    Ok(format!("DECIMAL({precision},{scale})"))
}
