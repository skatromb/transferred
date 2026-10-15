//! MySQL column definitions mapped to Arrow fields.

use crate::error::Result;
use arrow_schema::extension::{Json, Uuid};
use arrow_schema::{DataType, Field, TimeUnit};
use mysql_async::Column as WireColumn;
use mysql_async::Row;
use mysql_async::consts::{ColumnFlags, ColumnType};

use crate::catalog::cell;
use crate::table::quote;

pub(crate) const JSON_MARKER: &str = "transferred:arrow.json:v1";
pub(crate) const UUID_MARKER: &str = "transferred:arrow.uuid:v1";

pub(crate) struct Column {
    field: Field,
    native: String,
}

impl Column {
    pub(crate) const fn field(&self) -> &Field {
        &self.field
    }

    pub(crate) fn from_catalog(row: &Row, checks: &[String]) -> Result<Self> {
        let name: String = cell(row, "COLUMN_NAME")?;
        let native: String = cell(row, "DATA_TYPE")?;
        let declaration: String = cell(row, "COLUMN_TYPE")?;
        let data_type = mapped_type(row, &native, &declaration)
            .map_err(|error| format!("column {name} ({declaration}): {error}"))?;
        let mut field = Field::new(&name, data_type, true);
        tag(&mut field, row, &native, checks)?;
        Ok(Self { field, native })
    }

    pub(crate) fn check(&self, wire: &WireColumn) -> Result<()> {
        if wire.name_str() != self.field.name().as_str()
            || !wire_type(&self.native, wire.column_type())
        {
            return Err(format!(
                "column {} changed after schema inspection",
                self.field.name()
            )
            .into());
        }
        self.check_integer(wire)?;
        self.check_decimal(wire)
    }

    fn check_integer(&self, wire: &WireColumn) -> Result<()> {
        if matches!(
            self.native.as_str(),
            "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
        ) {
            let unsigned = self.field.data_type().is_unsigned_integer();
            if wire.flags().contains(ColumnFlags::UNSIGNED_FLAG) != unsigned {
                return Err(format!("column {} changed signedness", self.field.name()).into());
            }
        }
        Ok(())
    }

    fn check_decimal(&self, wire: &WireColumn) -> Result<()> {
        if let &DataType::Decimal128(_, scale) | &DataType::Decimal256(_, scale) =
            self.field.data_type()
            && wire.decimals() != scale.cast_unsigned()
        {
            return Err(format!("column {} changed decimal scale", self.field.name()).into());
        }
        Ok(())
    }

    pub(crate) fn expression(&self) -> String {
        quote(self.field.name())
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps the server type mapping in one exhaustive decision"
)]
fn mapped_type(row: &Row, native: &str, declaration: &str) -> Result<DataType> {
    let unsigned = declaration
        .split_ascii_whitespace()
        .any(|word| word == "unsigned");
    Ok(match (native, unsigned) {
        ("tinyint", false) => DataType::Int8,
        ("tinyint", true) => DataType::UInt8,
        ("smallint", false) => DataType::Int16,
        ("smallint", true) | ("year", _) => DataType::UInt16,
        ("mediumint" | "int", false) => DataType::Int32,
        ("mediumint" | "int", true) => DataType::UInt32,
        ("bigint", false) => DataType::Int64,
        ("bigint", true) | ("bit", _) => DataType::UInt64,
        ("float", _) => DataType::Float32,
        ("double", _) => DataType::Float64,
        ("decimal", _) => decimal(row)?,
        ("date", _) => DataType::Date32,
        ("datetime", _) => DataType::Timestamp(TimeUnit::Microsecond, None),
        ("timestamp", _) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ("time", _) => DataType::Duration(TimeUnit::Microsecond),
        (
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
            | "json",
            _,
        ) => DataType::Utf8,
        ("binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob", _) => {
            DataType::Binary
        }
        _ => return Err(format!("unsupported MySQL type {native}").into()),
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "validates each native type against its binary protocol family"
)]
fn wire_type(native: &str, wire: ColumnType) -> bool {
    let allowed: &[ColumnType] = match native {
        "tinyint" => &[ColumnType::MYSQL_TYPE_TINY],
        "smallint" => &[ColumnType::MYSQL_TYPE_SHORT],
        "mediumint" => &[ColumnType::MYSQL_TYPE_INT24],
        "int" => &[ColumnType::MYSQL_TYPE_LONG],
        "bigint" => &[ColumnType::MYSQL_TYPE_LONGLONG],
        "year" => &[ColumnType::MYSQL_TYPE_YEAR],
        "bit" => &[ColumnType::MYSQL_TYPE_BIT],
        "float" => &[ColumnType::MYSQL_TYPE_FLOAT],
        "double" => &[ColumnType::MYSQL_TYPE_DOUBLE],
        "decimal" => &[
            ColumnType::MYSQL_TYPE_DECIMAL,
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
        ],
        "date" => &[ColumnType::MYSQL_TYPE_DATE],
        "datetime" => &[
            ColumnType::MYSQL_TYPE_DATETIME,
            ColumnType::MYSQL_TYPE_DATETIME2,
        ],
        "timestamp" => &[
            ColumnType::MYSQL_TYPE_TIMESTAMP,
            ColumnType::MYSQL_TYPE_TIMESTAMP2,
        ],
        "time" => &[ColumnType::MYSQL_TYPE_TIME, ColumnType::MYSQL_TYPE_TIME2],
        "json" => &[ColumnType::MYSQL_TYPE_JSON],
        "char" | "varchar" | "binary" | "varbinary" | "enum" | "set" => &[
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnType::MYSQL_TYPE_VARCHAR,
        ],
        "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" => &[
            ColumnType::MYSQL_TYPE_TINY_BLOB,
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            ColumnType::MYSQL_TYPE_LONG_BLOB,
        ],
        _ => &[],
    };
    allowed.contains(&wire)
}

fn decimal(row: &Row) -> Result<DataType> {
    let precision: u8 = cell(row, "NUMERIC_PRECISION")?;
    let scale: i8 = cell(row, "NUMERIC_SCALE")?;
    if precision == 0
        || precision > 65
        || !(0..=30).contains(&scale)
        || scale.cast_unsigned() > precision
    {
        return Err("invalid MySQL decimal precision or scale".into());
    }
    Ok(if precision <= 38 {
        DataType::Decimal128(precision, scale)
    } else {
        DataType::Decimal256(precision, scale)
    })
}

fn tag(field: &mut Field, row: &Row, native: &str, checks: &[String]) -> Result<()> {
    let comment: String = cell(row, "COLUMN_COMMENT")?;
    let json_check = format!("json_valid({})", quote(field.name()));
    let json = native == "json"
        || (native == "longtext"
            && checks
                .iter()
                .any(|check| check.eq_ignore_ascii_case(&json_check)));
    if json {
        field.try_with_extension_type(Json::default())?;
    }
    if comment == UUID_MARKER
        && native == "binary"
        && cell::<Option<u64>>(row, "CHARACTER_MAXIMUM_LENGTH")? == Some(16)
    {
        field.set_data_type(DataType::FixedSizeBinary(16));
        field.try_with_extension_type(Uuid)?;
    }
    verify_marker(field, &comment)
}

fn verify_marker(field: &Field, comment: &str) -> Result<()> {
    match comment {
        JSON_MARKER if field.extension_type_name() != Some("arrow.json") => {
            return Err(format!(
                "column {}: JSON marker requires a JSON_VALID check on LONGTEXT",
                field.name()
            )
            .into());
        }
        UUID_MARKER if field.extension_type_name() != Some("arrow.uuid") => {
            return Err(format!("column {}: UUID marker requires BINARY(16)", field.name()).into());
        }
        JSON_MARKER | UUID_MARKER => {}
        marker if marker.starts_with("transferred:arrow.") => {
            return Err(format!("column {}: unknown type marker {marker}", field.name()).into());
        }
        _ => {}
    }
    Ok(())
}
