//! Decodes one MySQL column into its Arrow builder.

use std::any::type_name;
use std::fmt::Display;

use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder, StringBuilder,
    make_builder,
};
use arrow::datatypes::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type, i256,
};
use arrow_schema::{DataType, Field, FieldRef};
use mysql_async::Value;
use serde_json::value::RawValue;

use crate::error::Result;
use crate::{decimal, temporal};

pub(crate) struct ColumnDecoder {
    field: FieldRef,
    builder: Box<dyn ArrayBuilder>,
}

impl ColumnDecoder {
    pub(crate) fn new(field: FieldRef, capacity: usize) -> Self {
        let builder = make_builder(field.data_type(), capacity);
        Self { field, builder }
    }

    pub(crate) fn append(&mut self, cell: &Value) -> Result<()> {
        let present = (!matches!(cell, Value::NULL)).then_some(cell);
        append(&self.field, self.builder.as_mut(), present)
            .map_err(|error| format!("column {}: {error}", self.field.name()).into())
    }

    pub(crate) fn finish(&mut self) -> ArrayRef {
        self.builder.finish()
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "dispatches each supported Arrow storage type to its builder"
)]
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "the source catalog produces only the explicitly supported types"
)]
fn append(field: &Field, builder: &mut dyn ArrayBuilder, cell: Option<&Value>) -> Result<()> {
    match field.data_type() {
        DataType::Int8 => primitive::<Int8Type>(builder, cell, signed::<i8>),
        DataType::Int16 => primitive::<Int16Type>(builder, cell, signed::<i16>),
        DataType::Int32 => primitive::<Int32Type>(builder, cell, signed::<i32>),
        DataType::Int64 => primitive::<Int64Type>(builder, cell, signed::<i64>),
        DataType::UInt8 => primitive::<UInt8Type>(builder, cell, unsigned::<u8>),
        DataType::UInt16 => primitive::<UInt16Type>(builder, cell, unsigned::<u16>),
        DataType::UInt32 => primitive::<UInt32Type>(builder, cell, unsigned::<u32>),
        DataType::UInt64 => primitive::<UInt64Type>(builder, cell, unsigned::<u64>),
        DataType::Float32 => primitive::<Float32Type>(builder, cell, float32),
        DataType::Float64 => primitive::<Float64Type>(builder, cell, float64),
        DataType::Date32 => primitive::<Date32Type>(builder, cell, |incoming| {
            Ok(Date32Type::from_naive_date(
                temporal::datetime(incoming)?.date(),
            ))
        }),
        DataType::Timestamp(..) => {
            primitive::<TimestampMicrosecondType>(builder, cell, |incoming| {
                Ok(temporal::datetime(incoming)?.and_utc().timestamp_micros())
            })
        }
        DataType::Duration(_) => {
            primitive::<DurationMicrosecondType>(builder, cell, temporal::duration)
        }
        &DataType::Decimal128(precision, scale) => {
            primitive::<Decimal128Type>(builder, cell, |incoming| {
                narrow(decimal::parse(text(incoming)?, precision, scale)?)
            })
        }
        &DataType::Decimal256(precision, scale) => {
            primitive::<Decimal256Type>(builder, cell, |incoming| {
                decimal::parse(text(incoming)?, precision, scale)
            })
        }
        DataType::Utf8 => append_text(field, builder, cell),
        DataType::Binary => {
            cast::<BinaryBuilder>(builder)?.append_option(cell.map(bytes).transpose()?);
            Ok(())
        }
        DataType::FixedSizeBinary(16) => append_uuid(builder, cell),
        _ => Err("unsupported Arrow source storage type".into()),
    }
}

fn primitive<Arrow: ArrowPrimitiveType>(
    builder: &mut dyn ArrayBuilder,
    cell: Option<&Value>,
    decode: impl FnOnce(&Value) -> Result<Arrow::Native>,
) -> Result<()> {
    cast::<PrimitiveBuilder<Arrow>>(builder)?.append_option(cell.map(decode).transpose()?);
    Ok(())
}

fn append_text(field: &Field, builder: &mut dyn ArrayBuilder, cell: Option<&Value>) -> Result<()> {
    let text = cell.map(text).transpose()?;
    if field.extension_type_name() == Some("arrow.json")
        && let Some(document) = text
    {
        _ = serde_json::from_str::<&RawValue>(document)?;
    }
    cast::<StringBuilder>(builder)?.append_option(text);
    Ok(())
}

fn append_uuid(builder: &mut dyn ArrayBuilder, cell: Option<&Value>) -> Result<()> {
    let uuids = cast::<FixedSizeBinaryBuilder>(builder)?;
    if let Some(incoming) = cell {
        uuids.append_value(bytes(incoming)?)?;
    } else {
        uuids.append_null();
    }
    Ok(())
}

fn signed<Integer: TryFrom<i64>>(cell: &Value) -> Result<Integer>
where
    Integer::Error: Display,
{
    let integer = match *cell {
        Value::Int(integer) => integer,
        Value::UInt(integer) => i64::try_from(integer)?,
        Value::NULL
        | Value::Bytes(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::Date(..)
        | Value::Time(..) => return Err("expected an integer".into()),
    };
    Integer::try_from(integer).map_err(|error| {
        format!(
            "integer {integer} exceeds {}: {error}",
            type_name::<Integer>()
        )
        .into()
    })
}

fn unsigned<Integer: TryFrom<u64>>(cell: &Value) -> Result<Integer>
where
    Integer::Error: Display,
{
    let integer = match cell {
        Value::Int(integer) => u64::try_from(*integer)?,
        Value::UInt(integer) => *integer,
        Value::Bytes(bytes) if bytes.len() <= 8 => bytes
            .iter()
            .fold(0_u64, |bits, byte| (bits << 8) | u64::from(*byte)),
        Value::NULL
        | Value::Bytes(_)
        | Value::Float(_)
        | Value::Double(_)
        | Value::Date(..)
        | Value::Time(..) => return Err("expected an unsigned integer or BIT".into()),
    };
    Integer::try_from(integer).map_err(|error| {
        format!(
            "integer {integer} exceeds {}: {error}",
            type_name::<Integer>()
        )
        .into()
    })
}

fn float32(cell: &Value) -> Result<f32> {
    if let Value::Float(number) = *cell
        && number.is_finite()
    {
        Ok(number)
    } else {
        Err("expected a finite FLOAT".into())
    }
}

fn float64(cell: &Value) -> Result<f64> {
    if let Value::Double(number) = *cell
        && number.is_finite()
    {
        Ok(number)
    } else {
        Err("expected a finite DOUBLE".into())
    }
}

fn bytes(cell: &Value) -> Result<&[u8]> {
    if let Value::Bytes(bytes) = cell {
        Ok(bytes)
    } else {
        Err("expected bytes".into())
    }
}

fn text(cell: &Value) -> Result<&str> {
    Ok(str::from_utf8(bytes(cell)?)?)
}

fn narrow(mantissa: i256) -> Result<i128> {
    mantissa
        .to_i128()
        .ok_or_else(|| "decimal exceeds Decimal128".into())
}

fn cast<Builder: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> Result<&mut Builder> {
    builder
        .as_any_mut()
        .downcast_mut()
        .ok_or_else(|| format!("unexpected builder for {}", type_name::<Builder>()).into())
}
