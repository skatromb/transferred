//! Converts validated Arrow storage values to MySQL protocol parameters.

#![expect(
    clippy::wildcard_enum_match_arm,
    reason = "each decoder refuses every Arrow storage type outside its explicit contract"
)]

use std::any::type_name;

use crate::error::Result;
use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, LargeBinaryArray,
    LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, Date32Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, IntervalMonthDayNanoType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type, i256,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use mysql_async::Value;

use crate::connection::MAX_PACKET;
use crate::{decimal, temporal};

#[expect(
    clippy::too_many_lines,
    reason = "encodes each validated physical Arrow type without per-row casts of entire arrays"
)]
pub(crate) fn plain(data_type: &DataType, array: &dyn Array, row: usize) -> Result<Value> {
    Ok(match data_type {
        DataType::Boolean => Value::Int(i64::from(cast::<BooleanArray>(array)?.value(row))),
        DataType::Int8 => Value::Int(i64::from(primitive::<Int8Type>(array, row)?)),
        DataType::Int16 => Value::Int(i64::from(primitive::<Int16Type>(array, row)?)),
        DataType::Int32 => Value::Int(i64::from(primitive::<Int32Type>(array, row)?)),
        DataType::Int64 => Value::Int(primitive::<Int64Type>(array, row)?),
        DataType::UInt8 => Value::UInt(u64::from(primitive::<UInt8Type>(array, row)?)),
        DataType::UInt16 => Value::UInt(u64::from(primitive::<UInt16Type>(array, row)?)),
        DataType::UInt32 => Value::UInt(u64::from(primitive::<UInt32Type>(array, row)?)),
        DataType::UInt64 => Value::UInt(primitive::<UInt64Type>(array, row)?),
        DataType::Float32 => finite_float(primitive::<Float32Type>(array, row)?)?,
        DataType::Float64 => finite_double(primitive::<Float64Type>(array, row)?)?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            bytes(text(array, row)?.as_bytes())?
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => bytes(binary(array, row)?)?,
        &DataType::Decimal128(precision, scale) => Value::Bytes(decimal::format(
            i256::from_i128(primitive::<Decimal128Type>(array, row)?),
            precision,
            scale,
        )?),
        &DataType::Decimal256(precision, scale) => Value::Bytes(decimal::format(
            primitive::<Decimal256Type>(array, row)?,
            precision,
            scale,
        )?),
        DataType::Date32 => date(array, row)?,
        DataType::Timestamp(unit, zone) => {
            let micros = timestamp(array, row, *unit)?;
            if zone.is_some() && !(1_000_000..=2_147_483_647_999_999).contains(&micros) {
                return Err(format!(
                    "TIMESTAMP {micros} microseconds is outside this server's native range"
                )
                .into());
            }
            temporal::timestamp_parameter(micros)?
        }
        DataType::Duration(unit) => temporal::time_parameter(duration(array, row, *unit)?)?,
        DataType::Time32(_) | DataType::Time64(_) => clock(array, row)?,
        DataType::Interval(IntervalUnit::MonthDayNano) => interval(array, row)?,
        _ => return Err(format!("unsupported Arrow type {data_type}").into()),
    })
}

pub(crate) fn bytes(payload: &[u8]) -> Result<Value> {
    if payload.len() > MAX_PACKET {
        return Err(format!("value exceeds the packet limit of {MAX_PACKET} bytes").into());
    }
    Ok(Value::Bytes(payload.to_vec()))
}

pub(crate) fn text(array: &dyn Array, row: usize) -> Result<&str> {
    match array.data_type() {
        DataType::Utf8 => Ok(cast::<StringArray>(array)?.value(row)),
        DataType::LargeUtf8 => Ok(cast::<LargeStringArray>(array)?.value(row)),
        DataType::Utf8View => Ok(cast::<StringViewArray>(array)?.value(row)),
        _ => Err("expected a UTF-8 string array".into()),
    }
}

fn binary(array: &dyn Array, row: usize) -> Result<&[u8]> {
    match array.data_type() {
        DataType::Binary => Ok(cast::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => Ok(cast::<LargeBinaryArray>(array)?.value(row)),
        DataType::BinaryView => Ok(cast::<BinaryViewArray>(array)?.value(row)),
        DataType::FixedSizeBinary(_) => Ok(cast::<FixedSizeBinaryArray>(array)?.value(row)),
        _ => Err("expected a binary array".into()),
    }
}

fn timestamp(array: &dyn Array, row: usize, unit: TimeUnit) -> Result<i64> {
    let count = match unit {
        TimeUnit::Second => primitive::<TimestampSecondType>(array, row)?,
        TimeUnit::Millisecond => primitive::<TimestampMillisecondType>(array, row)?,
        TimeUnit::Microsecond => primitive::<TimestampMicrosecondType>(array, row)?,
        TimeUnit::Nanosecond => primitive::<TimestampNanosecondType>(array, row)?,
    };
    temporal::microseconds(count, unit)
}

fn duration(array: &dyn Array, row: usize, unit: TimeUnit) -> Result<i64> {
    let count = match unit {
        TimeUnit::Second => primitive::<DurationSecondType>(array, row)?,
        TimeUnit::Millisecond => primitive::<DurationMillisecondType>(array, row)?,
        TimeUnit::Microsecond => primitive::<DurationMicrosecondType>(array, row)?,
        TimeUnit::Nanosecond => primitive::<DurationNanosecondType>(array, row)?,
    };
    temporal::microseconds(count, unit)
}

fn clock(array: &dyn Array, row: usize) -> Result<Value> {
    let micros = match array.data_type() {
        DataType::Time32(TimeUnit::Second) => temporal::microseconds(
            i64::from(primitive::<Time32SecondType>(array, row)?),
            TimeUnit::Second,
        )?,
        DataType::Time32(TimeUnit::Millisecond) => temporal::microseconds(
            i64::from(primitive::<Time32MillisecondType>(array, row)?),
            TimeUnit::Millisecond,
        )?,
        DataType::Time64(TimeUnit::Microsecond) => primitive::<Time64MicrosecondType>(array, row)?,
        DataType::Time64(TimeUnit::Nanosecond) => temporal::microseconds(
            primitive::<Time64NanosecondType>(array, row)?,
            TimeUnit::Nanosecond,
        )?,
        _ => return Err("invalid Arrow clock representation".into()),
    };
    if !(0..86_400_000_000).contains(&micros) {
        return Err(format!("clock {micros} is outside one day").into());
    }
    temporal::time_parameter(micros)
}

fn interval(array: &dyn Array, row: usize) -> Result<Value> {
    let interval = primitive::<IntervalMonthDayNanoType>(array, row)?;
    if interval.months != 0 || interval.days != 0 {
        return Err("calendar months and days cannot be converted to TIME".into());
    }
    temporal::time_parameter(temporal::microseconds(
        interval.nanoseconds,
        TimeUnit::Nanosecond,
    )?)
}

fn date(array: &dyn Array, row: usize) -> Result<Value> {
    let days = primitive::<Date32Type>(array, row)?;
    let date = Date32Type::to_naive_date_opt(days)
        .ok_or_else(|| format!("date {days} is outside the calendar range"))?;
    temporal::date_parameter(date.and_hms_opt(0, 0, 0).ok_or("invalid midnight")?)
}

fn finite_float(number: f32) -> Result<Value> {
    if !number.is_finite() {
        return Err(format!("non-finite FLOAT {number}").into());
    }
    Ok(Value::Float(number))
}

fn finite_double(number: f64) -> Result<Value> {
    if !number.is_finite() {
        return Err(format!("non-finite DOUBLE {number}").into());
    }
    Ok(Value::Double(number))
}

fn primitive<Arrow: ArrowPrimitiveType>(array: &dyn Array, row: usize) -> Result<Arrow::Native> {
    Ok(cast::<PrimitiveArray<Arrow>>(array)?.value(row))
}

pub(crate) fn cast<Column: 'static>(array: &dyn Array) -> Result<&Column> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| format!("expected {}", type_name::<Column>()).into())
}
