//! Lossless MySQL temporal values in UTC or microseconds.

use crate::error::Result;
use arrow_schema::TimeUnit;
use chrono::{DateTime, Datelike as _, NaiveDate, NaiveDateTime, Timelike as _, Utc};
use mysql_async::Value;

const MAX_TIME: i64 = 3_020_399_000_000;

pub(crate) fn datetime(cell: &Value) -> Result<NaiveDateTime> {
    let Value::Date(year, month, day, hour, minute, second, micros) = *cell else {
        return Err("expected a binary date or timestamp".into());
    };
    let date = NaiveDate::from_ymd_opt(i32::from(year), u32::from(month), u32::from(day))
        .filter(|_| (1..=9999).contains(&year));
    date.and_then(|calendar| calendar.and_hms_micro_opt(u32::from(hour), u32::from(minute), u32::from(second), micros))
        .ok_or_else(|| format!("invalid date {year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}").into())
}

pub(crate) fn duration(cell: &Value) -> Result<i64> {
    let Value::Time(negative, days, hours, minutes, seconds, micros) = *cell else {
        return Err("expected a binary TIME duration".into());
    };
    if days > 34 || hours > 23 || minutes > 59 || seconds > 59 || micros > 999_999 {
        return Err("invalid TIME components".into());
    }
    let magnitude = i64::from(days)
        .saturating_mul(86_400_000_000)
        .saturating_add(i64::from(hours).saturating_mul(3_600_000_000))
        .saturating_add(i64::from(minutes).saturating_mul(60_000_000))
        .saturating_add(i64::from(seconds).saturating_mul(1_000_000))
        .saturating_add(i64::from(micros));
    let signed = if negative {
        magnitude.saturating_neg()
    } else {
        magnitude
    };
    check_duration(signed)?;
    Ok(signed)
}

pub(crate) fn microseconds(count: i64, unit: TimeUnit) -> Result<i64> {
    let micros = match unit {
        TimeUnit::Second => count.checked_mul(1_000_000),
        TimeUnit::Millisecond => count.checked_mul(1_000),
        TimeUnit::Microsecond => Some(count),
        TimeUnit::Nanosecond if count % 1_000 == 0 => Some(count.div_euclid(1_000)),
        TimeUnit::Nanosecond => {
            return Err(format!("{count} nanoseconds requires rounding to microseconds").into());
        }
    };
    micros.ok_or_else(|| "time overflows the microsecond range".into())
}

pub(crate) fn date_parameter(date: NaiveDateTime) -> Result<Value> {
    if !(1..=9999).contains(&date.year()) {
        return Err(format!("date {date} is outside MySQL's year range").into());
    }
    Ok(Value::Date(
        u16::try_from(date.year())?,
        u8::try_from(date.month())?,
        u8::try_from(date.day())?,
        u8::try_from(date.hour())?,
        u8::try_from(date.minute())?,
        u8::try_from(date.second())?,
        date.and_utc().timestamp_subsec_micros(),
    ))
}

pub(crate) fn timestamp_parameter(micros: i64) -> Result<Value> {
    let instant = DateTime::<Utc>::from_timestamp_micros(micros)
        .ok_or("timestamp is outside the calendar range")?;
    date_parameter(instant.naive_utc())
}

pub(crate) fn time_parameter(micros: i64) -> Result<Value> {
    check_duration(micros)?;
    let magnitude = micros.unsigned_abs();
    Ok(Value::Time(
        micros.is_negative(),
        u32::try_from(magnitude.div_euclid(86_400_000_000))?,
        u8::try_from(magnitude.div_euclid(3_600_000_000) % 24)?,
        u8::try_from(magnitude.div_euclid(60_000_000) % 60)?,
        u8::try_from(magnitude.div_euclid(1_000_000) % 60)?,
        u32::try_from(magnitude % 1_000_000)?,
    ))
}

fn check_duration(micros: i64) -> Result<()> {
    if !(-MAX_TIME..=MAX_TIME).contains(&micros) {
        return Err(format!("TIME {micros} microseconds exceeds 838:59:59.000000").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_zero_partial_and_impossible_dates() {
        for date in [
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            Value::Date(2024, 0, 1, 0, 0, 0, 0),
            Value::Date(2023, 2, 29, 0, 0, 0, 0),
        ] {
            assert!(datetime(&date).is_err());
        }
    }

    #[test]
    fn retains_negative_multi_day_durations() {
        let wire = Value::Time(true, 2, 0, 0, 0, 123_456);
        assert_eq!(duration(&wire).unwrap(), -172_800_123_456);
        assert_eq!(time_parameter(-172_800_123_456).unwrap(), wire);
        assert!(time_parameter(3_020_400_000_000).is_err());
    }

    #[test]
    fn refuses_fractions_beyond_mysql_time_bounds() {
        for micros in [-3_020_399_000_001, 3_020_399_000_001] {
            assert!(time_parameter(micros).is_err(), "{micros}");
        }
        for micros in [-3_020_399_000_000, 3_020_399_000_000] {
            assert_eq!(duration(&time_parameter(micros).unwrap()).unwrap(), micros);
        }
    }

    #[test]
    fn rejects_fractional_microseconds_and_overflow() {
        assert_eq!(microseconds(-1_000, TimeUnit::Nanosecond).unwrap(), -1);
        assert!(microseconds(-1, TimeUnit::Nanosecond).is_err());
        assert!(microseconds(i64::MAX, TimeUnit::Second).is_err());
    }
}
