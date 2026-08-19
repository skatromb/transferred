//! Scalar conversions both directions, plus the typmod decoders reading a column's precision.
//! `pg_*` converts towards Postgres, the rest towards Arrow;
//! wire framing and builders stay with whichever module owns them.

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, Date32Type, IntervalMonthDayNano};
use chrono::{DateTime, NaiveDate, Utc};
use pg_interval::Interval as PgInterval;
use rust_decimal::Decimal;
use transferred_core::{Result, TransferredError};

/// Precision and scale for bare `numeric`, matching BQ `NUMERIC` so it lands there uncoerced.
const BARE_NUMERIC: (u8, i8) = (DECIMAL128_MAX_PRECISION, 9);

/// Typmod PG reports for a `numeric` declared without precision.
pub const BARE_NUMERIC_TYPMOD: i32 = -1;

/// PG Typmod for a `geo...` without a coordinate system. Each value's own EWKB can name one.
const UNCONSTRAINED_GEO_TYPMOD: i32 = -1;

/// `PostGIS` spells "coordinate system unknown" as SRID 0.
const UNKNOWN_SRID: i32 = 0;

/// The `PostGIS` type names, which carry no fixed OID: `CREATE EXTENSION` assigns one per database.
pub const GEOMETRY: &str = "geometry";
pub const GEOGRAPHY: &str = "geography";

/// Case-insensitive text, another extension type answering to a name rather than a fixed OID.
pub const CITEXT: &str = "citext";

/// PG counts sub-second time in microseconds; Arrow intervals count nanoseconds.
const NANOS_PER_MICRO: i64 = 1_000;

/// Decodes a `numeric` typmod, defaulting bare `numeric` `-1` to (38,9).
pub fn numeric_precision_scale(typmod: i32) -> Result<(u8, i8)> {
    if typmod == BARE_NUMERIC_TYPMOD {
        return Ok(BARE_NUMERIC);
    }

    // `numeric_typmod_precision`/`numeric_typmod_scale`, minus `VARHDRSZ`; the XOR sign-extends the
    // 11-bit scale, which PG 15+ allows to be negative.
    // https://github.com/postgres/postgres/blob/REL_17_10/src/backend/utils/adt/numeric.c#L925
    let precision = ((typmod - 4) >> 16) & 0xffff;
    let scale = (((typmod - 4) & 0x7ff) ^ 0x400) - 0x400;

    // PG holds 1000 digits to Arrow's 38, and PG 15+ decouples scale from precision, so a narrow
    // column may still carry a scale that busts an i8 or outruns its own precision.
    match (u8::try_from(precision), i8::try_from(scale)) {
        (Ok(precision), Ok(scale))
            if precision <= DECIMAL128_MAX_PRECISION
                && i32::from(scale) <= i32::from(precision) =>
        {
            Ok((precision, scale))
        }
        _ => Err(TransferredError::source(format!(
            "`numeric({precision},{scale})` is outside Arrow `Decimal128`, which holds \
             {DECIMAL128_MAX_PRECISION} digits and no more scale than precision"
        ))),
    }
}

/// Decodes the SRID a `geometry`/`geography` typmod pins its column to, if it pins one at all.
pub fn geo_srid(typmod: i32) -> Option<i32> {
    // `TYPMOD_GET_SRID`: 20 SRID bits sitting above the 8 that hold the geometry subtype.
    // https://github.com/postgis/postgis/blob/3.6.0/postgis/gserialized_typmod.c
    let srid = (typmod & 0x0fff_ff00) >> 8;

    (typmod != UNCONSTRAINED_GEO_TYPMOD && srid != UNKNOWN_SRID).then_some(srid)
}

/// Restates a decimal as an integer count of `10^-scale` units, as Arrow `Decimal128` stores it.
pub fn decimal_units(mut decimal: Decimal, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).map_err(|_| {
        TransferredError::source("`numeric` with negative scale is not supported in 0.1")
    })?;

    decimal.rescale(scale);
    if decimal.scale() != scale {
        return Err(TransferredError::source(format!(
            "`numeric` value {decimal} does not fit scale {scale}"
        )));
    }

    Ok(decimal.mantissa())
}

/// PG counts interval time in microseconds; Arrow wants nanoseconds, which overflow past ~292 years.
pub fn month_day_nano(interval: PgInterval) -> Result<IntervalMonthDayNano> {
    let nanos = interval
        .microseconds
        .checked_mul(NANOS_PER_MICRO)
        .ok_or_else(|| {
            TransferredError::source(
                "`interval` exceeds the nanosecond range of Arrow `Interval(MonthDayNano)`",
            )
        })?;

    Ok(IntervalMonthDayNano::new(
        interval.months,
        interval.days,
        nanos,
    ))
}

/// Restates an Arrow count of `10^-scale` units as a decimal, as PG `numeric` carries it.
pub fn pg_numeric(units: i128, scale: i8) -> Result<Decimal> {
    let scale = u32::try_from(scale).map_err(|_| {
        TransferredError::destination("`Decimal128` with negative scale is not supported in 0.1")
    })?;

    Decimal::try_from_i128_with_scale(units, scale).map_err(TransferredError::destination)
}

/// PG counts interval time in microseconds, so anything finer than a microsecond has nowhere to go.
pub fn pg_interval(interval: IntervalMonthDayNano) -> Result<PgInterval> {
    if interval.nanoseconds % NANOS_PER_MICRO != 0 {
        return Err(TransferredError::destination(format!(
            "`interval` of {}ns is finer than the microsecond Postgres stores",
            interval.nanoseconds
        )));
    }

    Ok(PgInterval::new(
        interval.months,
        interval.days,
        interval.nanoseconds / NANOS_PER_MICRO,
    ))
}

/// Restates a count of days from the epoch as a date, as PG stores it.
pub fn pg_date(days: i32) -> Result<NaiveDate> {
    Date32Type::to_naive_date_opt(days).ok_or_else(|| {
        TransferredError::destination(format!("`date` {days} days from epoch is out of range"))
    })
}

/// Restates a count of microseconds from the epoch as a UTC instant, as PG stores it.
pub fn pg_timestamp(micros: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(micros).ok_or_else(|| {
        TransferredError::destination(format!("timestamp {micros}µs from epoch is out of range"))
    })
}

/// Reads 16 Arrow bytes as a uuid.
pub fn pg_uuid(bytes: &[u8]) -> Result<uuid::Uuid> {
    uuid::Uuid::from_slice(bytes).map_err(TransferredError::destination)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// Typmods as PG 17 stores them in `pg_attribute.atttypmod`, pinned here rather than reused
    /// from the constants above, so a wrong constant fails a test instead of agreeing with it.
    const NUMERIC_BARE: i32 = -1;
    const NUMERIC_18_4: i32 = 1_179_656;
    const NUMERIC_38_9: i32 = 2_490_381;
    const NUMERIC_5_NEG2: i32 = 329_730;
    const NUMERIC_1000_500: i32 = 65_536_504;
    const NUMERIC_5_200: i32 = 327_884;
    const NUMERIC_5_10: i32 = 327_694;
    const GEOMETRY_BARE: i32 = -1;
    const GEOMETRY_POINT: i32 = 4;
    const GEOMETRY_POINT_4326: i32 = 1_107_460;
    const GEOMETRY_ANY_4326: i32 = 1_107_456;

    /// Largest mantissa `rust_decimal` can hold: 2^96 - 1, with no room to zero-pad.
    const U96_MAX: Decimal = Decimal::from_parts(u32::MAX, u32::MAX, u32::MAX, false, 0);

    #[test]
    fn typmod_decodes_declared_precision_and_scale() {
        assert_eq!(numeric_precision_scale(NUMERIC_18_4).unwrap(), (18, 4));
        assert_eq!(numeric_precision_scale(NUMERIC_38_9).unwrap(), (38, 9));
    }

    #[test]
    fn bare_numeric_defaults_to_bq_numeric_shape() {
        assert_eq!(numeric_precision_scale(NUMERIC_BARE).unwrap(), (38, 9));
    }

    /// PG 15+ allows negative scale; the decode must not read it as a large positive one.
    #[test]
    fn typmod_keeps_negative_scale_negative() {
        assert_eq!(numeric_precision_scale(NUMERIC_5_NEG2).unwrap(), (5, -2));
    }

    #[test]
    fn typmod_rejects_precision_past_decimal128() {
        assert!(numeric_precision_scale(NUMERIC_1000_500).is_err());
    }

    /// PG 15+ decouples scale from precision, so a narrow column can still carry an i8-busting scale.
    #[test]
    fn typmod_rejects_scale_past_i8() {
        assert!(numeric_precision_scale(NUMERIC_5_200).is_err());
    }

    /// PG takes `numeric(5,10)`; Arrow `Decimal128` does not, and would build a broken array from it.
    #[test]
    fn typmod_rejects_scale_wider_than_precision() {
        assert!(numeric_precision_scale(NUMERIC_5_10).is_err());
    }

    /// The SRID sits above the subtype bits, so constraining one must not disturb the other.
    #[test]
    fn geo_typmod_decodes_the_declared_srid() {
        assert_eq!(geo_srid(GEOMETRY_POINT_4326), Some(4326));
        assert_eq!(geo_srid(GEOMETRY_ANY_4326), Some(4326));
    }

    /// An unconstrained column takes rows with differing SRIDs, so it has no single one to report.
    /// Masking `-1` blindly would read it as SRID 1048575.
    #[test]
    fn geo_typmod_reports_no_srid_for_an_unconstrained_column() {
        assert_eq!(geo_srid(GEOMETRY_BARE), None);
    }

    /// `geometry(Point)` pins the subtype only, which `PostGIS` records as SRID 0 — its own "unknown".
    #[test]
    fn geo_typmod_reports_no_srid_for_postgis_unknown() {
        assert_eq!(geo_srid(GEOMETRY_POINT), None);
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
    fn decimal_units_rejects_negative_scale() {
        assert!(decimal_units(Decimal::new(15, 1), -2).is_err());
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

    #[test]
    fn pg_numeric_restates_units_at_scale() {
        assert_eq!(pg_numeric(1_500_000_000, 9).unwrap(), Decimal::new(15, 1));
        assert_eq!(pg_numeric(-250_000_000, 9).unwrap(), Decimal::new(-25, 2));
        assert_eq!(pg_numeric(0, 4).unwrap(), Decimal::ZERO);
    }

    /// Arrow holds 38 digits, `rust_decimal` 29, so the widest Arrow decimals have nowhere to land.
    #[test]
    fn pg_numeric_rejects_units_past_the_rust_decimal_mantissa() {
        assert!(pg_numeric(i128::MAX, 9).is_err());
    }

    #[test]
    fn pg_numeric_rejects_negative_scale() {
        assert!(pg_numeric(15, -2).is_err());
    }

    #[test]
    fn pg_interval_keeps_months_days_and_micros_separate() {
        let interval = pg_interval(IntervalMonthDayNano::new(14, 3, 14_706_789_000_000)).unwrap();
        assert_eq!(
            (interval.months, interval.days, interval.microseconds),
            (14, 3, 14_706_789_000)
        );
    }

    #[test]
    fn pg_interval_rejects_sub_microsecond_precision() {
        assert!(pg_interval(IntervalMonthDayNano::new(0, 0, 1)).is_err());
    }
}
