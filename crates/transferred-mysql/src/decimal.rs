//! Exact decimal text exchanged with MySQL, without floating-point intermediates.

use crate::error::Result;
use arrow::datatypes::i256;

pub(crate) fn parse(text: &str, precision: u8, scale: i8) -> Result<i256> {
    let digits = coefficient(text, scale)?;
    let significant = digits.trim_start_matches('0');
    if significant.len() > usize::from(precision) {
        return Err(format!("decimal {text} exceeds precision {precision}").into());
    }
    if significant.is_empty() {
        return Ok(i256::ZERO);
    }
    let signed = if text.starts_with('-') {
        format!("-{significant}")
    } else {
        significant.to_owned()
    };
    Ok(signed.parse()?)
}

pub(crate) fn format(units: i256, precision: u8, scale: i8) -> Result<Vec<u8>> {
    let text = units.to_string();
    let magnitude = text.trim_start_matches('-');
    if magnitude.len() > usize::from(precision) {
        return Err(format!("decimal mantissa {text} exceeds precision {precision}").into());
    }
    let places = usize::try_from(scale)?;
    let width = magnitude.len().max(places.saturating_add(1));
    let mut padded = format!("{magnitude:0>width$}");
    if places > 0 {
        padded.insert(padded.len().saturating_sub(places), '.');
    }
    if text.starts_with('-') {
        padded.insert(0, '-');
    }
    Ok(padded.into_bytes())
}

fn coefficient(text: &str, scale: i8) -> Result<String> {
    let magnitude = text.strip_prefix('-').unwrap_or(text);
    let (integer, fraction) = magnitude.split_once('.').unwrap_or((magnitude, ""));
    if integer.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|digit| digit.is_ascii_digit())
    {
        return Err(format!("invalid decimal {text}").into());
    }
    let restated = scaled_fraction(fraction, usize::try_from(scale)?)
        .map_err(|error| format!("decimal {text}: {error}"))?;
    Ok(format!("{integer}{restated}"))
}

fn scaled_fraction(fraction: &str, places: usize) -> Result<String> {
    if let Some((kept, removed)) = fraction.split_at_checked(places) {
        if !removed.bytes().all(|digit| digit == b'0') {
            return Err("decimal requires rounding".into());
        }
        Ok(kept.to_owned())
    } else {
        Ok(format!("{fraction:0<places$}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fractional_zeros_but_never_rounds() {
        assert_eq!(parse("-0.0012300", 5, 5).unwrap(), i256::from_i128(-123));
        assert!(parse("0.0012301", 5, 5).is_err());
        assert!(parse("1e5", 10, 0).is_err());
        assert!(parse("NaN", 10, 0).is_err());
        assert!(parse("1000", 3, 0).is_err());
    }

    #[test]
    fn emits_server_decimal_text_exactly() {
        assert_eq!(format(i256::from_i128(-123), 5, 5).unwrap(), b"-0.00123");
        assert_eq!(
            format(i256::ZERO, 65, 38).unwrap(),
            b"0.00000000000000000000000000000000000000"
        );
        assert!(format(i256::from_i128(1000), 3, 0).is_err());
    }
}
