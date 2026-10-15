//! Exact conversion between PostgreSQL base-10000 numerics and Arrow decimal mantissas.

use std::fmt::Write as _;

use arrow::datatypes::i256;
use bytes::{Buf as _, BufMut as _, BytesMut};
use transferred_core::{Result, TransferredError};

/// Decodes a finite numeric without rounding to the requested decimal shape.
pub(crate) fn decode(mut bytes: &[u8], precision: u8, scale: u8) -> Result<i256> {
    let (count, weight, sign) = header(&mut bytes)?;
    if usize::try_from(count)
        .ok()
        .map(|length| length.saturating_mul(2))
        != Some(bytes.len())
    {
        return Err(TransferredError::in_source("invalid numeric digit count"));
    }
    let coefficient = coefficient(bytes)?;
    let exponent = i32::from(weight)
        .saturating_add(1)
        .saturating_sub(i32::from(count))
        .saturating_mul(4)
        .saturating_add(i32::from(scale));
    scaled_units(&coefficient, exponent, sign, precision)
}

/// Encodes a signed mantissa with its decimal scale into a PostgreSQL numeric field.
pub(crate) fn encode(units: i256, scale: u8, buf: &mut BytesMut) -> Result<()> {
    let text = units.to_string();
    let (groups, integer_groups) = groups(text.trim_start_matches('-'), scale)?;
    let (weight, significant) = trimmed(&groups, integer_groups)?;
    buf.put_i16(i16::try_from(significant.len()).map_err(TransferredError::in_destination)?);
    buf.put_i16(weight);
    buf.put_u16(if text.starts_with('-') && !significant.is_empty() {
        0x4000
    } else {
        0
    });
    buf.put_u16(u16::from(scale));
    for group in significant {
        buf.put_u16(*group);
    }
    Ok(())
}

#[expect(
    clippy::type_complexity,
    reason = "returns the wire header's digit count, weight and sign without a single-use type"
)]
fn header(bytes: &mut &[u8]) -> Result<(i16, i16, u16)> {
    let count = bytes.try_get_i16().map_err(TransferredError::in_source)?;
    let weight = bytes.try_get_i16().map_err(TransferredError::in_source)?;
    let sign = bytes.try_get_u16().map_err(TransferredError::in_source)?;
    let scale = bytes.try_get_u16().map_err(TransferredError::in_source)?;
    if !matches!(sign, 0 | 0x4000) || scale > 0x3fff {
        return Err(TransferredError::in_source("non-finite or invalid numeric"));
    }
    Ok((count, weight, sign))
}

fn coefficient(bytes: &[u8]) -> Result<String> {
    let mut coefficient = String::new();
    for pair in bytes.as_chunks::<2>().0 {
        let digit = u16::from_be_bytes(*pair);
        if digit >= 10_000 {
            return Err(TransferredError::in_source(
                "invalid base-10000 numeric digit",
            ));
        }
        write!(coefficient, "{digit:04}").map_err(TransferredError::in_source)?;
    }
    Ok(coefficient)
}

#[expect(
    clippy::type_complexity,
    reason = "the digits and integer-group count come from the same padding operation"
)]
fn groups(digits: &str, scale: u8) -> Result<(Vec<u16>, usize)> {
    let width = digits.len().max(usize::from(scale).saturating_add(1));
    let padded = format!("{digits:0>width$}");
    let integer_digits = width.saturating_sub(usize::from(scale));
    let leading = (4_usize.saturating_sub(integer_digits % 4)) % 4;
    let trailing = (4_usize.saturating_sub(usize::from(scale) % 4)) % 4;
    let grouped = format!("{}{}{}", "0".repeat(leading), padded, "0".repeat(trailing));
    let groups = grouped
        .as_bytes()
        .as_chunks::<4>()
        .0
        .iter()
        .copied()
        .map(parse_group)
        .collect::<Result<Vec<_>>>()?;
    let integer_groups = integer_digits
        .saturating_add(leading)
        .checked_div(4)
        .unwrap_or_default();
    Ok((groups, integer_groups))
}

fn parse_group(chunk: [u8; 4]) -> Result<u16> {
    str::from_utf8(&chunk)
        .map_err(TransferredError::in_destination)?
        .parse()
        .map_err(TransferredError::in_destination)
}

#[expect(
    clippy::type_complexity,
    reason = "the trimmed digits borrow the input and carry their adjusted wire weight"
)]
fn trimmed(groups: &[u16], integer_groups: usize) -> Result<(i16, &[u16])> {
    let Some(first) = groups.iter().position(|group| *group != 0) else {
        return Ok((0, &[]));
    };
    let end = groups
        .iter()
        .rposition(|group| *group != 0)
        .map_or(first, |last| last.saturating_add(1));
    let weight = i16::try_from(integer_groups)
        .map_err(TransferredError::in_destination)?
        .saturating_sub(i16::try_from(first).map_err(TransferredError::in_destination)?)
        .saturating_sub(1);
    Ok((weight, groups.get(first..end).unwrap_or_default()))
}

fn scaled_units(coefficient: &str, exponent: i32, sign: u16, precision: u8) -> Result<i256> {
    let mut significant = coefficient.trim_start_matches('0').to_owned();
    if significant.is_empty() {
        return Ok(i256::ZERO);
    }
    if exponent < 0 {
        truncate(&mut significant, exponent.unsigned_abs())?;
    } else {
        pad(&mut significant, exponent.cast_unsigned(), precision)?;
    }
    if significant.len() > usize::from(precision) {
        return Err(TransferredError::in_source(
            "numeric exceeds the Arrow precision",
        ));
    }
    if sign == 0x4000 {
        significant.insert(0, '-');
    }
    significant.parse().map_err(TransferredError::in_source)
}

fn truncate(significant: &mut String, count: u32) -> Result<()> {
    let remove = usize::try_from(count).map_err(TransferredError::in_source)?;
    let split = significant.len().checked_sub(remove).ok_or_else(|| {
        TransferredError::in_source("numeric requires rounding to the Arrow scale")
    })?;
    if !significant
        .get(split..)
        .is_some_and(|tail| tail.bytes().all(|digit| digit == b'0'))
    {
        return Err(TransferredError::in_source(
            "numeric requires rounding to the Arrow scale",
        ));
    }
    significant.truncate(split);
    Ok(())
}

fn pad(significant: &mut String, count: u32, precision: u8) -> Result<()> {
    let padding = usize::try_from(count).map_err(TransferredError::in_source)?;
    if significant.len().saturating_add(padding) > usize::from(precision) {
        return Err(TransferredError::in_source(
            "numeric exceeds the Arrow precision",
        ));
    }
    significant.push_str(&"0".repeat(padding));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_wire_values_without_an_intermediate_float() {
        assert_eq!(
            decode(&[0, 2, 0, 0, 0, 0, 0, 4, 0, 1, 19, 136], 38, 9).unwrap(),
            i256::from_i128(1_500_000_000)
        );
        assert_eq!(
            decode(&[0, 1, 255, 255, 64, 0, 0, 4, 9, 196], 38, 9).unwrap(),
            i256::from_i128(-250_000_000)
        );
        assert!(decode(&[0, 1, 255, 255, 0, 0, 0, 4, 0, 1], 38, 3).is_err());
    }

    #[test]
    fn handles_full_precision_at_every_scale() {
        let units: i256 = "9".repeat(76).parse().unwrap();
        for scale in 0..=76 {
            let mut bytes = BytesMut::new();
            encode(units, scale, &mut bytes).unwrap();
            assert_eq!(decode(&bytes, 76, scale).unwrap(), units);
        }
    }

    #[test]
    fn refuses_nonfinite_malformed_and_out_of_precision_values() {
        for bytes in [
            vec![],
            vec![0, 0, 0, 0, 192, 0, 0, 0],
            vec![0, 1, 0, 0, 0, 0, 0, 0],
            vec![0, 1, 0, 0, 0, 0, 0, 0, 39, 16],
        ] {
            assert!(decode(&bytes, 38, 9).is_err());
        }
        assert!(decode(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 1], 2, 2).is_err());
    }
}
