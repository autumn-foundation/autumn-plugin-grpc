//! The `grpc-timeout` header: parse (server side) and encode (client
//! side). The value is at most 8 digits and a unit (`H M S m u n`).

use std::time::Duration;

/// Largest value with 8 digits.
#[cfg_attr(not(feature = "client"), allow(dead_code))]
const MAX_VALUE: u128 = 99_999_999;

/// Parse a `grpc-timeout` value. A bad value gives `None`.
pub fn parse(value: &str) -> Option<Duration> {
    let unit = value.chars().last()?;
    let digits = value.get(..value.len() - unit.len_utf8())?;
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let amount: u64 = digits.parse().ok()?;
    Some(match unit {
        'H' => Duration::from_secs(amount * 3600),
        'M' => Duration::from_secs(amount * 60),
        'S' => Duration::from_secs(amount),
        'm' => Duration::from_millis(amount),
        'u' => Duration::from_micros(amount),
        'n' => Duration::from_nanos(amount),
        _ => return None,
    })
}

/// Encode `timeout` with the finest unit that fits in 8 digits. The value
/// rounds down, so the sent timeout is never longer than `timeout`.
#[cfg_attr(not(feature = "client"), allow(dead_code))]
pub fn encode(timeout: Duration) -> String {
    let units: [(u128, char); 6] = [
        (1, 'n'),
        (1_000, 'u'),
        (1_000_000, 'm'),
        (1_000_000_000, 'S'),
        (60_000_000_000, 'M'),
        (3_600_000_000_000, 'H'),
    ];
    let nanos = timeout.as_nanos();
    for (size, unit) in units {
        let value = nanos / size;
        if value <= MAX_VALUE {
            return format!("{value}{unit}");
        }
    }
    format!("{MAX_VALUE}H")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::unchecked_time_subtraction)]
mod tests {
    use super::*;

    #[test]
    fn values_parse_as_in_the_spec() {
        for (value, expected) in [
            ("3H", Duration::from_secs(3 * 3600)),
            ("1M", Duration::from_secs(60)),
            ("42S", Duration::from_secs(42)),
            ("13m", Duration::from_millis(13)),
            ("2u", Duration::from_micros(2)),
            ("82n", Duration::from_nanos(82)),
            ("99999999S", Duration::from_secs(99_999_999)),
        ] {
            assert_eq!(parse(value), Some(expected), "{value}");
        }
        for value in ["", "S", "123456789S", "5x", "-1S", "1.5S", "5é"] {
            assert_eq!(parse(value), None, "{value}");
        }
    }

    #[test]
    fn encode_uses_the_finest_unit_that_fits() {
        for (timeout, expected) in [
            (Duration::ZERO, "0n"),
            (Duration::from_nanos(99_999_999), "99999999n"),
            (Duration::from_millis(100), "100000u"),
            (Duration::from_millis(1_500), "1500000u"),
            (Duration::from_secs(100), "100000m"),
            (Duration::from_secs(100_000), "100000S"),
            (Duration::from_secs(100_000_000), "1666666M"),
            (Duration::from_secs(10_000_000_000), "2777777H"),
            (Duration::MAX, "99999999H"),
        ] {
            assert_eq!(encode(timeout), expected, "{timeout:?}");
        }
    }

    #[test]
    fn encode_rounds_down_and_parses_back() {
        for millis in [1, 7, 999, 1_001, 59_999, 86_400_000] {
            let timeout = Duration::from_millis(millis) + Duration::from_nanos(123);
            let parsed = parse(&encode(timeout)).unwrap();
            assert!(parsed <= timeout, "{timeout:?} -> {parsed:?}");
            assert!(timeout - parsed < Duration::from_millis(1), "{timeout:?}");
        }
    }
}
