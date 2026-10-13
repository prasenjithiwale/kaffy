//! Formatting and parsing of times and positions.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use kaffy_kafka::StartAt;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Epoch milliseconds as local time.
pub fn timestamp(ms: i64) -> String {
    match Local.timestamp_millis_opt(ms).single() {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        None => ms.to_string(),
    }
}

pub fn start_at(start: StartAt) -> String {
    match start {
        StartAt::Beginning => "beginning".to_owned(),
        StartAt::End => "end (new messages only)".to_owned(),
        StartAt::LastN(n) => format!("last {n} per partition"),
        StartAt::Offset(n) => format!("offset {n}"),
        StartAt::Timestamp(ms) => timestamp(ms),
    }
}

/// Accepts relative times (`-15m`, `2h`, `30s`, `1d`, meaning "ago"),
/// `YYYY-MM-DD HH:MM[:SS]` in local time, RFC 3339, or epoch milliseconds.
pub fn parse_timestamp(input: &str, now_ms: i64) -> Option<i64> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    if let Some(ms) = parse_relative(input) {
        return Some(now_ms - ms);
    }
    if let Ok(ms) = input.parse::<i64>() {
        return Some(ms);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Some(dt.timestamp_millis());
    }
    for format in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(input, format) {
            return Local
                .from_local_datetime(&naive)
                .earliest()
                .map(|dt| dt.timestamp_millis());
        }
    }
    None
}

fn parse_relative(input: &str) -> Option<i64> {
    let body = input.strip_prefix('-').unwrap_or(input);
    let unit_ms = match body.chars().last()? {
        's' => 1_000,
        'm' => 60_000,
        'h' => 3_600_000,
        'd' => 86_400_000,
        _ => return None,
    };
    let amount: i64 = body[..body.len() - 1].parse().ok()?;
    (amount >= 0).then_some(amount * unit_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_760_000_000_000;

    #[test]
    fn parses_relative_times() {
        assert_eq!(parse_timestamp("-15m", NOW), Some(NOW - 15 * 60_000));
        assert_eq!(parse_timestamp("2h", NOW), Some(NOW - 2 * 3_600_000));
        assert_eq!(parse_timestamp("-1d", NOW), Some(NOW - 86_400_000));
        assert_eq!(parse_timestamp("-xm", NOW), None);
    }

    #[test]
    fn parses_absolute_times() {
        assert_eq!(
            parse_timestamp("1700000000000", NOW),
            Some(1_700_000_000_000)
        );
        assert_eq!(
            parse_timestamp("2023-11-14T22:13:20Z", NOW),
            Some(1_700_000_000_000)
        );
        let local = parse_timestamp("2023-11-14 22:13", NOW).unwrap();
        assert_eq!(timestamp(local), "2023-11-14 22:13:00.000");
        assert_eq!(parse_timestamp("yesterday", NOW), None);
        assert_eq!(parse_timestamp("", NOW), None);
    }
}
