pub(super) const CANONICAL_DATE_FORMAT: &str = "YYYY-MM-DD";
pub(super) const CANONICAL_TIME_FORMAT: &str = "HH:MM:SS";
pub(super) const CANONICAL_DATETIME_FORMAT: &str = "YYYY-MM-DD HH:MM:SS";

pub(super) fn normalize_date(s: &str) -> Option<String> {
    parse_canonical_date(s).map(|(y, m, d)| format!("{:04}-{:02}-{:02}", y, m, d))
}

pub(super) fn normalize_time(s: &str) -> Option<String> {
    parse_canonical_time(s).map(|(h, m, sec)| format!("{:02}:{:02}:{:02}", h, m, sec))
}

pub(super) fn normalize_datetime(s: &str) -> Option<String> {
    let (date_part, time_part) = s.split_once(' ')?;
    let (y, month, day) = parse_canonical_date(date_part)?;
    let (hour, minute, second) = parse_canonical_time(time_part)?;
    Some(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y, month, day, hour, minute, second
    ))
}

pub(super) fn datetime_date_part(s: &str) -> Option<String> {
    normalize_datetime(s).map(|dt| dt[..10].to_string())
}

pub(super) fn datetime_time_part(s: &str) -> Option<String> {
    normalize_datetime(s).map(|dt| dt[11..].to_string())
}

pub(super) fn parse_date_components(s: &str) -> Option<(i64, i64, i64)> {
    if let Some(parts) = parse_canonical_date(s) {
        return Some(parts);
    }

    let (date_part, time_part) = s.split_once(' ')?;
    parse_canonical_time(time_part)?;
    parse_canonical_date(date_part)
}

fn parse_canonical_date(s: &str) -> Option<(i64, i64, i64)> {
    let bytes = s.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }

    let y = parse_fixed_digits(&bytes[0..4])? as i64;
    let m = parse_fixed_digits(&bytes[5..7])? as i64;
    let d = parse_fixed_digits(&bytes[8..10])? as i64;

    if !(1..=12).contains(&m) {
        return None;
    }
    if !(1..=days_in_month(y, m)).contains(&d) {
        return None;
    }

    Some((y, m, d))
}

fn parse_canonical_time(s: &str) -> Option<(i64, i64, i64)> {
    let bytes = s.as_bytes();
    if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
        return None;
    }

    let h = parse_fixed_digits(&bytes[0..2])? as i64;
    let m = parse_fixed_digits(&bytes[3..5])? as i64;
    let sec = parse_fixed_digits(&bytes[6..8])? as i64;

    if h > 23 || m > 59 || sec > 59 {
        return None;
    }

    Some((h, m, sec))
}

fn parse_fixed_digits(bytes: &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u64::from(byte - b'0');
    }
    Some(value)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(super) fn ymd_to_days(y: i64, m: i64, d: i64) -> i64 {
    let m_adj = if m <= 2 { m + 9 } else { m - 3 };
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 {
        y_adj / 400
    } else {
        (y_adj - 399) / 400
    };
    let yoe = y_adj - era * 400;
    let doy = (153 * m_adj + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub(super) fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
