//! Text input and output of values, matching PostgreSQL's type input and
//! output functions (int4in/int4out, float8in/float8out, boolin/boolout,
//! timestamp_in/timestamp_out, textin/textout).

use super::error::{Result, SqlError, code};
use crate::types::{DataType, TimestampMicros, Value};

/// The text form of a value as PostgreSQL sends it; `None` for NULL.
pub fn format_value(v: &Value) -> Option<String> {
    Some(match v {
        Value::Null => return None,
        Value::Int4(x) => x.to_string(),
        Value::Int8(x) => x.to_string(),
        Value::Bool(b) => if *b { "t" } else { "f" }.to_owned(),
        Value::Float8(f) => format_float8(*f),
        Value::Timestamp(t) => format_timestamp(*t),
        Value::Text(s) => s.clone(),
    })
}

/// float8out with extra_float_digits > 0 (PostgreSQL 12+ default): the
/// shortest decimal that reads back exactly, in exponent form when the
/// decimal exponent is below -4 or at least 15.
pub fn format_float8(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_owned();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    let sign = if f.is_sign_negative() { "-" } else { "" };
    // Rust's `{:e}` is the shortest round-trip form: "d.ddde-x".
    let e = format!("{:e}", f.abs());
    let (mantissa, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if f == 0.0 {
        return format!("{sign}0");
    }
    if !(-4..15).contains(&exp) {
        let rest = &digits[1..];
        let m = if rest.is_empty() {
            digits[..1].to_owned()
        } else {
            format!("{}.{}", &digits[..1], rest)
        };
        let esign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{m}e{esign}{:02}", exp.abs());
    }
    let point = exp + 1; // digits before the decimal point
    let s = if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if point as usize >= digits.len() {
        format!("{}{}", digits, "0".repeat(point as usize - digits.len()))
    } else {
        let (a, b) = digits.split_at(point as usize);
        format!("{a}.{b}")
    };
    format!("{sign}{s}")
}

const USECS_PER_DAY: i64 = 86_400_000_000;
/// Days from 1970-01-01 to 2000-01-01.
const EPOCH_2000_DAYS: i64 = 10_957;

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        4 | 6 | 9 | 11 => 30,
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        _ => 31,
    }
}

/// timestamp_out in ISO style: `YYYY-MM-DD HH:MM:SS[.ffffff]` with
/// trailing zeros of the fraction removed (` BC` for years before 1).
pub fn format_timestamp(t: TimestampMicros) -> String {
    let days = t.div_euclid(USECS_PER_DAY);
    let us = t.rem_euclid(USECS_PER_DAY);
    let (y, m, d) = civil_from_days(days + EPOCH_2000_DAYS);
    let secs = us / 1_000_000;
    let frac = us % 1_000_000;
    let (year, bc) = if y <= 0 { (1 - y, " BC") } else { (y, "") };
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year,
        m,
        d,
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    );
    if frac != 0 {
        let f = format!("{frac:06}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push_str(bc);
    s
}

fn invalid(type_name: &str, s: &str) -> SqlError {
    SqlError::new(
        code::INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type {type_name}: \"{s}\""),
    )
}

fn out_of_range(type_name: &str, s: &str) -> SqlError {
    SqlError::new(
        code::NUMERIC_VALUE_OUT_OF_RANGE,
        format!("value \"{s}\" is out of range for type {type_name}"),
    )
}

/// Parses a decimal integer the way pg_strtoint64 does: surrounding
/// whitespace, an optional sign, digits with single underscores between
/// them. `None` on bad syntax, `Some(None)` on overflow.
fn parse_int(s: &str) -> Option<Option<i64>> {
    let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
    let (neg, digits) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') {
        return None;
    }
    // Accumulate as a negative number so i64::MIN fits.
    let mut acc: Option<i64> = Some(0);
    let mut prev_us = false;
    for c in digits.chars() {
        if c == '_' {
            if prev_us {
                return None;
            }
            prev_us = true;
            continue;
        }
        prev_us = false;
        let d = c.to_digit(10)?;
        acc = acc.and_then(|a| a.checked_mul(10)?.checked_sub(i64::from(d)));
    }
    Some(match acc {
        Some(a) if neg => Some(a),
        Some(a) => a.checked_neg(),
        None => None,
    })
}

fn parse_bool(s: &str) -> Option<bool> {
    let t = s
        .trim_matches(|c: char| c.is_ascii_whitespace())
        .to_ascii_lowercase();
    let n = t.len();
    let prefix_of = |word: &str, min: usize| n >= min && n <= word.len() && word.starts_with(&t);
    match t.as_bytes().first()? {
        b't' if prefix_of("true", 1) => Some(true),
        b'y' if prefix_of("yes", 1) => Some(true),
        b'f' if prefix_of("false", 1) => Some(false),
        b'n' if prefix_of("no", 1) => Some(false),
        b'o' if t == "on" => Some(true),
        b'o' if prefix_of("off", 2) => Some(false),
        b'1' if n == 1 => Some(true),
        b'0' if n == 1 => Some(false),
        _ => None,
    }
}

fn parse_float8(s: &str) -> Result<f64> {
    let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
    let lower = t.to_ascii_lowercase();
    let unsigned = lower.trim_start_matches(['+', '-']);
    if matches!(unsigned, "inf" | "infinity" | "nan") {
        // Rust accepts these spellings too, PostgreSQL's float8in as well.
        return t.parse().map_err(|_| invalid("double precision", s));
    }
    if !unsigned.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
        return Err(invalid("double precision", s));
    }
    let f: f64 = t.parse().map_err(|_| invalid("double precision", s))?;
    let mantissa = lower.split('e').next().unwrap_or("");
    let nonzero = mantissa.chars().any(|c| ('1'..='9').contains(&c));
    if f.is_infinite() || (f == 0.0 && nonzero) {
        return Err(SqlError::new(
            code::NUMERIC_VALUE_OUT_OF_RANGE,
            format!("\"{t}\" is out of range for type double precision"),
        ));
    }
    Ok(f)
}

/// Parses `YYYY-MM-DD[( |T)HH:MM[:SS[.fraction]]]`.
fn parse_timestamp(s: &str) -> Result<TimestampMicros> {
    let bad = || invalid("timestamp", s);
    let range = || {
        SqlError::new(
            code::DATETIME_FIELD_OVERFLOW,
            format!("date/time field value out of range: \"{s}\""),
        )
    };
    let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
    let (date, time) = match t.find([' ', 'T', 't']) {
        Some(i) => (&t[..i], Some(t[i + 1..].trim_start())),
        None => (t, None),
    };
    let num = |p: &str, max_len: usize| -> Result<i64> {
        if p.is_empty() || p.len() > max_len || !p.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        p.parse().map_err(|_| bad())
    };
    let mut dp = date.split('-');
    let (Some(ys), Some(ms), Some(ds), None) = (dp.next(), dp.next(), dp.next(), dp.next()) else {
        return Err(bad());
    };
    let y = num(ys, 6)?;
    let m = num(ms, 2)?;
    let d = num(ds, 2)?;
    if y < 1 || !(1..=12).contains(&m) || d < 1 || d > i64::from(days_in_month(y, m as u32)) {
        return Err(range());
    }
    let mut us = 0i64;
    if let Some(time) = time {
        let (hms, frac) = match time.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (time, None),
        };
        let mut tp = hms.split(':');
        let h = num(tp.next().ok_or_else(bad)?, 2)?;
        let mi = num(tp.next().ok_or_else(bad)?, 2)?;
        let se = match tp.next() {
            Some(p) => num(p, 2)?,
            None if frac.is_none() => 0,
            None => return Err(bad()),
        };
        if tp.next().is_some() {
            return Err(bad());
        }
        let mut f = 0i64;
        if let Some(fr) = frac {
            if fr.is_empty() || !fr.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad());
            }
            // Round to microseconds, half to even (rint).
            let padded = format!("{fr:0<7}");
            f = padded[..6].parse().map_err(|_| bad())?;
            let rest = &padded[6..];
            let first = rest.as_bytes()[0] - b'0';
            let tail_nonzero = rest[1..].bytes().any(|b| b != b'0');
            if first > 5 || (first == 5 && (tail_nonzero || f % 2 == 1)) {
                f += 1;
            }
        }
        let midnight = h == 24 && mi == 0 && se == 0 && f == 0;
        if !(h < 24 || midnight) || mi > 59 || se > 60 {
            return Err(range());
        }
        us = ((h * 60 + mi) * 60 + se) * 1_000_000 + f;
    }
    let days = days_from_civil(y, m as u32, d as u32) - EPOCH_2000_DAYS;
    days.checked_mul(USECS_PER_DAY)
        .and_then(|x| x.checked_add(us))
        .ok_or_else(range)
}

/// Converts text to a value of `ty` (the type's input function; also used
/// for string literals and casts from text).
pub fn parse_value(ty: DataType, s: &str) -> Result<Value> {
    Ok(match ty {
        DataType::Int4 => match parse_int(s) {
            None => return Err(invalid("integer", s)),
            Some(Some(v)) => Value::Int4(i32::try_from(v).map_err(|_| out_of_range("integer", s))?),
            Some(None) => return Err(out_of_range("integer", s)),
        },
        DataType::Int8 => match parse_int(s) {
            None => return Err(invalid("bigint", s)),
            Some(Some(v)) => Value::Int8(v),
            Some(None) => return Err(out_of_range("bigint", s)),
        },
        DataType::Bool => Value::Bool(parse_bool(s).ok_or_else(|| invalid("boolean", s))?),
        DataType::Float8 => Value::Float8(parse_float8(s)?),
        DataType::Timestamp => Value::Timestamp(parse_timestamp(s)?),
        DataType::Text | DataType::Varchar(_) => {
            if s.contains('\0') {
                return Err(SqlError::new(
                    code::CHARACTER_NOT_IN_REPERTOIRE,
                    "invalid byte sequence for encoding \"UTF8\": 0x00",
                ));
            }
            Value::Text(s.to_owned())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float8_output_matches_postgres() {
        let cases = [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1.0 / 3.0, "0.3333333333333333"),
            (100.0, "100"),
            (1e14, "100000000000000"),
            (1e15, "1e+15"),
            (1.5e15, "1.5e+15"),
            (123456789012345.0, "123456789012345"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.25e-7, "1.25e-07"),
            (1e100, "1e+100"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (f64::NAN, "NaN"),
        ];
        for (f, want) in cases {
            assert_eq!(format_float8(f), want, "{f:?}");
        }
    }

    #[test]
    fn timestamp_roundtrip() {
        let cases = [
            ("2000-01-01 00:00:00", 0),
            ("2000-01-02 00:00:01", USECS_PER_DAY + 1_000_000),
            ("1999-12-31 23:59:59.5", -500_000),
            ("2024-02-29 12:34:56.000123", 0),
            ("1970-01-01 00:00:00", -EPOCH_2000_DAYS * USECS_PER_DAY),
            ("0001-01-01 00:00:00", 0),
        ];
        for (s, us) in cases {
            let Value::Timestamp(t) = parse_value(DataType::Timestamp, s).unwrap() else {
                panic!()
            };
            if us != 0 || s.starts_with("2000-01-01") {
                assert_eq!(t, us, "{s}");
            }
            assert_eq!(format_timestamp(t), s);
        }
        let p = |s| parse_value(DataType::Timestamp, s);
        assert_eq!(p("2024-01-02").unwrap(), p("2024-01-02 00:00:00").unwrap());
        assert_eq!(
            p("2024-01-02T03:04").unwrap(),
            p("2024-01-02 03:04:00").unwrap()
        );
        assert_eq!(
            p("2000-01-01 00:00:00.0000005").unwrap(),
            Value::Timestamp(0),
            "half to even"
        );
        assert_eq!(
            p("2000-01-01 00:00:00.0000015").unwrap(),
            Value::Timestamp(2)
        );
        assert_eq!(p("2000-01-01 24:00:00").unwrap(), p("2000-01-02").unwrap());
        assert_eq!(
            p("2023-02-29").unwrap_err().code,
            code::DATETIME_FIELD_OVERFLOW
        );
        assert_eq!(
            p("2023-01-01 25:00").unwrap_err().code,
            code::DATETIME_FIELD_OVERFLOW
        );
        assert_eq!(
            p("yesterday-ish").unwrap_err().code,
            code::INVALID_TEXT_REPRESENTATION
        );
        assert_eq!(
            p("2023-1-1x").unwrap_err().code,
            code::INVALID_TEXT_REPRESENTATION
        );
    }

    #[test]
    fn integer_input() {
        let i4 = |s| parse_value(DataType::Int4, s);
        assert_eq!(i4(" 42 ").unwrap(), Value::Int4(42));
        assert_eq!(i4("-2147483648").unwrap(), Value::Int4(i32::MIN));
        assert_eq!(i4("+1_000").unwrap(), Value::Int4(1000));
        let e = i4("2147483648").unwrap_err();
        assert_eq!(e.code, code::NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(
            e.message,
            "value \"2147483648\" is out of range for type integer"
        );
        let e = i4("12a").unwrap_err();
        assert_eq!(e.message, "invalid input syntax for type integer: \"12a\"");
        for bad in ["", "-", "1__0", "_1", "1_", "1.0"] {
            assert!(i4(bad).is_err(), "{bad}");
        }
        assert_eq!(
            parse_value(DataType::Int8, "-9223372036854775808").unwrap(),
            Value::Int8(i64::MIN)
        );
        assert!(parse_value(DataType::Int8, "9223372036854775808").is_err());
    }

    #[test]
    fn bool_and_float_input() {
        let b = |s| parse_value(DataType::Bool, s);
        for t in ["t", "TRUE", "tr", "yes", "y", "on", "1", " true "] {
            assert_eq!(b(t).unwrap(), Value::Bool(true), "{t}");
        }
        for f in ["f", "false", "no", "n", "off", "of", "0"] {
            assert_eq!(b(f).unwrap(), Value::Bool(false), "{f}");
        }
        for bad in ["o", "truex", "2", ""] {
            assert!(b(bad).is_err(), "{bad}");
        }
        let f = |s| parse_value(DataType::Float8, s);
        assert_eq!(f(" 1.5 ").unwrap(), Value::Float8(1.5));
        assert_eq!(f("-Infinity").unwrap(), Value::Float8(f64::NEG_INFINITY));
        assert_eq!(f(".5e1").unwrap(), Value::Float8(5.0));
        assert!(matches!(f("NaN").unwrap(), Value::Float8(x) if x.is_nan()));
        assert_eq!(
            f("1e400").unwrap_err().code,
            code::NUMERIC_VALUE_OUT_OF_RANGE
        );
        assert_eq!(
            f("1e-400").unwrap_err().code,
            code::NUMERIC_VALUE_OUT_OF_RANGE
        );
        assert_eq!(f("0e-400").unwrap(), Value::Float8(0.0));
        assert!(f("abc").is_err());
    }

    #[test]
    fn value_output() {
        assert_eq!(format_value(&Value::Null), None);
        assert_eq!(format_value(&Value::Bool(true)).unwrap(), "t");
        assert_eq!(format_value(&Value::Int8(-5)).unwrap(), "-5");
        assert_eq!(format_value(&Value::Text("x y".into())).unwrap(), "x y");
    }
}
