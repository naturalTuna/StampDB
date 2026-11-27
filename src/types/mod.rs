//! SQL data types, values, and their on-disk encoding.
//!
//! Fixed-width types are little-endian. Variable-length types use the
//! [`varlena`] framing. Values are packed with no alignment padding and are
//! decoded byte-wise.

pub mod varlena;

use std::fmt;

/// Errors from value validation and (de)serialization.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TypeError {
    #[error("value of kind {found} does not match type {expected}")]
    TypeMismatch {
        expected: DataType,
        found: &'static str,
    },

    #[error("value too long for type character varying({max})")]
    StringTooLong { max: u32 },

    #[error("invalid byte sequence for encoding \"UTF8\": 0x00")]
    NulInText,

    #[error("value of {len} bytes exceeds the varlena limit")]
    ValueTooLarge { len: usize },

    #[error("invalid byte sequence for encoding \"UTF8\"")]
    InvalidUtf8,

    #[error("truncated value")]
    Truncated,

    #[error("external (TOAST) values are not supported yet")]
    ExternalNotSupported,

    #[error("corrupt value: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, TypeError>;

/// Microseconds since 2000-01-01 00:00:00 (PostgreSQL's timestamp epoch).
pub type TimestampMicros = i64;

/// A column data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Int4,
    Int8,
    Bool,
    Float8,
    Timestamp,
    Text,
    /// `VARCHAR(n)` limits the length in characters; `None` is unlimited.
    Varchar(Option<u32>),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int4 => f.write_str("integer"),
            DataType::Int8 => f.write_str("bigint"),
            DataType::Bool => f.write_str("boolean"),
            DataType::Float8 => f.write_str("double precision"),
            DataType::Timestamp => f.write_str("timestamp without time zone"),
            DataType::Text => f.write_str("text"),
            DataType::Varchar(None) => f.write_str("character varying"),
            DataType::Varchar(Some(n)) => write!(f, "character varying({n})"),
        }
    }
}

/// A single datum. `Varchar` columns hold `Value::Text`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int4(i32),
    Int8(i64),
    Bool(bool),
    Float8(f64),
    Timestamp(TimestampMicros),
    Text(String),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    fn kind(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Int4(_) => "int4",
            Value::Int8(_) => "int8",
            Value::Bool(_) => "bool",
            Value::Float8(_) => "float8",
            Value::Timestamp(_) => "timestamp",
            Value::Text(_) => "text",
        }
    }
}

impl DataType {
    /// PostgreSQL type OID (fixed to PG's values; sent in RowDescription).
    pub fn oid(&self) -> u32 {
        match self {
            DataType::Bool => 16,
            DataType::Int8 => 20,
            DataType::Int4 => 23,
            DataType::Text => 25,
            DataType::Float8 => 701,
            DataType::Varchar(_) => 1043,
            DataType::Timestamp => 1114,
        }
    }

    /// Encoded size of a fixed-width type, `None` for varlena types.
    pub fn fixed_len(&self) -> Option<usize> {
        match self {
            DataType::Bool => Some(1),
            DataType::Int4 => Some(4),
            DataType::Int8 | DataType::Float8 | DataType::Timestamp => Some(8),
            DataType::Text | DataType::Varchar(_) => None,
        }
    }

    /// Checks that `v` is storable in this type. `Null` is always accepted
    /// (NOT NULL is a constraint, checked elsewhere).
    pub fn validate(&self, v: &Value) -> Result<()> {
        match (self, v) {
            (_, Value::Null)
            | (DataType::Int4, Value::Int4(_))
            | (DataType::Int8, Value::Int8(_))
            | (DataType::Bool, Value::Bool(_))
            | (DataType::Float8, Value::Float8(_))
            | (DataType::Timestamp, Value::Timestamp(_)) => Ok(()),
            (DataType::Text | DataType::Varchar(_), Value::Text(s)) => {
                if s.contains('\0') {
                    return Err(TypeError::NulInText);
                }
                if let DataType::Varchar(Some(max)) = self
                    && s.chars().count() > *max as usize
                {
                    return Err(TypeError::StringTooLong { max: *max });
                }
                if s.len() > varlena::MAX_VARLENA_LEN {
                    return Err(TypeError::ValueTooLarge { len: s.len() });
                }
                Ok(())
            }
            _ => Err(TypeError::TypeMismatch {
                expected: *self,
                found: v.kind(),
            }),
        }
    }

    /// Encoded size of a non-null value that passed `validate`.
    pub fn encoded_len(&self, v: &Value) -> usize {
        match (self.fixed_len(), v) {
            (Some(n), _) => n,
            (None, Value::Text(s)) => varlena::encoded_len(s.len()),
            (None, _) => 0,
        }
    }

    /// Validates and appends a non-null value.
    pub fn encode(&self, v: &Value, out: &mut Vec<u8>) -> Result<()> {
        self.validate(v)?;
        match v {
            Value::Null => {
                return Err(TypeError::TypeMismatch {
                    expected: *self,
                    found: "null",
                });
            }
            Value::Int4(x) => out.extend_from_slice(&x.to_le_bytes()),
            Value::Int8(x) | Value::Timestamp(x) => out.extend_from_slice(&x.to_le_bytes()),
            Value::Bool(b) => out.push(u8::from(*b)),
            Value::Float8(x) => out.extend_from_slice(&x.to_bits().to_le_bytes()),
            Value::Text(s) => varlena::encode(s.as_bytes(), out)?,
        }
        Ok(())
    }

    /// Decodes one non-null value at the start of `buf`.
    /// Returns the value and the number of bytes consumed.
    pub fn decode(&self, buf: &[u8]) -> Result<(Value, usize)> {
        if let Some(n) = self.fixed_len() {
            let b = buf.get(..n).ok_or(TypeError::Truncated)?;
            let v = match self {
                DataType::Bool => match b[0] {
                    0 => Value::Bool(false),
                    1 => Value::Bool(true),
                    _ => return Err(TypeError::Corrupt("bool byte")),
                },
                DataType::Int4 => Value::Int4(i32::from_le_bytes([b[0], b[1], b[2], b[3]])),
                DataType::Int8 => Value::Int8(i64::from_le_bytes(arr8(b))),
                DataType::Float8 => Value::Float8(f64::from_bits(u64::from_le_bytes(arr8(b)))),
                DataType::Timestamp => Value::Timestamp(i64::from_le_bytes(arr8(b))),
                DataType::Text | DataType::Varchar(_) => {
                    return Err(TypeError::Corrupt("varlena type with fixed length"));
                }
            };
            return Ok((v, n));
        }
        let (data, used) = varlena::decode(buf)?;
        let s = std::str::from_utf8(data).map_err(|_| TypeError::InvalidUtf8)?;
        Ok((Value::Text(s.to_owned()), used))
    }
}

fn arr8(b: &[u8]) -> [u8; 8] {
    [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn oids_match_postgres() {
        assert_eq!(DataType::Bool.oid(), 16);
        assert_eq!(DataType::Int8.oid(), 20);
        assert_eq!(DataType::Int4.oid(), 23);
        assert_eq!(DataType::Text.oid(), 25);
        assert_eq!(DataType::Float8.oid(), 701);
        assert_eq!(DataType::Varchar(Some(3)).oid(), 1043);
        assert_eq!(DataType::Timestamp.oid(), 1114);
    }

    #[test]
    fn varchar_counts_characters() {
        let t = DataType::Varchar(Some(3));
        t.validate(&Value::Text("한글자".into())).unwrap(); // 3 chars, 9 bytes
        assert_eq!(
            t.validate(&Value::Text("abcd".into())),
            Err(TypeError::StringTooLong { max: 3 })
        );
        DataType::Varchar(None)
            .validate(&Value::Text("x".repeat(10_000)))
            .unwrap();
    }

    #[test]
    fn rejects_nul_and_mismatch() {
        assert_eq!(
            DataType::Text.validate(&Value::Text("a\0b".into())),
            Err(TypeError::NulInText)
        );
        assert!(matches!(
            DataType::Int4.validate(&Value::Int8(1)),
            Err(TypeError::TypeMismatch { .. })
        ));
        DataType::Int4.validate(&Value::Null).unwrap();
        assert!(
            DataType::Int4
                .encode(&Value::Null, &mut Vec::new())
                .is_err()
        );
    }

    #[test]
    fn fixed_encodings() {
        let mut out = Vec::new();
        DataType::Int4.encode(&Value::Int4(-2), &mut out).unwrap();
        DataType::Bool.encode(&Value::Bool(true), &mut out).unwrap();
        DataType::Timestamp
            .encode(&Value::Timestamp(1), &mut out)
            .unwrap();
        assert_eq!(out, [0xFE, 0xFF, 0xFF, 0xFF, 1, 1, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn decode_errors() {
        assert_eq!(DataType::Int8.decode(&[1, 2, 3]), Err(TypeError::Truncated));
        assert!(matches!(
            DataType::Bool.decode(&[2]),
            Err(TypeError::Corrupt(_))
        ));
        assert_eq!(
            DataType::Text.decode(&[2, 0xC3, 0x28]),
            Err(TypeError::InvalidUtf8)
        );
    }

    fn typed_value() -> impl Strategy<Value = (DataType, Value)> {
        prop_oneof![
            any::<i32>().prop_map(|x| (DataType::Int4, Value::Int4(x))),
            any::<i64>().prop_map(|x| (DataType::Int8, Value::Int8(x))),
            any::<bool>().prop_map(|x| (DataType::Bool, Value::Bool(x))),
            any::<f64>().prop_map(|x| (DataType::Float8, Value::Float8(x))),
            any::<i64>().prop_map(|x| (DataType::Timestamp, Value::Timestamp(x))),
            "[^\u{0}]{0,300}".prop_map(|s| (DataType::Text, Value::Text(s))),
            "[^\u{0}]{0,20}".prop_map(|s| (DataType::Varchar(Some(20)), Value::Text(s))),
        ]
    }

    fn same(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Float8(x), Value::Float8(y)) => x.to_bits() == y.to_bits(),
            _ => a == b,
        }
    }

    proptest! {
        #[test]
        fn sequence_roundtrip(vals in prop::collection::vec(typed_value(), 0..20)) {
            let mut out = Vec::new();
            for (t, v) in &vals {
                let before = out.len();
                t.encode(v, &mut out).unwrap();
                prop_assert_eq!(out.len() - before, t.encoded_len(v));
            }
            let mut pos = 0;
            for (t, v) in &vals {
                let (got, used) = t.decode(&out[pos..]).unwrap();
                prop_assert!(same(&got, v), "{:?} != {:?}", got, v);
                pos += used;
            }
            prop_assert_eq!(pos, out.len());
        }

        #[test]
        fn truncated_input_never_panics((t, v) in typed_value(), cut in 0usize..400) {
            let mut out = Vec::new();
            t.encode(&v, &mut out).unwrap();
            let cut = cut.min(out.len());
            let r = t.decode(&out[..cut]);
            if cut < out.len() {
                prop_assert!(r.is_err());
            }
        }
    }
}
