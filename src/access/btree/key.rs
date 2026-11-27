//! Memcomparable index key encoding: comparing encoded keys with `memcmp`
//! gives the same order as comparing the typed values.
//!
//! Per attribute, a marker byte (`0x01` value, `0x02` NULL: NULLS LAST)
//! followed by the value:
//!
//! | type                 | encoding                                          |
//! |----------------------|---------------------------------------------------|
//! | INT4/INT8/TIMESTAMP  | big-endian with the sign bit flipped              |
//! | BOOL                 | `0x00` / `0x01`                                   |
//! | FLOAT8               | -0 → 0 and NaN → one NaN, then: positive → flip   |
//! |                      | sign bit, negative → flip all bits (NaN greatest) |
//! | TEXT/VARCHAR         | bytes + `0x00` (text never contains NUL; C order) |
//!
//! Float semantics match PostgreSQL: -0 equals 0, NaN equals NaN and sorts
//! above every other value. Decoding returns the normalized value.
//!
//! Index entries append the heap TID (block u32 BE, slot u16 BE) so every
//! entry is unique and duplicates are ordered by TID.

use crate::access::tuple::Tid;
use crate::storage::page::BlockNumber;
use crate::storage::slotted::SlotId;
use crate::types::{DataType, TypeError, Value};

/// Marker for a non-null attribute.
const NOT_NULL: u8 = 0x01;
/// Marker for NULL; larger than `NOT_NULL`, so NULLS LAST.
const NULL: u8 = 0x02;
/// Terminator of a text value.
const TEXT_END: u8 = 0x00;

/// Encoded size of a TID suffix.
pub const TID_LEN: usize = 6;

const SIGN64: u64 = 1 << 63;
const SIGN32: u32 = 1 << 31;
const CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;

/// Errors from key encoding and decoding.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum KeyError {
    #[error(transparent)]
    Type(#[from] TypeError),

    #[error("expected {expected} key values, got {got}")]
    AttributeCountMismatch { expected: usize, got: usize },

    #[error("truncated key")]
    Truncated,

    #[error("corrupt key: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, KeyError>;

/// Appends the encoding of `values` (one per attribute in `attrs`).
pub fn encode_key(attrs: &[DataType], values: &[Value], out: &mut Vec<u8>) -> Result<()> {
    if attrs.len() != values.len() {
        return Err(KeyError::AttributeCountMismatch {
            expected: attrs.len(),
            got: values.len(),
        });
    }
    for (ty, v) in attrs.iter().zip(values) {
        ty.validate(v)?;
        match v {
            Value::Null => {
                out.push(NULL);
                continue;
            }
            _ => out.push(NOT_NULL),
        }
        match v {
            Value::Null => {}
            Value::Int4(x) => out.extend_from_slice(&((*x as u32) ^ SIGN32).to_be_bytes()),
            Value::Int8(x) | Value::Timestamp(x) => {
                out.extend_from_slice(&((*x as u64) ^ SIGN64).to_be_bytes());
            }
            Value::Bool(b) => out.push(u8::from(*b)),
            Value::Float8(x) => out.extend_from_slice(&float_key(*x).to_be_bytes()),
            Value::Text(s) => {
                out.extend_from_slice(s.as_bytes());
                out.push(TEXT_END);
            }
        }
    }
    Ok(())
}

/// Decodes a key from the start of `buf`.
/// Returns the values and the number of bytes consumed.
pub fn decode_key(attrs: &[DataType], buf: &[u8]) -> Result<(Vec<Value>, usize)> {
    let mut pos = 0;
    let mut values = Vec::with_capacity(attrs.len());
    for ty in attrs {
        let marker = *buf.get(pos).ok_or(KeyError::Truncated)?;
        pos += 1;
        match marker {
            NULL => {
                values.push(Value::Null);
                continue;
            }
            NOT_NULL => {}
            _ => return Err(KeyError::Corrupt("null marker")),
        }
        let rest = &buf[pos..];
        let (v, used) = match ty {
            DataType::Int4 => {
                let b = rest.get(..4).ok_or(KeyError::Truncated)?;
                let u = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) ^ SIGN32;
                (Value::Int4(u as i32), 4)
            }
            DataType::Int8 => (Value::Int8((be_u64(rest)? ^ SIGN64) as i64), 8),
            DataType::Timestamp => (Value::Timestamp((be_u64(rest)? ^ SIGN64) as i64), 8),
            DataType::Bool => match rest.first() {
                Some(0) => (Value::Bool(false), 1),
                Some(1) => (Value::Bool(true), 1),
                Some(_) => return Err(KeyError::Corrupt("bool byte")),
                None => return Err(KeyError::Truncated),
            },
            DataType::Float8 => (Value::Float8(float_from_key(be_u64(rest)?)), 8),
            DataType::Text | DataType::Varchar(_) => {
                let end = rest
                    .iter()
                    .position(|&b| b == TEXT_END)
                    .ok_or(KeyError::Truncated)?;
                let s = std::str::from_utf8(&rest[..end]).map_err(|_| TypeError::InvalidUtf8)?;
                (Value::Text(s.to_owned()), end + 1)
            }
        };
        values.push(v);
        pos += used;
    }
    Ok((values, pos))
}

/// Appends a TID suffix (sorts by block, then slot).
pub fn encode_tid(tid: Tid, out: &mut Vec<u8>) {
    out.extend_from_slice(&tid.blk.0.to_be_bytes());
    out.extend_from_slice(&tid.slot.0.to_be_bytes());
}

/// Decodes a TID suffix of exactly [`TID_LEN`] bytes.
pub fn decode_tid(buf: &[u8]) -> Result<Tid> {
    if buf.len() != TID_LEN {
        return Err(KeyError::Corrupt("tid length"));
    }
    Ok(Tid {
        blk: BlockNumber(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])),
        slot: SlotId(u16::from_be_bytes([buf[4], buf[5]])),
    })
}

fn be_u64(b: &[u8]) -> Result<u64> {
    let b = b.get(..8).ok_or(KeyError::Truncated)?;
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    Ok(u64::from_be_bytes(a))
}

fn float_key(x: f64) -> u64 {
    let bits = if x.is_nan() {
        CANONICAL_NAN
    } else if x == 0.0 {
        0 // -0 and +0 both become +0
    } else {
        x.to_bits()
    };
    if bits & SIGN64 == 0 {
        bits ^ SIGN64
    } else {
        !bits
    }
}

fn float_from_key(k: u64) -> f64 {
    let bits = if k & SIGN64 != 0 { k ^ SIGN64 } else { !k };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cmp::Ordering;

    fn enc(attrs: &[DataType], values: &[Value]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_key(attrs, values, &mut out).unwrap();
        out
    }

    /// Reference order with PostgreSQL semantics (NULLS LAST, NaN greatest,
    /// -0 == 0, text in byte order).
    fn cmp_value(a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (Value::Int4(x), Value::Int4(y)) => x.cmp(y),
            (Value::Int8(x), Value::Int8(y)) | (Value::Timestamp(x), Value::Timestamp(y)) => {
                x.cmp(y)
            }
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Float8(x), Value::Float8(y)) => match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
            },
            (Value::Text(x), Value::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
            _ => panic!("mixed types"),
        }
    }

    fn cmp_row(a: &[Value], b: &[Value]) -> Ordering {
        a.iter()
            .zip(b)
            .map(|(x, y)| cmp_value(x, y))
            .find(|o| o.is_ne())
            .unwrap_or(Ordering::Equal)
    }

    #[test]
    fn integer_and_sign_order() {
        let t = [DataType::Int4];
        let keys: Vec<_> = [i32::MIN, -1, 0, 1, i32::MAX]
            .iter()
            .map(|&x| enc(&t, &[Value::Int4(x)]))
            .collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn float_special_values() {
        let t = [DataType::Float8];
        let k = |x: f64| enc(&t, &[Value::Float8(x)]);
        assert_eq!(k(-0.0), k(0.0));
        assert_eq!(k(f64::NAN), k(-f64::NAN));
        let ordered = [
            f64::NEG_INFINITY,
            -1.5,
            -f64::MIN_POSITIVE,
            0.0,
            2.0,
            f64::INFINITY,
            f64::NAN,
        ];
        let keys: Vec<_> = ordered.iter().map(|&x| k(x)).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        let (v, _) = decode_key(&t, &k(-0.0)).unwrap();
        assert_eq!(v[0], Value::Float8(0.0));
        assert!(
            matches!(decode_key(&t, &k(f64::NAN)).unwrap().0[0], Value::Float8(x) if x.is_nan())
        );
    }

    #[test]
    fn nulls_last_and_text_prefix() {
        let t = [DataType::Text];
        let a = enc(&t, &[Value::Text("a".into())]);
        let ab = enc(&t, &[Value::Text("ab".into())]);
        let empty = enc(&t, &[Value::Text(String::new())]);
        let null = enc(&t, &[Value::Null]);
        assert!(empty < a && a < ab && ab < null);
        // A text attribute ending does not bleed into the next attribute.
        let t2 = [DataType::Text, DataType::Int4];
        assert!(
            enc(&t2, &[Value::Text("a".into()), Value::Int4(i32::MAX)])
                < enc(&t2, &[Value::Text("ab".into()), Value::Int4(i32::MIN)])
        );
    }

    #[test]
    fn tid_suffix_orders_by_block_then_slot() {
        let tid = |b, s| Tid {
            blk: BlockNumber(b),
            slot: SlotId(s),
        };
        let e = |t| {
            let mut v = Vec::new();
            encode_tid(t, &mut v);
            v
        };
        assert!(e(tid(1, 65535)) < e(tid(2, 0)));
        assert!(e(tid(2, 0)) < e(tid(2, 1)));
        assert_eq!(decode_tid(&e(tid(7, 9))).unwrap(), tid(7, 9));
        assert!(decode_tid(&[0; 5]).is_err());
    }

    #[test]
    fn rejects_bad_input() {
        assert!(matches!(
            encode_key(&[DataType::Int4], &[], &mut Vec::new()),
            Err(KeyError::AttributeCountMismatch { .. })
        ));
        assert!(matches!(
            encode_key(
                &[DataType::Text],
                &[Value::Text("a\0".into())],
                &mut Vec::new()
            ),
            Err(KeyError::Type(TypeError::NulInText))
        ));
        assert_eq!(
            decode_key(&[DataType::Int8], &[1, 0, 0]),
            Err(KeyError::Truncated)
        );
        assert_eq!(
            decode_key(&[DataType::Text], &[1, b'a']),
            Err(KeyError::Truncated)
        );
        assert!(matches!(
            decode_key(&[DataType::Int4], &[9]),
            Err(KeyError::Corrupt(_))
        ));
    }

    fn scalar(ty: DataType) -> BoxedStrategy<Value> {
        let v: BoxedStrategy<Value> = match ty {
            DataType::Int4 => any::<i32>().prop_map(Value::Int4).boxed(),
            DataType::Int8 => any::<i64>().prop_map(Value::Int8).boxed(),
            DataType::Timestamp => any::<i64>().prop_map(Value::Timestamp).boxed(),
            DataType::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
            DataType::Float8 => prop_oneof![
                any::<f64>(),
                Just(f64::NAN),
                Just(-0.0),
                Just(0.0),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
            ]
            .prop_map(Value::Float8)
            .boxed(),
            DataType::Text | DataType::Varchar(_) => "[^\u{0}]{0,6}".prop_map(Value::Text).boxed(),
        };
        prop_oneof![9 => v, 1 => Just(Value::Null)].boxed()
    }

    fn schema() -> impl Strategy<Value = Vec<DataType>> {
        prop::collection::vec(
            prop_oneof![
                Just(DataType::Int4),
                Just(DataType::Int8),
                Just(DataType::Timestamp),
                Just(DataType::Bool),
                Just(DataType::Float8),
                Just(DataType::Text),
            ],
            1..4,
        )
    }

    fn rows() -> impl Strategy<Value = (Vec<DataType>, Vec<Value>, Vec<Value>)> {
        schema().prop_flat_map(|s| {
            let row = s.iter().map(|&t| scalar(t)).collect::<Vec<_>>();
            (Just(s), row.clone(), row)
        })
    }

    fn normalized(v: &Value) -> Value {
        match v {
            Value::Float8(x) if *x == 0.0 => Value::Float8(0.0),
            other => other.clone(),
        }
    }

    fn same(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Float8(x), Value::Float8(y)) => {
                (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits()
            }
            _ => a == b,
        }
    }

    proptest! {
        #[test]
        fn byte_order_matches_value_order((s, a, b) in rows()) {
            let (ka, kb) = (enc(&s, &a), enc(&s, &b));
            prop_assert_eq!(ka.cmp(&kb), cmp_row(&a, &b), "{:?} vs {:?}", a, b);
        }

        #[test]
        fn decode_roundtrip((s, a, _b) in rows(), tail in prop::collection::vec(any::<u8>(), 0..8)) {
            let mut k = enc(&s, &a);
            let len = k.len();
            k.extend_from_slice(&tail);
            let (vals, used) = decode_key(&s, &k).unwrap();
            prop_assert_eq!(used, len);
            for (got, want) in vals.iter().zip(&a) {
                prop_assert!(same(got, &normalized(want)), "{:?} vs {:?}", got, want);
            }
        }
    }
}
