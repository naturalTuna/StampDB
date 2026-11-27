//! Heap tuple format.
//!
//! ```text
//! off size field
//!   0    8 xmin      inserting transaction
//!   8    8 xmax      deleting/updating transaction (0 = none)
//!  16    4 cmin      inserting command
//!  20    4 cmax      deleting command
//!  24    6 ctid      block u32 + slot u16: self, or the newer version
//!  30    2 infomask  hint bits and flags
//!  32    2 natts     number of stored attributes
//!  34    1 hoff      offset of attribute data
//!  35    - null bitmap, ceil(natts/8) bytes, only if HAS_NULL (1 = not null)
//! hoff   - attribute data: non-null values packed with no padding
//! ```
//!
//! Compared to PostgreSQL's 23-byte header, xids are 64-bit (+8) and
//! cmin/cmax are separate fields (+4) instead of a combo command id.
//! Attributes beyond `natts` (columns added later) read as NULL, as in PG.

use crate::storage::page::BlockNumber;
use crate::storage::slotted::{MAX_ITEM_SIZE, SlotId};
use crate::txn::{CommandId, Xid};
use crate::types::{DataType, TypeError, Value};

/// Size of the fixed part of the tuple header.
pub const TUPLE_HEADER_SIZE: usize = 35;

/// Maximum number of attributes (same as PostgreSQL's
/// MaxHeapAttributeNumber); keeps `hoff` within a byte.
pub const MAX_ATTRS: usize = 1600;

/// Largest tuple that fits on a heap page (no TOAST yet).
pub const MAX_TUPLE_SIZE: usize = MAX_ITEM_SIZE;

const _: () = assert!(TUPLE_HEADER_SIZE + MAX_ATTRS.div_ceil(8) <= u8::MAX as usize);

/// `infomask` bits.
pub mod infomask {
    /// Hint: xmin is known committed.
    pub const XMIN_COMMITTED: u16 = 0x0001;
    /// Hint: xmin is known aborted.
    pub const XMIN_INVALID: u16 = 0x0002;
    /// Hint: xmax is known committed.
    pub const XMAX_COMMITTED: u16 = 0x0004;
    /// Hint: xmax is invalid or known aborted.
    pub const XMAX_INVALID: u16 = 0x0008;
    /// The tuple has a null bitmap.
    pub const HAS_NULL: u16 = 0x0010;

    /// All hint bits (optional information; may be set opportunistically).
    pub const HINT_MASK: u16 = XMIN_COMMITTED | XMIN_INVALID | XMAX_COMMITTED | XMAX_INVALID;
}

mod off {
    pub const XMIN: usize = 0;
    pub const XMAX: usize = 8;
    pub const CMIN: usize = 16;
    pub const CMAX: usize = 20;
    pub const CTID: usize = 24;
    pub const INFOMASK: usize = 30;
    pub const NATTS: usize = 32;
    pub const HOFF: usize = 34;
}

/// Errors from tuple formation and decoding.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TupleError {
    #[error(transparent)]
    Type(#[from] TypeError),

    #[error("expected {expected} values, got {got}")]
    AttributeCountMismatch { expected: usize, got: usize },

    #[error("tables can have at most {MAX_ATTRS} columns")]
    TooManyAttributes,

    #[error("row is too big: size {len}, maximum size {max}")]
    TooLarge { len: usize, max: usize },

    #[error("corrupt tuple: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, TupleError>;

/// Tuple identifier: physical location (block, slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tid {
    pub blk: BlockNumber,
    pub slot: SlotId,
}

/// Attribute types of a row, in column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleDesc {
    pub attrs: Vec<DataType>,
}

impl TupleDesc {
    pub fn new(attrs: Vec<DataType>) -> Result<Self> {
        if attrs.len() > MAX_ATTRS {
            return Err(TupleError::TooManyAttributes);
        }
        Ok(Self { attrs })
    }
}

/// Decoded fixed header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleHeader {
    pub xmin: Xid,
    pub xmax: Xid,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub ctid: Tid,
    pub infomask: u16,
    pub natts: u16,
    pub hoff: u8,
}

impl TupleHeader {
    /// Decodes and sanity-checks the header of `tuple`.
    pub fn decode(tuple: &[u8]) -> Result<Self> {
        if tuple.len() < TUPLE_HEADER_SIZE {
            return Err(TupleError::Corrupt("shorter than header"));
        }
        let h = Self {
            xmin: Xid(u64_at(tuple, off::XMIN)),
            xmax: Xid(u64_at(tuple, off::XMAX)),
            cmin: CommandId(u32_at(tuple, off::CMIN)),
            cmax: CommandId(u32_at(tuple, off::CMAX)),
            ctid: Tid {
                blk: BlockNumber(u32_at(tuple, off::CTID)),
                slot: SlotId(u16_at(tuple, off::CTID + 4)),
            },
            infomask: u16_at(tuple, off::INFOMASK),
            natts: u16_at(tuple, off::NATTS),
            hoff: tuple[off::HOFF],
        };
        let bitmap = if h.has_null() {
            usize::from(h.natts).div_ceil(8)
        } else {
            0
        };
        if usize::from(h.natts) > MAX_ATTRS
            || usize::from(h.hoff) != TUPLE_HEADER_SIZE + bitmap
            || usize::from(h.hoff) > tuple.len()
        {
            return Err(TupleError::Corrupt("inconsistent natts/hoff"));
        }
        Ok(h)
    }

    pub fn has_null(&self) -> bool {
        self.infomask & infomask::HAS_NULL != 0
    }
}

/// Builds a tuple from `values` with the given inserter. `xmax` is invalid
/// (and hinted so); `ctid` is zero until the heap assigns a location.
pub fn form_tuple(
    desc: &TupleDesc,
    values: &[Value],
    xmin: Xid,
    cmin: CommandId,
) -> Result<Vec<u8>> {
    if values.len() != desc.attrs.len() {
        return Err(TupleError::AttributeCountMismatch {
            expected: desc.attrs.len(),
            got: values.len(),
        });
    }
    let natts = values.len();
    let has_null = values.iter().any(Value::is_null);
    let bitmap_len = if has_null { natts.div_ceil(8) } else { 0 };
    let hoff = TUPLE_HEADER_SIZE + bitmap_len;

    let mut data_len = 0;
    for (ty, v) in desc.attrs.iter().zip(values) {
        ty.validate(v)?;
        if !v.is_null() {
            data_len += ty.encoded_len(v);
        }
    }
    let len = hoff + data_len;
    if len > MAX_TUPLE_SIZE {
        return Err(TupleError::TooLarge {
            len,
            max: MAX_TUPLE_SIZE,
        });
    }

    let mut t = Vec::with_capacity(len);
    t.resize(hoff, 0);
    t[off::XMIN..off::XMIN + 8].copy_from_slice(&xmin.0.to_le_bytes());
    t[off::CMIN..off::CMIN + 4].copy_from_slice(&cmin.0.to_le_bytes());
    let mut mask = infomask::XMAX_INVALID;
    if has_null {
        mask |= infomask::HAS_NULL;
    }
    t[off::INFOMASK..off::INFOMASK + 2].copy_from_slice(&mask.to_le_bytes());
    t[off::NATTS..off::NATTS + 2].copy_from_slice(&(natts as u16).to_le_bytes());
    t[off::HOFF] = hoff as u8;
    if has_null {
        for (i, v) in values.iter().enumerate() {
            if !v.is_null() {
                t[TUPLE_HEADER_SIZE + i / 8] |= 1 << (i % 8);
            }
        }
    }
    for (ty, v) in desc.attrs.iter().zip(values) {
        if !v.is_null() {
            ty.encode(v, &mut t)?;
        }
    }
    debug_assert_eq!(t.len(), len);
    Ok(t)
}

/// Decodes all attribute values of `tuple` according to `desc`.
pub fn deform_tuple(desc: &TupleDesc, tuple: &[u8]) -> Result<Vec<Value>> {
    let h = TupleHeader::decode(tuple)?;
    let natts = usize::from(h.natts);
    if natts > desc.attrs.len() {
        return Err(TupleError::Corrupt("more attributes than descriptor"));
    }
    let mut pos = usize::from(h.hoff);
    let mut values = Vec::with_capacity(desc.attrs.len());
    for (i, ty) in desc.attrs.iter().enumerate() {
        let present =
            i < natts && (!h.has_null() || tuple[TUPLE_HEADER_SIZE + i / 8] & (1 << (i % 8)) != 0);
        if !present {
            values.push(Value::Null);
            continue;
        }
        let (v, used) = ty.decode(&tuple[pos..])?;
        values.push(v);
        pos += used;
    }
    if pos != tuple.len() {
        return Err(TupleError::Corrupt("trailing bytes after attributes"));
    }
    Ok(values)
}

/// Records a deleter/updater. Clears the xmax hint bits, since they
/// described the previous (invalid) xmax.
pub fn set_xmax(tuple: &mut [u8], xmax: Xid, cmax: CommandId) -> Result<()> {
    check_header(tuple)?;
    tuple[off::XMAX..off::XMAX + 8].copy_from_slice(&xmax.0.to_le_bytes());
    tuple[off::CMAX..off::CMAX + 4].copy_from_slice(&cmax.0.to_le_bytes());
    let mask = u16_at(tuple, off::INFOMASK) & !(infomask::XMAX_COMMITTED | infomask::XMAX_INVALID);
    tuple[off::INFOMASK..off::INFOMASK + 2].copy_from_slice(&mask.to_le_bytes());
    Ok(())
}

/// Sets `ctid` (self after insert, the new version after update).
pub fn set_ctid(tuple: &mut [u8], tid: Tid) -> Result<()> {
    check_header(tuple)?;
    tuple[off::CTID..off::CTID + 4].copy_from_slice(&tid.blk.0.to_le_bytes());
    tuple[off::CTID + 4..off::CTID + 6].copy_from_slice(&tid.slot.0.to_le_bytes());
    Ok(())
}

/// ORs hint bits into `infomask`. Only bits in [`infomask::HINT_MASK`] are
/// accepted. Callers must hold the page's exclusive content lock.
pub fn set_hint_bits(tuple: &mut [u8], bits: u16) -> Result<()> {
    check_header(tuple)?;
    if bits & !infomask::HINT_MASK != 0 {
        return Err(TupleError::Corrupt("not a hint bit"));
    }
    let mask = u16_at(tuple, off::INFOMASK) | bits;
    tuple[off::INFOMASK..off::INFOMASK + 2].copy_from_slice(&mask.to_le_bytes());
    Ok(())
}

fn check_header(tuple: &[u8]) -> Result<()> {
    if tuple.len() < TUPLE_HEADER_SIZE {
        return Err(TupleError::Corrupt("shorter than header"));
    }
    Ok(())
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn desc(attrs: &[DataType]) -> TupleDesc {
        TupleDesc::new(attrs.to_vec()).unwrap()
    }

    #[test]
    fn layout_without_nulls() {
        let d = desc(&[DataType::Int4, DataType::Text]);
        let t = form_tuple(
            &d,
            &[Value::Int4(7), Value::Text("hi".into())],
            Xid(42),
            CommandId(3),
        )
        .unwrap();
        assert_eq!(t.len(), TUPLE_HEADER_SIZE + 4 + 3);
        let h = TupleHeader::decode(&t).unwrap();
        assert_eq!(h.xmin, Xid(42));
        assert_eq!(h.xmax, Xid::INVALID);
        assert_eq!(h.cmin, CommandId(3));
        assert_eq!(h.natts, 2);
        assert_eq!(usize::from(h.hoff), TUPLE_HEADER_SIZE);
        assert_eq!(h.infomask, infomask::XMAX_INVALID);
        assert_eq!(&t[35..39], &7i32.to_le_bytes());
        assert_eq!(&t[39..], &[2, b'h', b'i']);
    }

    #[test]
    fn null_bitmap() {
        let d = desc(&[DataType::Int4; 10]);
        let mut vals = vec![Value::Int4(1); 10];
        vals[1] = Value::Null;
        vals[9] = Value::Null;
        let t = form_tuple(&d, &vals, Xid(1), CommandId(0)).unwrap();
        let h = TupleHeader::decode(&t).unwrap();
        assert!(h.has_null());
        assert_eq!(usize::from(h.hoff), TUPLE_HEADER_SIZE + 2);
        assert_eq!(t[35], 0b1111_1101);
        assert_eq!(t[36], 0b0000_0001);
        assert_eq!(t.len(), TUPLE_HEADER_SIZE + 2 + 8 * 4);
        assert_eq!(deform_tuple(&d, &t).unwrap(), vals);
    }

    #[test]
    fn missing_trailing_attributes_read_as_null() {
        let old = desc(&[DataType::Int8]);
        let t = form_tuple(&old, &[Value::Int8(5)], Xid(1), CommandId(0)).unwrap();
        let new = desc(&[DataType::Int8, DataType::Bool]);
        assert_eq!(
            deform_tuple(&new, &t).unwrap(),
            vec![Value::Int8(5), Value::Null]
        );
        assert!(deform_tuple(&desc(&[]), &t).is_err());
    }

    #[test]
    fn rejects_bad_input() {
        let d = desc(&[DataType::Int4]);
        assert_eq!(
            form_tuple(&d, &[], Xid(1), CommandId(0)),
            Err(TupleError::AttributeCountMismatch {
                expected: 1,
                got: 0
            })
        );
        assert!(matches!(
            form_tuple(&d, &[Value::Bool(true)], Xid(1), CommandId(0)),
            Err(TupleError::Type(TypeError::TypeMismatch { .. }))
        ));
        let big = desc(&[DataType::Text]);
        assert!(matches!(
            form_tuple(
                &big,
                &[Value::Text("x".repeat(MAX_TUPLE_SIZE))],
                Xid(1),
                CommandId(0)
            ),
            Err(TupleError::TooLarge { .. })
        ));
        assert_eq!(
            TupleDesc::new(vec![DataType::Bool; MAX_ATTRS + 1]),
            Err(TupleError::TooManyAttributes)
        );
    }

    #[test]
    fn largest_tuple_fits() {
        let d = desc(&[DataType::Text]);
        let n = MAX_TUPLE_SIZE - TUPLE_HEADER_SIZE - 4;
        let t = form_tuple(&d, &[Value::Text("y".repeat(n))], Xid(1), CommandId(0)).unwrap();
        assert_eq!(t.len(), MAX_TUPLE_SIZE);
    }

    #[test]
    fn header_updates() {
        let d = desc(&[DataType::Int4]);
        let mut t = form_tuple(&d, &[Value::Int4(9)], Xid(10), CommandId(1)).unwrap();
        set_hint_bits(&mut t, infomask::XMIN_COMMITTED).unwrap();
        set_xmax(&mut t, Xid(11), CommandId(2)).unwrap();
        let tid = Tid {
            blk: BlockNumber(5),
            slot: SlotId(6),
        };
        set_ctid(&mut t, tid).unwrap();
        let h = TupleHeader::decode(&t).unwrap();
        assert_eq!((h.xmax, h.cmax, h.ctid), (Xid(11), CommandId(2), tid));
        assert_eq!(h.infomask, infomask::XMIN_COMMITTED, "xmax hints cleared");
        assert!(set_hint_bits(&mut t, infomask::HAS_NULL).is_err());
        assert_eq!(deform_tuple(&d, &t).unwrap(), vec![Value::Int4(9)]);
    }

    #[test]
    fn corrupt_tuples_are_errors() {
        let d = desc(&[DataType::Int4, DataType::Text]);
        let t = form_tuple(
            &d,
            &[Value::Int4(1), Value::Text("abc".into())],
            Xid(1),
            CommandId(0),
        )
        .unwrap();
        assert!(deform_tuple(&d, &t[..10]).is_err());
        assert!(deform_tuple(&d, &t[..t.len() - 1]).is_err());
        let mut extra = t.clone();
        extra.push(0);
        assert!(deform_tuple(&d, &extra).is_err());
        let mut bad_hoff = t.clone();
        bad_hoff[off::HOFF] = 40;
        assert!(deform_tuple(&d, &bad_hoff).is_err());
    }

    fn column() -> impl Strategy<Value = (DataType, Value)> {
        let v = prop_oneof![
            any::<i32>().prop_map(|x| (DataType::Int4, Value::Int4(x))),
            any::<i64>().prop_map(|x| (DataType::Int8, Value::Int8(x))),
            any::<bool>().prop_map(|x| (DataType::Bool, Value::Bool(x))),
            any::<i64>().prop_map(|x| (DataType::Float8, Value::Float8(x as f64))),
            any::<i64>().prop_map(|x| (DataType::Timestamp, Value::Timestamp(x))),
            "[^\u{0}]{0,200}".prop_map(|s| (DataType::Text, Value::Text(s))),
            "[^\u{0}]{0,8}".prop_map(|s| (DataType::Varchar(Some(8)), Value::Text(s))),
        ];
        (v, any::<bool>()).prop_map(|((t, v), null)| (t, if null { Value::Null } else { v }))
    }

    proptest! {
        #[test]
        fn roundtrip(cols in prop::collection::vec(column(), 0..40), xmin in any::<u64>(), cmin in any::<u32>()) {
            let d = TupleDesc::new(cols.iter().map(|c| c.0).collect()).unwrap();
            let vals: Vec<Value> = cols.into_iter().map(|c| c.1).collect();
            let t = form_tuple(&d, &vals, Xid(xmin), CommandId(cmin)).unwrap();
            let h = TupleHeader::decode(&t).unwrap();
            prop_assert_eq!(h.xmin, Xid(xmin));
            prop_assert_eq!(h.cmin, CommandId(cmin));
            prop_assert_eq!(h.has_null(), vals.iter().any(Value::is_null));
            prop_assert_eq!(deform_tuple(&d, &t).unwrap(), vals);
        }
    }
}
