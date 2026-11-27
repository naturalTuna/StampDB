//! Page constants, identifiers, and the common page header.
//!
//! Header layout (24 bytes, little-endian, encoded by byte offset rather than
//! by transmuting a `#[repr(C)]` struct):
//!
//! ```text
//! off size field
//!   0    8 lsn       page LSN (last WAL record applied to this page)
//!   8    4 checksum  page checksum
//!  12    2 flags
//!  14    2 lower     end of the line pointer array
//!  16    2 upper     start of item data
//!  18    2 special   start of the special area
//!  20    1 version   page layout version
//!  21    1 kind      page kind (heap, btree, ...)
//!  22    2 reserved  must be zero
//! ```
//!
//! An all-zero page is a "new" page (e.g. file extended but never written)
//! and is not a valid header; callers check [`is_new_page`] first, as with
//! PostgreSQL's `PageIsNew`.

use super::aligned::IO_ALIGN;
use super::{Result, StorageError};

/// Page size in bytes. A compile-time constant so 4KB/16KB can be tried.
pub const PAGE_SIZE: usize = 8192;

/// Size of the common page header.
pub const PAGE_HEADER_SIZE: usize = 24;

/// Current page layout version.
pub const PAGE_LAYOUT_VERSION: u8 = 1;

/// Alignment of the special area start (and its size).
pub const SPECIAL_ALIGN: usize = 8;

// Direct I/O: a page must be a whole number of alignment units.
const _: () = assert!(PAGE_SIZE.is_multiple_of(IO_ALIGN));
// Line pointers store offsets/lengths in 15 bits.
const _: () = assert!(PAGE_SIZE <= 32 * 1024);
const _: () = assert!(PAGE_SIZE.is_power_of_two());

/// Log sequence number: byte position in the WAL stream.
///
/// Kept as a newtype so it can later grow into a global ordering scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    /// "No WAL record" marker.
    pub const INVALID: Lsn = Lsn(0);
}

/// Block number within a file (page index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockNumber(pub u32);

impl BlockNumber {
    /// Byte offset of this block in its file.
    pub fn byte_offset(self) -> u64 {
        u64::from(self.0) * PAGE_SIZE as u64
    }
}

/// What kind of structure a page belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageKind {
    Heap = 1,
    BTree = 2,
    /// Commit log page: transaction status bits after the header.
    Clog = 3,
}

impl PageKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Heap),
            2 => Some(Self::BTree),
            3 => Some(Self::Clog),
            _ => None,
        }
    }
}

/// Decoded common page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub lsn: Lsn,
    pub checksum: u32,
    pub flags: u16,
    pub lower: u16,
    pub upper: u16,
    pub special: u16,
    pub version: u8,
    pub kind: PageKind,
}

pub(super) mod off {
    pub const LSN: usize = 0;
    pub const CHECKSUM: usize = 8;
    pub const FLAGS: usize = 12;
    pub const LOWER: usize = 14;
    pub const UPPER: usize = 16;
    pub const SPECIAL: usize = 18;
    pub const VERSION: usize = 20;
    pub const KIND: usize = 21;
    pub const RESERVED: usize = 22;
}

impl PageHeader {
    /// Header for an empty page of `kind` with a special area of
    /// `special_size` bytes at the end of the page.
    pub fn new_empty(kind: PageKind, special_size: usize) -> Result<Self> {
        if !special_size.is_multiple_of(SPECIAL_ALIGN)
            || special_size > PAGE_SIZE - PAGE_HEADER_SIZE
        {
            return Err(StorageError::CorruptPage("invalid special area size"));
        }
        let special = to_u16(PAGE_SIZE - special_size)?;
        Ok(Self {
            lsn: Lsn::INVALID,
            checksum: 0,
            flags: 0,
            lower: to_u16(PAGE_HEADER_SIZE)?,
            upper: special,
            special,
            version: PAGE_LAYOUT_VERSION,
            kind,
        })
    }

    /// Decodes and validates the header of `page`.
    pub fn decode(page: &[u8]) -> Result<Self> {
        check_page_len(page)?;
        if read_u16(page, off::RESERVED) != 0 {
            return Err(StorageError::CorruptPage(
                "reserved header bytes are not zero",
            ));
        }
        let kind = PageKind::from_u8(page[off::KIND])
            .ok_or(StorageError::CorruptPage("unknown page kind"))?;
        let hdr = Self {
            lsn: Lsn(read_u64(page, off::LSN)),
            checksum: read_u32(page, off::CHECKSUM),
            flags: read_u16(page, off::FLAGS),
            lower: read_u16(page, off::LOWER),
            upper: read_u16(page, off::UPPER),
            special: read_u16(page, off::SPECIAL),
            version: page[off::VERSION],
            kind,
        };
        hdr.validate()?;
        Ok(hdr)
    }

    /// Validates and writes this header into the first bytes of `page`.
    pub fn encode(&self, page: &mut [u8]) -> Result<()> {
        check_page_len(page)?;
        self.validate()?;
        page[off::LSN..off::LSN + 8].copy_from_slice(&self.lsn.0.to_le_bytes());
        page[off::CHECKSUM..off::CHECKSUM + 4].copy_from_slice(&self.checksum.to_le_bytes());
        page[off::FLAGS..off::FLAGS + 2].copy_from_slice(&self.flags.to_le_bytes());
        page[off::LOWER..off::LOWER + 2].copy_from_slice(&self.lower.to_le_bytes());
        page[off::UPPER..off::UPPER + 2].copy_from_slice(&self.upper.to_le_bytes());
        page[off::SPECIAL..off::SPECIAL + 2].copy_from_slice(&self.special.to_le_bytes());
        page[off::VERSION] = self.version;
        page[off::KIND] = self.kind as u8;
        page[off::RESERVED..off::RESERVED + 2].copy_from_slice(&0u16.to_le_bytes());
        Ok(())
    }

    /// Checks `HEADER <= lower <= upper <= special <= PAGE_SIZE`, special
    /// alignment, and the layout version.
    pub fn validate(&self) -> Result<()> {
        let (lower, upper, special) = (
            usize::from(self.lower),
            usize::from(self.upper),
            usize::from(self.special),
        );
        if self.version != PAGE_LAYOUT_VERSION {
            return Err(StorageError::CorruptPage("unsupported page layout version"));
        }
        if !(PAGE_HEADER_SIZE <= lower
            && lower <= upper
            && upper <= special
            && special <= PAGE_SIZE)
        {
            return Err(StorageError::CorruptPage(
                "inconsistent lower/upper/special",
            ));
        }
        if !special.is_multiple_of(SPECIAL_ALIGN) {
            return Err(StorageError::CorruptPage("misaligned special area"));
        }
        Ok(())
    }

    /// Bytes between the line pointer array and the item data.
    pub fn free_space(&self) -> usize {
        usize::from(self.upper) - usize::from(self.lower)
    }
}

/// Page LSN read directly from the header bytes (no validation; an all-zero
/// page yields `Lsn::INVALID`). `page` must be at least 8 bytes.
pub fn page_lsn(page: &[u8]) -> Lsn {
    Lsn(read_u64(page, off::LSN))
}

/// Sets the page LSN in the header bytes. `page` must be at least 8 bytes.
pub fn set_page_lsn(page: &mut [u8], lsn: Lsn) {
    page[off::LSN..off::LSN + 8].copy_from_slice(&lsn.0.to_le_bytes());
}

/// True if `page` is entirely zero (allocated but never initialized).
pub fn is_new_page(page: &[u8]) -> bool {
    page.iter().all(|&b| b == 0)
}

pub(super) fn check_page_len(page: &[u8]) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(StorageError::InvalidPageLength { len: page.len() });
    }
    Ok(())
}

fn to_u16(v: usize) -> Result<u16> {
    u16::try_from(v).map_err(|_| StorageError::CorruptPage("offset exceeds u16"))
}

pub(super) fn read_u16(b: &[u8], at: usize) -> u16 {
    let mut a = [0u8; 2];
    a.copy_from_slice(&b[at..at + 2]);
    u16::from_le_bytes(a)
}

pub(super) fn read_u32(b: &[u8], at: usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(a)
}

pub(super) fn read_u64(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::AlignedBuf;
    use proptest::prelude::*;

    fn page() -> AlignedBuf {
        AlignedBuf::new(PAGE_SIZE).unwrap()
    }

    #[test]
    fn empty_header_layout() {
        let h = PageHeader::new_empty(PageKind::BTree, 16).unwrap();
        assert_eq!(usize::from(h.lower), PAGE_HEADER_SIZE);
        assert_eq!(usize::from(h.upper), PAGE_SIZE - 16);
        assert_eq!(h.special, h.upper);
        assert_eq!(h.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE - 16);
    }

    #[test]
    fn encode_writes_documented_offsets() {
        let mut p = page();
        let mut h = PageHeader::new_empty(PageKind::Heap, 0).unwrap();
        h.lsn = Lsn(0x0102_0304_0506_0708);
        h.checksum = 0xAABB_CCDD;
        h.encode(&mut p).unwrap();
        assert_eq!(&p[0..8], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&p[8..12], &0xAABB_CCDDu32.to_le_bytes());
        assert_eq!(&p[14..16], &(PAGE_HEADER_SIZE as u16).to_le_bytes());
        assert_eq!(&p[18..20], &(PAGE_SIZE as u16).to_le_bytes());
        assert_eq!(p[20], PAGE_LAYOUT_VERSION);
        assert_eq!(p[21], PageKind::Heap as u8);
        assert!(p[PAGE_HEADER_SIZE..].iter().all(|&b| b == 0));
    }

    #[test]
    fn new_page_is_detected_and_not_decodable() {
        let mut p = page();
        assert!(is_new_page(&p));
        assert!(PageHeader::decode(&p).is_err());
        PageHeader::new_empty(PageKind::Heap, 0)
            .unwrap()
            .encode(&mut p)
            .unwrap();
        assert!(!is_new_page(&p));
    }

    #[test]
    fn rejects_wrong_page_length() {
        let buf = vec![0u8; PAGE_SIZE - 1];
        assert!(matches!(
            PageHeader::decode(&buf),
            Err(StorageError::InvalidPageLength { len }) if len == PAGE_SIZE - 1
        ));
    }

    #[test]
    fn rejects_bad_special_size() {
        assert!(PageHeader::new_empty(PageKind::Heap, 3).is_err());
        assert!(PageHeader::new_empty(PageKind::Heap, PAGE_SIZE).is_err());
    }

    #[test]
    fn decode_rejects_corruption() {
        let mut p = page();
        PageHeader::new_empty(PageKind::Heap, 0)
            .unwrap()
            .encode(&mut p)
            .unwrap();

        let mut bad = p.to_vec();
        bad[21] = 99; // unknown kind
        assert!(PageHeader::decode(&bad).is_err());

        let mut bad = p.to_vec();
        bad[20] = PAGE_LAYOUT_VERSION + 1; // version
        assert!(PageHeader::decode(&bad).is_err());

        let mut bad = p.to_vec();
        bad[22] = 1; // reserved
        assert!(PageHeader::decode(&bad).is_err());

        let mut bad = p.to_vec();
        bad[16..18].copy_from_slice(&10u16.to_le_bytes()); // upper < lower
        assert!(PageHeader::decode(&bad).is_err());
    }

    #[test]
    fn block_byte_offset() {
        assert_eq!(BlockNumber(0).byte_offset(), 0);
        assert_eq!(BlockNumber(3).byte_offset(), 3 * PAGE_SIZE as u64);
        assert_eq!(
            BlockNumber(u32::MAX).byte_offset(),
            u64::from(u32::MAX) * PAGE_SIZE as u64
        );
    }

    fn valid_header() -> impl Strategy<Value = PageHeader> {
        let max = PAGE_SIZE / SPECIAL_ALIGN;
        (
            any::<u64>(),
            any::<u32>(),
            any::<u16>(),
            prop_oneof![Just(PageKind::Heap), Just(PageKind::BTree)],
            (PAGE_HEADER_SIZE / SPECIAL_ALIGN..=max),
        )
            .prop_flat_map(move |(lsn, checksum, flags, kind, special_units)| {
                let special = special_units * SPECIAL_ALIGN;
                (PAGE_HEADER_SIZE..=special).prop_flat_map(move |lower| {
                    (lower..=special).prop_map(move |upper| PageHeader {
                        lsn: Lsn(lsn),
                        checksum,
                        flags,
                        lower: lower as u16,
                        upper: upper as u16,
                        special: special as u16,
                        version: PAGE_LAYOUT_VERSION,
                        kind,
                    })
                })
            })
    }

    proptest! {
        #[test]
        fn header_roundtrip(h in valid_header()) {
            let mut p = page();
            h.encode(&mut p).unwrap();
            prop_assert_eq!(PageHeader::decode(&p).unwrap(), h);
            prop_assert!(p[PAGE_HEADER_SIZE..].iter().all(|&b| b == 0));
        }
    }
}
