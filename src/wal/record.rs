//! WAL record format (generic container; operation payloads are defined by
//! each resource manager).
//!
//! ```text
//! header (32 bytes, little-endian)
//!   0  total_len u32   whole record, header included
//!   4  crc       u32   CRC32C of the record with this field zeroed
//!   8  prev_lsn  u64   start of the previous record (chain check)
//!  16  xid       u64
//!  24  rmgr      u8
//!  25  info      u8    operation within the rmgr
//!  26  nblocks   u8
//!  27  reserved  u8
//!  28  main_len  u32
//! block refs, each:
//!   rel u32 | blk u32 | flags u8
//!   [HAS_IMAGE] hole_off u16 | hole_len u16 | page bytes without the hole
//!   [HAS_DATA]  data_len u16 | data
//! main data (main_len bytes)
//! ```
//!
//! Page images omit the free space between `lower` and `upper` (PostgreSQL's
//! "hole"); it is restored as zeros.

use crate::storage::backend::RelFileId;
use crate::storage::page::{BlockNumber, Lsn, PAGE_SIZE, PageHeader};
use crate::txn::Xid;

/// Size of the fixed record header.
pub const HEADER_SIZE: usize = 32;

/// Maximum block references per record (same as PostgreSQL).
pub const MAX_BLOCKS: usize = 32;

/// Upper bound on a record's size, to reject garbage lengths early.
pub const MAX_RECORD_SIZE: usize = 64 << 20;

mod bflag {
    pub const HAS_IMAGE: u8 = 0x01;
    pub const WILL_INIT: u8 = 0x02;
    pub const HAS_DATA: u8 = 0x04;
}

/// Errors from record encoding and decoding.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RecordError {
    #[error("truncated record")]
    Truncated,
    #[error("record checksum mismatch")]
    CrcMismatch,
    #[error("record too large: {0} bytes")]
    TooLarge(usize),
    #[error("corrupt record: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, RecordError>;

/// Resource manager: which subsystem's redo function applies a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RmgrId {
    /// WAL itself: checkpoints, hint-bit page images.
    Xlog = 0,
    /// Relation files: create.
    Smgr = 1,
    Heap = 2,
    Btree = 3,
}

impl RmgrId {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Xlog),
            1 => Some(Self::Smgr),
            2 => Some(Self::Heap),
            3 => Some(Self::Btree),
            _ => None,
        }
    }
}

/// A page touched by a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    pub rel: RelFileId,
    pub blk: BlockNumber,
    /// Redo rebuilds the page from scratch; its old contents are irrelevant.
    pub will_init: bool,
    /// Full page image (full page write); redo restores it instead of
    /// applying `data`.
    pub image: Option<Vec<u8>>,
    /// Operation-specific data for this page.
    pub data: Vec<u8>,
}

impl BlockRef {
    pub fn new(rel: RelFileId, blk: BlockNumber) -> Self {
        Self {
            rel,
            blk,
            will_init: false,
            image: None,
            data: Vec::new(),
        }
    }
}

/// A WAL record before it has a position in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub xid: Xid,
    pub rmgr: RmgrId,
    pub info: u8,
    pub blocks: Vec<BlockRef>,
    pub main: Vec<u8>,
}

/// A record read back from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub prev_lsn: Lsn,
    pub record: Record,
}

impl Record {
    pub fn new(rmgr: RmgrId, info: u8) -> Self {
        Self {
            xid: Xid::INVALID,
            rmgr,
            info,
            blocks: Vec::new(),
            main: Vec::new(),
        }
    }

    /// Serializes the record, chained to `prev_lsn`.
    pub fn encode(&self, prev_lsn: Lsn) -> Result<Vec<u8>> {
        if self.blocks.len() > MAX_BLOCKS {
            return Err(RecordError::Corrupt("too many block references"));
        }
        let main_len =
            u32::try_from(self.main.len()).map_err(|_| RecordError::TooLarge(self.main.len()))?;
        let mut out = vec![0u8; HEADER_SIZE];
        for b in &self.blocks {
            encode_block(b, &mut out)?;
        }
        out.extend_from_slice(&self.main);
        if out.len() > MAX_RECORD_SIZE {
            return Err(RecordError::TooLarge(out.len()));
        }
        let total = out.len() as u32;
        out[0..4].copy_from_slice(&total.to_le_bytes());
        out[8..16].copy_from_slice(&prev_lsn.0.to_le_bytes());
        out[16..24].copy_from_slice(&self.xid.0.to_le_bytes());
        out[24] = self.rmgr as u8;
        out[25] = self.info;
        out[26] = self.blocks.len() as u8;
        out[28..32].copy_from_slice(&main_len.to_le_bytes());
        let crc = crc32c::crc32c(&out);
        out[4..8].copy_from_slice(&crc.to_le_bytes());
        Ok(out)
    }
}

/// Total length announced by the header at the start of `buf`, if there is
/// a plausible one. A zero length means "no record here" (end of WAL).
pub fn peek_len(buf: &[u8]) -> Option<usize> {
    let b = buf.get(..4)?;
    let len = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize;
    (HEADER_SIZE..=MAX_RECORD_SIZE)
        .contains(&len)
        .then_some(len)
}

/// Decodes and verifies one record occupying exactly `buf`.
pub fn decode(buf: &[u8]) -> Result<Decoded> {
    if buf.len() < HEADER_SIZE {
        return Err(RecordError::Truncated);
    }
    let total = u32_at(buf, 0) as usize;
    if total != buf.len() {
        return Err(RecordError::Corrupt("length mismatch"));
    }
    let mut copy = buf.to_vec();
    copy[4..8].fill(0);
    if crc32c::crc32c(&copy) != u32_at(buf, 4) {
        return Err(RecordError::CrcMismatch);
    }
    let rmgr = RmgrId::from_u8(buf[24]).ok_or(RecordError::Corrupt("unknown rmgr"))?;
    let nblocks = usize::from(buf[26]);
    if nblocks > MAX_BLOCKS {
        return Err(RecordError::Corrupt("too many block references"));
    }
    let main_len = u32_at(buf, 28) as usize;

    let mut r = Reader {
        buf,
        pos: HEADER_SIZE,
    };
    let mut blocks = Vec::with_capacity(nblocks);
    for _ in 0..nblocks {
        blocks.push(decode_block(&mut r)?);
    }
    let main = r.take(main_len)?.to_vec();
    if r.pos != buf.len() {
        return Err(RecordError::Corrupt("trailing bytes"));
    }
    Ok(Decoded {
        prev_lsn: Lsn(u64_at(buf, 8)),
        record: Record {
            xid: Xid(u64_at(buf, 16)),
            rmgr,
            info: buf[25],
            blocks,
            main,
        },
    })
}

fn encode_block(b: &BlockRef, out: &mut Vec<u8>) -> Result<()> {
    let mut flags = 0;
    if b.image.is_some() {
        flags |= bflag::HAS_IMAGE;
    }
    if b.will_init {
        flags |= bflag::WILL_INIT;
    }
    if !b.data.is_empty() {
        flags |= bflag::HAS_DATA;
    }
    out.extend_from_slice(&b.rel.0.to_le_bytes());
    out.extend_from_slice(&b.blk.0.to_le_bytes());
    out.push(flags);
    if let Some(img) = &b.image {
        if img.len() != PAGE_SIZE {
            return Err(RecordError::Corrupt("page image is not a full page"));
        }
        let (off, len) = hole(img);
        out.extend_from_slice(&(off as u16).to_le_bytes());
        out.extend_from_slice(&(len as u16).to_le_bytes());
        out.extend_from_slice(&img[..off]);
        out.extend_from_slice(&img[off + len..]);
    }
    if !b.data.is_empty() {
        let len = u16::try_from(b.data.len()).map_err(|_| RecordError::TooLarge(b.data.len()))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&b.data);
    }
    Ok(())
}

fn decode_block(r: &mut Reader<'_>) -> Result<BlockRef> {
    let rel = RelFileId(r.u32()?);
    let blk = BlockNumber(r.u32()?);
    let flags = r.u8()?;
    if flags & !(bflag::HAS_IMAGE | bflag::WILL_INIT | bflag::HAS_DATA) != 0 {
        return Err(RecordError::Corrupt("unknown block flags"));
    }
    let image = if flags & bflag::HAS_IMAGE != 0 {
        let off = usize::from(r.u16()?);
        let len = usize::from(r.u16()?);
        if off + len > PAGE_SIZE {
            return Err(RecordError::Corrupt("image hole out of range"));
        }
        let mut page = vec![0u8; PAGE_SIZE];
        page[..off].copy_from_slice(r.take(off)?);
        page[off + len..].copy_from_slice(r.take(PAGE_SIZE - off - len)?);
        Some(page)
    } else {
        None
    };
    let data = if flags & bflag::HAS_DATA != 0 {
        let len = usize::from(r.u16()?);
        r.take(len)?.to_vec()
    } else {
        Vec::new()
    };
    Ok(BlockRef {
        rel,
        blk,
        will_init: flags & bflag::WILL_INIT != 0,
        image,
        data,
    })
}

/// The free-space hole of a page image, if the page has a valid header and
/// the hole is all zeros; otherwise no hole.
fn hole(page: &[u8]) -> (usize, usize) {
    match PageHeader::decode(page) {
        Ok(h) => {
            let (lo, up) = (usize::from(h.lower), usize::from(h.upper));
            if page[lo..up].iter().all(|&b| b == 0) {
                (lo, up - lo)
            } else {
                (0, 0)
            }
        }
        Err(_) => (0, 0),
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let s = self
            .buf
            .get(self.pos..self.pos + n)
            .ok_or(RecordError::Truncated)?;
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
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
    use crate::storage::page::PageKind;
    use crate::storage::slotted::SlottedPage;
    use proptest::prelude::*;

    fn heap_page(items: usize) -> Vec<u8> {
        let mut p = SlottedPage::init(vec![0u8; PAGE_SIZE], PageKind::Heap, 0).unwrap();
        for i in 0..items {
            p.add_item(&[i as u8; 40]).unwrap().unwrap();
        }
        p.into_inner()
    }

    fn sample() -> Record {
        let mut b0 = BlockRef::new(RelFileId(7), BlockNumber(3));
        b0.data = vec![1, 2, 3];
        let mut b1 = BlockRef::new(RelFileId(7), BlockNumber(4));
        b1.image = Some(heap_page(5));
        b1.will_init = true;
        Record {
            xid: Xid(99),
            rmgr: RmgrId::Heap,
            info: 2,
            blocks: vec![b0, b1],
            main: b"main".to_vec(),
        }
    }

    #[test]
    fn roundtrip_with_image_and_data() {
        let rec = sample();
        let bytes = rec.encode(Lsn(1234)).unwrap();
        assert_eq!(peek_len(&bytes), Some(bytes.len()));
        let d = decode(&bytes).unwrap();
        assert_eq!(d.prev_lsn, Lsn(1234));
        assert_eq!(d.record, rec);
    }

    #[test]
    fn image_hole_is_omitted() {
        let mut b = BlockRef::new(RelFileId(1), BlockNumber(0));
        b.image = Some(heap_page(2));
        let mut rec = Record::new(RmgrId::Xlog, 0);
        rec.blocks.push(b);
        let bytes = rec.encode(Lsn(0)).unwrap();
        assert!(bytes.len() < 200, "{} bytes", bytes.len());
        assert_eq!(decode(&bytes).unwrap().record, rec);

        // A page without a valid header is stored whole.
        let mut raw = BlockRef::new(RelFileId(1), BlockNumber(0));
        raw.image = Some(vec![0xAB; PAGE_SIZE]);
        let mut rec2 = Record::new(RmgrId::Xlog, 0);
        rec2.blocks.push(raw);
        let bytes2 = rec2.encode(Lsn(0)).unwrap();
        assert!(bytes2.len() > PAGE_SIZE);
        assert_eq!(decode(&bytes2).unwrap().record, rec2);
    }

    #[test]
    fn detects_corruption_and_truncation() {
        let bytes = sample().encode(Lsn(1)).unwrap();
        for i in [0usize, 5, 20, HEADER_SIZE + 3, bytes.len() - 1] {
            let mut bad = bytes.clone();
            bad[i] ^= 0x40;
            assert!(decode(&bad).is_err(), "flip at {i} undetected");
        }
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
        assert_eq!(decode(&bytes[..10]), Err(RecordError::Truncated));
        assert_eq!(peek_len(&[0, 0, 0, 0]), None, "zero length is end of WAL");
    }

    #[test]
    fn rejects_bad_inputs() {
        let mut rec = Record::new(RmgrId::Heap, 0);
        rec.blocks = vec![BlockRef::new(RelFileId(1), BlockNumber(0)); MAX_BLOCKS + 1];
        assert!(rec.encode(Lsn(0)).is_err());
        let mut rec = Record::new(RmgrId::Heap, 0);
        let mut b = BlockRef::new(RelFileId(1), BlockNumber(0));
        b.image = Some(vec![0; 100]);
        rec.blocks.push(b);
        assert!(rec.encode(Lsn(0)).is_err());
    }

    fn block() -> impl Strategy<Value = BlockRef> {
        (
            any::<u32>(),
            any::<u32>(),
            any::<bool>(),
            prop::option::of(0usize..60),
            prop::collection::vec(any::<u8>(), 0..300),
        )
            .prop_map(|(rel, blk, will_init, image, data)| BlockRef {
                rel: RelFileId(rel),
                blk: BlockNumber(blk),
                will_init,
                image: image.map(heap_page),
                data,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn roundtrip(xid in any::<u64>(), info in any::<u8>(), prev in any::<u64>(),
                     blocks in prop::collection::vec(block(), 0..4),
                     main in prop::collection::vec(any::<u8>(), 0..500),
                     rmgr in prop_oneof![Just(RmgrId::Xlog), Just(RmgrId::Smgr), Just(RmgrId::Heap), Just(RmgrId::Btree)]) {
            let rec = Record { xid: Xid(xid), rmgr, info, blocks, main };
            let bytes = rec.encode(Lsn(prev)).unwrap();
            let d = decode(&bytes).unwrap();
            prop_assert_eq!(d.prev_lsn, Lsn(prev));
            prop_assert_eq!(d.record, rec);
        }
    }
}
