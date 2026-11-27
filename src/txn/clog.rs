//! Commit log (CLOG): two status bits per xid, stored in a dedicated
//! relation through the buffer pool (DESIGN r7), so pages get checksums,
//! full page writes, WAL-before-data, and checkpoints like any other page.
//!
//! ```text
//! page: 24-byte header | status bits, 4 xids per byte (2 bits each)
//! bits: 00 in progress | 01 committed | 10 aborted
//! ```
//!
//! A commit or abort sets the bits while the CLOG page is locked and logs a
//! record carrying that page as a block reference, so redo is page-local
//! and full page writes apply. Whether others see the outcome is decided by
//! the running set first: a transaction is "in progress" while it is still
//! running, whatever its bits say, and it leaves the running set only after
//! its commit record is flushed.

use std::sync::Arc;

use parking_lot::Mutex;

use super::{Xid, XidStatus};
use crate::buffer::{BufferError, BufferPool, BufferTag};
use crate::storage::StorageError;
use crate::storage::backend::RelFileId;
use crate::storage::page::{BlockNumber, Lsn, PAGE_HEADER_SIZE, PAGE_SIZE, PageHeader, PageKind};
use crate::wal::log_and_stamp;
use crate::wal::record::{BlockRef, Record, RmgrId};

/// Relation file holding the commit log. Ids below 16384 are reserved for
/// system files (as in PostgreSQL).
pub const CLOG_REL: RelFileId = RelFileId(1);

/// Transactions per CLOG page.
pub const XIDS_PER_PAGE: u64 = ((PAGE_SIZE - PAGE_HEADER_SIZE) * 4) as u64;

const BITS_IN_PROGRESS: u8 = 0b00;
const BITS_COMMITTED: u8 = 0b01;
const BITS_ABORTED: u8 = 0b10;

/// `info` values of the Xact resource manager. Each record's xid is the
/// transaction concerned.
pub mod xact_info {
    /// Block 0: the CLOG page; redo marks the xid committed. Main data:
    /// relation files to delete (u32 each), the transaction's DROPs.
    pub const COMMIT: u8 = 0;
    /// Block 0: the CLOG page; redo marks the xid aborted. Main data:
    /// relation files to delete (u32 each), the transaction's CREATEs.
    pub const ABORT: u8 = 1;
    /// Block 0 (will-init): a new, all-in-progress CLOG page.
    pub const ZERO_PAGE: u8 = 2;
}

/// Errors from the commit log.
#[derive(Debug, thiserror::Error)]
pub enum ClogError {
    #[error(transparent)]
    Buffer(#[from] BufferError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error("corrupt commit log record: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, ClogError>;

fn page_of(xid: Xid) -> BlockNumber {
    BlockNumber((xid.0 / XIDS_PER_PAGE) as u32)
}

/// Byte offset and bit shift of `xid` within its page.
fn position(xid: Xid) -> (usize, u32) {
    let i = xid.0 % XIDS_PER_PAGE;
    (PAGE_HEADER_SIZE + (i / 4) as usize, ((i % 4) * 2) as u32)
}

fn read_bits(page: &[u8], xid: Xid) -> u8 {
    let (byte, shift) = position(xid);
    (page[byte] >> shift) & 0b11
}

fn write_bits(page: &mut [u8], xid: Xid, bits: u8) {
    let (byte, shift) = position(xid);
    page[byte] = (page[byte] & !(0b11 << shift)) | (bits << shift);
}

fn init_page(page: &mut [u8]) -> Result<()> {
    page.fill(0);
    PageHeader::new_empty(PageKind::Clog, 0)?.encode(page)?;
    Ok(())
}

/// The commit log.
#[derive(Debug)]
pub struct Clog {
    pool: Arc<BufferPool>,
    /// Number of CLOG pages; extension is serialized by this lock.
    nblocks: Mutex<u32>,
}

impl Clog {
    pub fn open(pool: Arc<BufferPool>) -> Result<Self> {
        let n = pool.nblocks(CLOG_REL)?;
        Ok(Self {
            pool,
            nblocks: Mutex::new(n),
        })
    }

    /// Makes sure the page for `xid` exists (called when assigning it).
    pub fn ensure(&self, xid: Xid) -> Result<()> {
        let blk = page_of(xid);
        let mut n = self.nblocks.lock();
        while *n <= blk.0 {
            let (nblk, pin) = self.pool.extend(CLOG_REL)?;
            let mut page = pin.write();
            init_page(&mut page)?;
            page.mark_dirty();
            let mut b = BlockRef::new(CLOG_REL, nblk);
            b.will_init = true;
            let mut rec = Record::new(RmgrId::Xact, xact_info::ZERO_PAGE);
            rec.blocks.push(b);
            log_and_stamp(self.pool.wal(), rec, &mut [&mut page[..]]);
            *n = nblk.0 + 1;
        }
        Ok(())
    }

    /// Status bits of `xid` (in progress if its page does not exist yet).
    pub fn get(&self, xid: Xid) -> Result<XidStatus> {
        let blk = page_of(xid);
        if blk.0 >= *self.nblocks.lock() {
            return Ok(XidStatus::InProgress);
        }
        let pin = self.pool.fetch(BufferTag { rel: CLOG_REL, blk })?;
        let page = pin.read();
        match read_bits(&page, xid) {
            BITS_IN_PROGRESS => Ok(XidStatus::InProgress),
            BITS_COMMITTED => Ok(XidStatus::Committed),
            BITS_ABORTED => Ok(XidStatus::Aborted),
            _ => Err(ClogError::Corrupt("invalid status bits")),
        }
    }

    /// Records the outcome of `xid`: sets its bits and logs a COMMIT or
    /// ABORT record. Returns the record's end LSN (to flush for a commit).
    pub fn record_outcome(&self, xid: Xid, committed: bool, drop: &[RelFileId]) -> Result<Lsn> {
        self.ensure(xid)?;
        let blk = page_of(xid);
        let pin = self.pool.fetch(BufferTag { rel: CLOG_REL, blk })?;
        let mut page = pin.write();
        let (bits, info) = if committed {
            (BITS_COMMITTED, xact_info::COMMIT)
        } else {
            (BITS_ABORTED, xact_info::ABORT)
        };
        write_bits(&mut page, xid, bits);
        page.mark_dirty();
        let mut rec = Record::new(RmgrId::Xact, info);
        rec.xid = xid;
        rec.main = encode_rels(drop);
        rec.blocks.push(BlockRef::new(CLOG_REL, blk));
        log_and_stamp(self.pool.wal(), rec, &mut [&mut page[..]]);
        Ok(crate::storage::page::page_lsn(&page))
    }
}

fn encode_rels(rels: &[RelFileId]) -> Vec<u8> {
    rels.iter().flat_map(|r| r.0.to_le_bytes()).collect()
}

/// Relation files a COMMIT or ABORT record deletes.
pub fn dropped_rels(rec: &Record) -> Vec<RelFileId> {
    if rec.rmgr != RmgrId::Xact || !(rec.info == xact_info::COMMIT || rec.info == xact_info::ABORT)
    {
        return Vec::new();
    }
    rec.main
        .as_chunks::<4>()
        .0
        .iter()
        .map(|b| RelFileId(u32::from_le_bytes(*b)))
        .collect()
}

/// Redo of an Xact record's block (which carries no image).
pub fn redo(rec: &Record, page: &mut [u8]) -> Result<()> {
    match rec.info {
        xact_info::ZERO_PAGE => init_page(page),
        xact_info::COMMIT => {
            write_bits(page, rec.xid, BITS_COMMITTED);
            Ok(())
        }
        xact_info::ABORT => {
            write_bits(page, rec.xid, BITS_ABORTED);
            Ok(())
        }
        _ => Err(ClogError::Corrupt("unknown xact record")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_positions() {
        let mut page = vec![0u8; PAGE_SIZE];
        init_page(&mut page).unwrap();
        for x in [0u64, 1, 2, 3, 4, XIDS_PER_PAGE - 1] {
            write_bits(&mut page, Xid(x), BITS_COMMITTED);
        }
        write_bits(&mut page, Xid(2), BITS_ABORTED);
        assert_eq!(read_bits(&page, Xid(0)), BITS_COMMITTED);
        assert_eq!(read_bits(&page, Xid(2)), BITS_ABORTED);
        assert_eq!(read_bits(&page, Xid(3)), BITS_COMMITTED);
        assert_eq!(read_bits(&page, Xid(5)), BITS_IN_PROGRESS);
        assert_eq!(read_bits(&page, Xid(XIDS_PER_PAGE - 1)), BITS_COMMITTED);
        assert_eq!(position(Xid(XIDS_PER_PAGE - 1)).0, PAGE_SIZE - 1);
        assert_eq!(page_of(Xid(XIDS_PER_PAGE)), BlockNumber(1));
        PageHeader::decode(&page).unwrap();
    }
}
