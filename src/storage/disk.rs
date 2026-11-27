//! Disk manager: page-granular access to relation files.
//!
//! Maps (relation, block) to a byte offset in the backend, stamps the page
//! checksum on write, and verifies checksum + header on read (PostgreSQL's
//! `PageSetChecksumInplace` / `PageIsVerified`). All-zero pages are "new"
//! pages: they are written without a checksum and accepted on read.
//!
//! Only `extend` may write past the end of a file; it is serialized by an
//! internal lock (the role of PostgreSQL's relation extension lock).

use std::sync::Arc;

use parking_lot::Mutex;

use super::aligned::AlignedBuf;
use super::backend::{FenceToken, FileKey, RelFileId, StorageBackend};
use super::checksum::{set_checksum, verify_checksum};
use super::page::{self, BlockNumber, PAGE_SIZE, PageHeader, is_new_page};
use super::{Result, StorageError};

/// Page-level access to relation files on top of a [`StorageBackend`].
#[derive(Debug)]
pub struct DiskManager {
    backend: Arc<dyn StorageBackend>,
    extend_lock: Mutex<()>,
}

impl DiskManager {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            extend_lock: Mutex::new(()),
        }
    }

    /// Creates an empty relation file.
    pub fn create_relation(&self, rel: RelFileId, fence: FenceToken) -> Result<()> {
        self.backend.create(FileKey::Relation(rel), fence)
    }

    /// Removes a relation file.
    pub fn remove_relation(&self, rel: RelFileId, fence: FenceToken) -> Result<()> {
        self.backend.remove(FileKey::Relation(rel), fence)
    }

    /// Number of blocks in the relation.
    pub fn nblocks(&self, rel: RelFileId) -> Result<u32> {
        let file = FileKey::Relation(rel);
        let len = self.backend.file_len(file)?;
        if !len.is_multiple_of(PAGE_SIZE as u64) {
            return Err(StorageError::InvalidFileLength { file, len });
        }
        u32::try_from(len / PAGE_SIZE as u64).map_err(|_| StorageError::RelationTooLarge { file })
    }

    /// Reads block `blk` into `buf` and verifies its checksum and header.
    ///
    /// `buf` must be `PAGE_SIZE` bytes and `IO_ALIGN`-aligned.
    pub fn read_page(&self, rel: RelFileId, blk: BlockNumber, buf: &mut [u8]) -> Result<()> {
        page::check_page_len(buf)?;
        self.backend
            .read_at(FileKey::Relation(rel), blk.byte_offset(), buf)?;
        verify_checksum(buf, blk)?;
        if !is_new_page(buf) {
            PageHeader::decode(buf)?;
        }
        Ok(())
    }

    /// Stamps the checksum into `buf` and writes it to block `blk`.
    ///
    /// The block must already exist (see [`DiskManager::extend`]).
    /// Not durable until [`DiskManager::sync_relation`].
    pub fn write_page(
        &self,
        rel: RelFileId,
        blk: BlockNumber,
        buf: &mut [u8],
        fence: FenceToken,
    ) -> Result<()> {
        page::check_page_len(buf)?;
        if !is_new_page(buf) {
            set_checksum(buf, blk)?;
        }
        self.backend
            .write_at(FileKey::Relation(rel), blk.byte_offset(), buf, fence)
    }

    /// Appends an all-zero page and returns its block number.
    pub fn extend(&self, rel: RelFileId, fence: FenceToken) -> Result<BlockNumber> {
        let _guard = self.extend_lock.lock();
        let n = self.nblocks(rel)?;
        if n == u32::MAX {
            return Err(StorageError::RelationTooLarge {
                file: FileKey::Relation(rel),
            });
        }
        let blk = BlockNumber(n);
        let zero = AlignedBuf::new(PAGE_SIZE)?;
        self.backend
            .write_at(FileKey::Relation(rel), blk.byte_offset(), &zero, fence)?;
        Ok(blk)
    }

    /// Makes written pages and extensions durable (fdatasync).
    pub fn sync_relation(&self, rel: RelFileId) -> Result<()> {
        self.backend.sync(FileKey::Relation(rel))
    }
}
