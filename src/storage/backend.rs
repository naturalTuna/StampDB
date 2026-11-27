//! File access abstraction.
//!
//! [`StorageBackend`] is the single path to persistent storage, so that the
//! local-directory implementation can later be replaced by a cluster
//! filesystem or raw block device manager (the role of PostgreSQL's smgr).
//!
//! I/O is by aligned byte offset rather than by page, because WAL segments
//! (partial 4KB blocks) and the control file will use the same trait.
//!
//! Every mutating operation takes a [`FenceToken`]. Single-mode backends
//! ignore it; ha-mode backends verify it right before the write, and it is
//! the hook for storage-level fencing (SCSI-3 PR, NVMe reservations,
//! storage-side epoch checks). `sync` takes no token: it creates no new data,
//! only makes already-checked writes durable.

use std::fmt;

use super::aligned::IO_ALIGN;
use super::{Result, StorageError};

/// Writer epoch used to fence stale writers in ha mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceToken(u64);

impl FenceToken {
    pub const fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub const fn epoch(self) -> u64 {
        self.0
    }
}

/// Identifier of a relation's data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelFileId(pub u32);

/// Logical name of a file managed by a backend.
///
/// WAL and control files will be added in the WAL step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FileKey {
    Relation(RelFileId),
}

impl fmt::Display for FileKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileKey::Relation(r) => write!(f, "relation {}", r.0),
        }
    }
}

/// Persistent storage access with Direct I/O alignment rules.
///
/// For `read_at`/`write_at`, the buffer address, `offset` and buffer length
/// must be multiples of [`IO_ALIGN`]; otherwise `StorageError::Misaligned`.
pub trait StorageBackend: Send + Sync + fmt::Debug {
    /// Reads exactly `buf.len()` bytes at `offset`.
    fn read_at(&self, file: FileKey, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// Writes all of `buf` at `offset`, extending the file if needed.
    /// Not durable until [`StorageBackend::sync`].
    fn write_at(&self, file: FileKey, offset: u64, buf: &[u8], fence: FenceToken) -> Result<()>;

    /// Creates an empty file; fails if it exists. Durable on return.
    fn create(&self, file: FileKey, fence: FenceToken) -> Result<()>;

    /// Removes a file. Durable on return.
    fn remove(&self, file: FileKey, fence: FenceToken) -> Result<()>;

    /// Sets the file length (a multiple of `IO_ALIGN`).
    /// Not durable until [`StorageBackend::sync`].
    fn truncate(&self, file: FileKey, len: u64, fence: FenceToken) -> Result<()>;

    /// Current file length in bytes.
    fn file_len(&self, file: FileKey) -> Result<u64>;

    /// Makes previous writes and length changes durable (fdatasync).
    fn sync(&self, file: FileKey) -> Result<()>;
}

/// Checks Direct I/O alignment of an offset/buffer pair.
pub fn check_io_alignment(offset: u64, buf: &[u8]) -> Result<()> {
    let align = IO_ALIGN as u64;
    if !offset.is_multiple_of(align) {
        return Err(StorageError::Misaligned {
            what: "offset",
            value: offset,
        });
    }
    if buf.is_empty() || !buf.len().is_multiple_of(IO_ALIGN) {
        return Err(StorageError::Misaligned {
            what: "length",
            value: buf.len() as u64,
        });
    }
    let addr = buf.as_ptr() as usize;
    if !addr.is_multiple_of(IO_ALIGN) {
        return Err(StorageError::Misaligned {
            what: "buffer address",
            value: addr as u64,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::AlignedBuf;

    #[test]
    fn alignment_rules() {
        let buf = AlignedBuf::new(2 * IO_ALIGN).unwrap();
        check_io_alignment(0, &buf).unwrap();
        check_io_alignment(IO_ALIGN as u64 * 5, &buf[..IO_ALIGN]).unwrap();
        assert!(matches!(
            check_io_alignment(512, &buf),
            Err(StorageError::Misaligned { what: "offset", .. })
        ));
        assert!(matches!(
            check_io_alignment(0, &buf[..512]),
            Err(StorageError::Misaligned { what: "length", .. })
        ));
        assert!(matches!(
            check_io_alignment(0, &buf[..0]),
            Err(StorageError::Misaligned { what: "length", .. })
        ));
        assert!(matches!(
            check_io_alignment(0, &buf[512..512 + IO_ALIGN]),
            Err(StorageError::Misaligned {
                what: "buffer address",
                ..
            })
        ));
    }

    #[test]
    fn fence_token_epoch() {
        assert_eq!(FenceToken::new(3).epoch(), 3);
        assert!(FenceToken::new(1) < FenceToken::new(2));
    }
}
