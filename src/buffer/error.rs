//! Error type for the buffer pool.

use crate::storage::StorageError;

/// Errors produced by the buffer pool.
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    /// Underlying storage failure (I/O, checksum, corrupt page, ...).
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// No frame can be used for a new page (PostgreSQL's
    /// "no unpinned buffers available").
    #[error("no unpinned buffers available")]
    Exhausted,

    /// A buffer that had to be dropped is still pinned.
    #[error("buffer of relation {rel} block {blk} is pinned")]
    Pinned { rel: u32, blk: u32 },

    /// The pool must have at least one frame.
    #[error("buffer pool size must be at least 1")]
    InvalidPoolSize,
}

/// Result alias for the buffer pool.
pub type Result<T> = std::result::Result<T, BufferError>;
