//! Error type for the storage layer.

/// Errors produced by the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Underlying OS I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// OS I/O failure on a specific file.
    #[error("{op} {}: {source}", path.display())]
    FileIo {
        op: &'static str,
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Read hit end of file before filling the buffer.
    #[error("short read from {file} at offset {offset} (len {len})")]
    ShortRead {
        file: crate::storage::backend::FileKey,
        offset: u64,
        len: usize,
    },

    /// Direct I/O offset, length or buffer address is not aligned.
    #[error("misaligned direct I/O {what}: {value}")]
    Misaligned { what: &'static str, value: u64 },

    /// Device logical block size could not be determined (startup refused).
    #[error("cannot determine logical block size for {}: {reason}", path.display())]
    BlockSizeUnknown {
        path: std::path::PathBuf,
        reason: String,
    },

    /// File length is not a whole number of pages.
    #[error("{file} has length {len}, not a multiple of the page size")]
    InvalidFileLength {
        file: crate::storage::backend::FileKey,
        len: u64,
    },

    /// Relation would exceed the maximum block number.
    #[error("{file} is too large")]
    RelationTooLarge {
        file: crate::storage::backend::FileKey,
    },

    /// Device logical block size is not supported (startup refused).
    #[error("unsupported logical block size {size} (must divide 4096)")]
    BlockSizeUnsupported { size: u64 },

    /// Direct I/O buffer length is zero or not a multiple of the alignment.
    #[error("invalid aligned buffer size: {len}")]
    InvalidBufferSize { len: usize },

    /// A page buffer does not have exactly `PAGE_SIZE` bytes.
    #[error("invalid page length: {len}")]
    InvalidPageLength { len: usize },

    /// A page header failed validation.
    #[error("corrupt page: {0}")]
    CorruptPage(&'static str),

    /// Item length is zero or exceeds `MAX_ITEM_SIZE`.
    #[error("invalid item size: {len}")]
    InvalidItemSize { len: usize },

    /// Slot number is beyond the line pointer array.
    #[error("slot {slot} does not exist")]
    InvalidSlot { slot: u16 },

    /// Operation not allowed in the slot's current state.
    #[error("slot {slot} is in state {state:?}")]
    InvalidSlotState {
        slot: u16,
        state: crate::storage::slotted::ItemState,
    },

    /// Stored page checksum does not match the page contents.
    #[error("checksum mismatch in block {block}: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        block: u32,
        stored: u32,
        computed: u32,
    },
}

/// Result alias for the storage layer.
pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts_into_storage_error() {
        fn fails() -> Result<()> {
            Err(std::io::Error::other("boom"))?;
            Ok(())
        }
        let err = fails().unwrap_err();
        assert!(matches!(err, StorageError::Io(_)));
        assert_eq!(err.to_string(), "I/O error: boom");
    }
}
