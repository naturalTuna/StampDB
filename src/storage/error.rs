//! Error type for the storage layer.

/// Errors produced by the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Underlying OS I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

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
