//! Error type for the storage layer.

/// Errors produced by the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Underlying OS I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
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
