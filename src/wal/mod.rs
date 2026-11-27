//! Write-ahead log: record format, log files, control file, and recovery.

pub mod control;
pub mod log;
pub mod record;

use crate::storage::StorageError;

pub use control::{ControlData, ControlFile, DbState};
pub use log::{Wal, WalConfig, WalReader};

/// Errors from WAL operations that are not fatal.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Record(#[from] record::RecordError),

    #[error("invalid WAL configuration: {0}")]
    InvalidConfig(&'static str),

    #[error("no valid control file block")]
    ControlCorrupt,
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, WalError>;

/// Stops the process after an error that must not be retried: a failed WAL
/// write or fsync, or a failure inside a critical section. Restart recovers
/// from WAL (PostgreSQL's PANIC; DESIGN r6).
pub(crate) fn fatal(what: &str, err: &dyn std::fmt::Display) -> ! {
    eprintln!("FATAL: {what}: {err}");
    std::process::abort()
}

/// Whether a storage error means "the file does not exist".
pub(crate) fn is_not_found(e: &StorageError) -> bool {
    matches!(e, StorageError::FileIo { source, .. } if source.kind() == std::io::ErrorKind::NotFound)
}
