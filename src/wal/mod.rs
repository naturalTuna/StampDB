//! Write-ahead log: record format, log files, control file, and recovery.

pub mod background;
pub mod control;
pub mod log;
pub mod record;
pub mod recovery;

use crate::storage::StorageError;

pub use background::{Background, BackgroundConfig};
pub use control::{ControlData, ControlFile, DbState};
pub use log::{Wal, WalConfig, WalReader};
pub use recovery::{Engine, RecoveryError, initdb};

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

/// Logs `rec` (one block reference per page, in order; pages already
/// modified and locked) with full page writes as needed, and sets each
/// page's LSN to the record's end. No-op without a WAL. Aborts the process
/// on failure, since the pages are already modified (critical section).
pub fn log_and_stamp(
    wal: Option<&std::sync::Arc<Wal>>,
    rec: record::Record,
    pages: &mut [&mut [u8]],
) {
    let Some(wal) = wal else {
        return;
    };
    let images: Vec<&[u8]> = pages.iter().map(|p| &**p).collect();
    let end = match wal.insert_fpw(rec, &images) {
        Ok((_, end)) => end,
        Err(e) => fatal("WAL insert failed in a critical section", &e),
    };
    for p in pages.iter_mut() {
        crate::storage::page::set_page_lsn(p, end);
    }
}

/// Whether a storage error means "the file does not exist".
pub(crate) fn is_not_found(e: &StorageError) -> bool {
    matches!(e, StorageError::FileIo { source, .. } if source.kind() == std::io::ErrorKind::NotFound)
}
