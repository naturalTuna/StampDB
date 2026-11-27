//! Transactions: identifiers, the running-transaction set, snapshots, and
//! the transaction manager interface.
//!
//! * [`TxnId`] identifies a transaction from its start and owns its locks
//!   (PostgreSQL's virtual transaction id).
//! * [`Xid`] is assigned only when the transaction first writes (DESIGN
//!   r7), so read-only transactions consume none.
//! * [`ProcArray`] assigns xids and tracks the running ones; a
//!   [`Snapshot`] is a copy of that state (PostgreSQL's GetSnapshotData).

mod procarray;
mod snapshot;

pub use procarray::ProcArray;
pub use snapshot::Snapshot;

use std::fmt;

/// 64-bit transaction ID (no wraparound, no freezing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Xid(pub u64);

impl Xid {
    /// "No transaction" (e.g. a tuple's xmax when it was never deleted).
    pub const INVALID: Xid = Xid(0);

    /// The first xid handed out by a new cluster.
    pub const FIRST: Xid = Xid(1);

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    pub fn next(self) -> Xid {
        Xid(self.0 + 1)
    }
}

/// Command number within a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct CommandId(pub u32);

/// Identifies a transaction from its start, whether or not it has an xid;
/// the owner of its locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(pub u64);

/// Final or current state of a transaction, as recorded in the commit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XidStatus {
    /// Not committed or aborted (yet). After a crash, an xid that is not
    /// running and still in progress was aborted by the crash.
    InProgress,
    Committed,
    Aborted,
}

/// Errors from transaction management.
#[derive(Debug, thiserror::Error)]
pub enum TxnError {
    #[error("commit log: {0}")]
    Clog(String),

    #[error("transaction {0:?} is not running")]
    NotRunning(Xid),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, TxnError>;

/// Transaction services used by the access methods and the executor.
///
/// Kept behind a trait so that xid assignment, snapshots, and commit status
/// can later come from a cluster-wide service (DESIGN: ha_and_backup_design,
/// future active-active); today there is one local implementation.
pub trait TransactionManager: Send + Sync + fmt::Debug {
    /// Starts a transaction (no xid yet).
    fn begin(&self) -> TxnId;

    /// Assigns an xid to a transaction about to write.
    fn assign_xid(&self) -> Result<Xid>;

    /// A snapshot of the transactions running now.
    fn snapshot(&self) -> Snapshot;

    /// Commit state of `xid` from the commit log.
    fn status(&self, xid: Xid) -> Result<XidStatus>;

    /// Makes `xid` committed durably, then visible to new snapshots.
    fn commit(&self, xid: Xid) -> Result<()>;

    /// Marks `xid` aborted and removes it from the running set.
    fn abort(&self, xid: Xid) -> Result<()>;
}
