//! A transaction as seen by one session: identity, lazily assigned xid,
//! snapshots per isolation level, command ids, and lock ownership.

use std::sync::Arc;

use super::{CommandId, Snapshot, TransactionManager, TxnError, TxnId, Xid};
use crate::lock::{LockError, LockManager, LockMode, LockTag, WaitPolicy};

/// Supported isolation levels (DESIGN: no SSI).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// A new snapshot per statement; concurrent updates are followed to the
    /// newest version (EvalPlanQual).
    ReadCommitted,
    /// One snapshot for the whole transaction; a concurrent committed update
    /// of a row being changed is a serialization failure (first updater
    /// wins). PostgreSQL's REPEATABLE READ.
    SnapshotIsolation,
}

/// Errors from transaction operations.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error(transparent)]
    Txn(#[from] TxnError),

    #[error(transparent)]
    Lock(#[from] LockError),

    #[error("could not serialize access due to concurrent update")]
    SerializationFailure,

    #[error("transaction is already finished")]
    Finished,
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, TransactionError>;

/// One transaction. Dropping it without commit or abort aborts it.
#[derive(Debug)]
pub struct Transaction {
    tm: Arc<dyn TransactionManager>,
    locks: Arc<LockManager>,
    id: TxnId,
    xid: Option<Xid>,
    cid: CommandId,
    isolation: Isolation,
    snapshot: Option<Snapshot>,
    policy: WaitPolicy,
    finished: bool,
}

impl Transaction {
    pub fn begin(
        tm: Arc<dyn TransactionManager>,
        locks: Arc<LockManager>,
        isolation: Isolation,
        policy: WaitPolicy,
    ) -> Self {
        let id = tm.begin();
        Self {
            tm,
            locks,
            id,
            xid: None,
            cid: CommandId(0),
            isolation,
            snapshot: None,
            policy,
            finished: false,
        }
    }

    pub fn id(&self) -> TxnId {
        self.id
    }

    pub fn isolation(&self) -> Isolation {
        self.isolation
    }

    pub fn tm(&self) -> &dyn TransactionManager {
        self.tm.as_ref()
    }

    /// The xid, if the transaction has written anything.
    pub fn xid(&self) -> Option<Xid> {
        self.xid
    }

    /// The xid, assigning one (and taking the transaction's own lock) on
    /// first write.
    pub fn write_xid(&mut self) -> Result<Xid> {
        if self.finished {
            return Err(TransactionError::Finished);
        }
        if let Some(x) = self.xid {
            return Ok(x);
        }
        let x = self.tm.assign_xid()?;
        // Never waits: nobody else can hold a brand-new xid's lock.
        self.locks.acquire(
            self.id,
            LockTag::Transaction(x),
            LockMode::Exclusive,
            self.policy,
        )?;
        self.xid = Some(x);
        Ok(x)
    }

    pub fn command_id(&self) -> CommandId {
        self.cid
    }

    /// Makes this statement's changes visible to the next statement.
    pub fn next_command(&mut self) {
        self.cid = CommandId(self.cid.0 + 1);
    }

    /// Snapshot for a new statement: fresh under Read Committed, the first
    /// one under Snapshot Isolation.
    pub fn statement_snapshot(&mut self) -> Snapshot {
        match (&self.snapshot, self.isolation) {
            (Some(s), Isolation::SnapshotIsolation) => s.clone(),
            _ => {
                let s = self.tm.snapshot_for(self.id);
                self.snapshot = Some(s.clone());
                s
            }
        }
    }

    /// The snapshot of the current statement (taking one if none yet).
    pub fn snapshot(&mut self) -> Snapshot {
        match &self.snapshot {
            Some(s) => s.clone(),
            None => self.statement_snapshot(),
        }
    }

    /// Waits until transaction `xid` ends (row update conflicts).
    pub fn wait_for(&self, xid: Xid) -> Result<()> {
        Ok(self.locks.wait_for_xid(self.id, xid, self.policy)?)
    }

    /// Takes a table lock until the transaction ends.
    pub fn lock_relation(
        &self,
        rel: crate::storage::backend::RelFileId,
        mode: LockMode,
    ) -> Result<()> {
        Ok(self
            .locks
            .acquire(self.id, LockTag::Relation(rel), mode, self.policy)?)
    }

    pub fn commit(mut self) -> Result<()> {
        self.finish(true)
    }

    pub fn abort(mut self) -> Result<()> {
        self.finish(false)
    }

    fn finish(&mut self, commit: bool) -> Result<()> {
        if self.finished {
            return Err(TransactionError::Finished);
        }
        self.finished = true;
        let res = match self.xid {
            Some(x) if commit => self.tm.commit(x),
            Some(x) => self.tm.abort(x),
            None => Ok(()),
        };
        // Locks go after the outcome is recorded, so waiters see it.
        self.locks.release_all(self.id);
        self.tm.release_snapshot(self.id);
        Ok(res?)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish(false);
        }
    }
}
