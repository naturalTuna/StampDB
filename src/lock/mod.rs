//! Transaction-level lock manager (heavyweight locks).
//!
//! Every transaction-level wait goes through here (DESIGN: lock manager),
//! so that deadlocks can be detected: table locks, and waiting for another
//! transaction to end (a transaction holds its own transaction lock in
//! Exclusive mode from xid assignment to commit/abort; waiting for it is
//! taking that lock in Share mode, as in PostgreSQL's XactLockTableWait).
//!
//! Locks are owned by [`TxnId`]s and released together at transaction end.
//! Requests are granted if they conflict neither with modes held by other
//! owners nor with earlier waiters (FIFO), unless the owner already holds
//! the lock. On release, waiters are granted in queue order as far as
//! possible (PostgreSQL's ProcLockWakeup).
//!
//! A wait that lasts `deadlock_timeout` checks the wait-for graph once; if
//! the waiter is in a cycle, its request fails with [`LockError::Deadlock`]
//! (PostgreSQL's deadlock_timeout and DeadLockCheck; the requester is the
//! victim). Latches are never part of this graph (DESIGN).
//!
//! The table is one mutex with one condition variable; PostgreSQL
//! partitions it 16 ways. Deadlock detection needs a consistent view of all
//! waits, which one mutex gives directly.

mod mode;

pub use mode::LockMode;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::storage::backend::RelFileId;
use crate::txn::{TxnId, Xid};

/// What a lock protects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockTag {
    /// A table (or index).
    Relation(RelFileId),
    /// A transaction, held by itself until it ends.
    Transaction(Xid),
}

/// Errors from lock acquisition.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LockError {
    #[error("canceling statement due to lock timeout")]
    Timeout,

    #[error("deadlock detected")]
    Deadlock,
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, LockError>;

/// Waiting policy for one request.
#[derive(Debug, Clone, Copy)]
pub struct WaitPolicy {
    /// Give up after this long (`None`: wait forever; PostgreSQL's
    /// lock_timeout = 0).
    pub lock_timeout: Option<Duration>,
    /// Check for deadlocks once a wait has lasted this long.
    pub deadlock_timeout: Duration,
}

impl Default for WaitPolicy {
    fn default() -> Self {
        Self {
            lock_timeout: None,
            deadlock_timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Debug)]
struct Waiter {
    owner: TxnId,
    mode: LockMode,
    granted: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct LockState {
    /// Granted modes per owner, with acquisition counts.
    holders: HashMap<TxnId, [u32; LockMode::COUNT]>,
    waiters: VecDeque<Waiter>,
}

impl LockState {
    fn holds_any(&self, owner: TxnId) -> bool {
        self.holders.contains_key(&owner)
    }

    /// Whether `mode` conflicts with a mode held by an owner other than
    /// `owner`.
    fn conflicts_with_holders(&self, owner: TxnId, mode: LockMode) -> bool {
        self.holders.iter().any(|(o, counts)| {
            *o != owner
                && LockMode::ALL
                    .iter()
                    .any(|m| counts[m.index()] > 0 && mode.conflicts(*m))
        })
    }

    fn grant(&mut self, owner: TxnId, mode: LockMode) {
        self.holders.entry(owner).or_insert([0; LockMode::COUNT])[mode.index()] += 1;
    }

    fn is_empty(&self) -> bool {
        self.holders.is_empty() && self.waiters.is_empty()
    }

    /// Grants waiters in queue order while they are compatible with the
    /// holders and with the waiters still ahead of them.
    fn wake(&mut self) -> bool {
        let mut woke = false;
        let mut still_waiting: Vec<LockMode> = Vec::new();
        let mut i = 0;
        while i < self.waiters.len() {
            let w = &self.waiters[i];
            let blocked_by_queue = still_waiting.iter().any(|m| w.mode.conflicts(*m));
            if !blocked_by_queue && !self.conflicts_with_holders(w.owner, w.mode) {
                if let Some(w) = self.waiters.remove(i) {
                    w.granted.store(true, Ordering::SeqCst);
                    self.grant(w.owner, w.mode);
                    woke = true;
                }
            } else {
                still_waiting.push(w.mode);
                i += 1;
            }
        }
        woke
    }
}

/// The lock manager.
#[derive(Debug, Default)]
pub struct LockManager {
    table: Mutex<HashMap<LockTag, LockState>>,
    cv: Condvar,
}

impl LockManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquires `tag` in `mode` for `owner`, waiting as `policy` allows.
    pub fn acquire(
        &self,
        owner: TxnId,
        tag: LockTag,
        mode: LockMode,
        policy: WaitPolicy,
    ) -> Result<()> {
        let mut table = self.table.lock();
        let state = table.entry(tag).or_default();
        let ahead_conflict =
            !state.holds_any(owner) && state.waiters.iter().any(|w| mode.conflicts(w.mode));
        if !ahead_conflict && !state.conflicts_with_holders(owner, mode) {
            state.grant(owner, mode);
            return Ok(());
        }

        let granted = Arc::new(AtomicBool::new(false));
        state.waiters.push_back(Waiter {
            owner,
            mode,
            granted: granted.clone(),
        });
        let start = Instant::now();
        let deadline = policy.lock_timeout.map(|t| start + t);
        let mut deadlock_check = Some(start + policy.deadlock_timeout);
        loop {
            if granted.load(Ordering::SeqCst) {
                return Ok(());
            }
            let now = Instant::now();
            let failure = if deadline.is_some_and(|d| now >= d) {
                Some(LockError::Timeout)
            } else if deadlock_check.is_some_and(|d| now >= d) {
                // Checked once, like PostgreSQL: a cycle completed later is
                // found by the waiter whose wait completes it.
                deadlock_check = None;
                Self::in_cycle(&table, owner).then_some(LockError::Deadlock)
            } else {
                None
            };
            if let Some(err) = failure {
                Self::cancel_wait(&mut table, tag, &granted);
                // Our departure may unblock waiters queued behind us.
                if table.get_mut(&tag).is_some_and(LockState::wake) {
                    self.cv.notify_all();
                }
                return if granted.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(err)
                };
            }
            match [deadline, deadlock_check].into_iter().flatten().min() {
                Some(d) => {
                    self.cv.wait_until(&mut table, d);
                }
                None => self.cv.wait(&mut table),
            }
        }
    }

    /// Whether `me` waits in a cycle of the wait-for graph. An owner waits
    /// for every other owner holding a conflicting mode on the lock it is
    /// queued for, and for every conflicting waiter queued ahead of it.
    fn in_cycle(table: &HashMap<LockTag, LockState>, me: TxnId) -> bool {
        let mut edges: HashMap<TxnId, Vec<TxnId>> = HashMap::new();
        for state in table.values() {
            for (i, w) in state.waiters.iter().enumerate() {
                let out = edges.entry(w.owner).or_default();
                for (holder, counts) in &state.holders {
                    if *holder != w.owner
                        && LockMode::ALL
                            .iter()
                            .any(|m| counts[m.index()] > 0 && w.mode.conflicts(*m))
                    {
                        out.push(*holder);
                    }
                }
                for ahead in state.waiters.iter().take(i) {
                    if ahead.owner != w.owner && w.mode.conflicts(ahead.mode) {
                        out.push(ahead.owner);
                    }
                }
            }
        }
        // Depth-first search for a path from `me` back to `me`.
        let mut stack: Vec<TxnId> = edges.get(&me).cloned().unwrap_or_default();
        let mut seen = std::collections::HashSet::new();
        while let Some(t) = stack.pop() {
            if t == me {
                return true;
            }
            if seen.insert(t)
                && let Some(next) = edges.get(&t)
            {
                stack.extend(next.iter().copied());
            }
        }
        false
    }

    /// Removes our queued request (if not granted meanwhile).
    fn cancel_wait(
        table: &mut HashMap<LockTag, LockState>,
        tag: LockTag,
        granted: &Arc<AtomicBool>,
    ) {
        if let Some(state) = table.get_mut(&tag) {
            state.waiters.retain(|w| !Arc::ptr_eq(&w.granted, granted));
        }
    }

    /// Releases one acquisition of `tag` in `mode`.
    pub fn release(&self, owner: TxnId, tag: LockTag, mode: LockMode) {
        let mut table = self.table.lock();
        let Some(state) = table.get_mut(&tag) else {
            return;
        };
        if let Some(counts) = state.holders.get_mut(&owner) {
            let c = &mut counts[mode.index()];
            *c = c.saturating_sub(1);
            if counts.iter().all(|&c| c == 0) {
                state.holders.remove(&owner);
            }
        }
        let woke = state.wake();
        if state.is_empty() {
            table.remove(&tag);
        }
        if woke {
            self.cv.notify_all();
        }
    }

    /// Releases every lock held by `owner` (at transaction end).
    pub fn release_all(&self, owner: TxnId) {
        let mut table = self.table.lock();
        let mut woke = false;
        table.retain(|_, state| {
            if state.holders.remove(&owner).is_some() {
                woke |= state.wake();
            }
            !state.is_empty()
        });
        if woke {
            self.cv.notify_all();
        }
    }

    /// Waits until transaction `xid` has ended (its lock is free).
    pub fn wait_for_xid(&self, owner: TxnId, xid: Xid, policy: WaitPolicy) -> Result<()> {
        let tag = LockTag::Transaction(xid);
        self.acquire(owner, tag, LockMode::Share, policy)?;
        self.release(owner, tag, LockMode::Share);
        Ok(())
    }

    /// Modes `owner` holds on `tag` (diagnostics and tests).
    pub fn held(&self, owner: TxnId, tag: LockTag) -> Vec<LockMode> {
        let table = self.table.lock();
        table
            .get(&tag)
            .and_then(|s| s.holders.get(&owner))
            .map(|counts| {
                LockMode::ALL
                    .iter()
                    .copied()
                    .filter(|m| counts[m.index()] > 0)
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests;
