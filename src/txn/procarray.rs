//! Running-transaction set and xid assignment.
//!
//! Assigning an xid and adding it to the running set happen under one lock,
//! as does taking a snapshot, so a snapshot sees every assigned xid either
//! as running or (once removed) as finished — never half-registered.
//! Commit removes the xid only after its commit is durable and recorded in
//! the commit log, so a snapshot cannot see a transaction as finished
//! before the commit log knows its outcome.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::{Snapshot, TxnId, Xid};

#[derive(Debug)]
struct State {
    next_xid: Xid,
    running: BTreeSet<Xid>,
    /// xmin of the snapshot each transaction is using (PostgreSQL's
    /// MyProc->xmin): tuples deleted before it may still be visible to it.
    xmins: HashMap<TxnId, Xid>,
}

/// The set of running transactions.
#[derive(Debug)]
pub struct ProcArray {
    state: Mutex<State>,
    next_txn: AtomicU64,
}

impl ProcArray {
    /// Starts handing out xids from `next_xid`.
    pub fn new(next_xid: Xid) -> Self {
        Self {
            state: Mutex::new(State {
                next_xid,
                running: BTreeSet::new(),
                xmins: HashMap::new(),
            }),
            next_txn: AtomicU64::new(1),
        }
    }

    /// A new transaction identity.
    pub fn new_txn_id(&self) -> TxnId {
        TxnId(self.next_txn.fetch_add(1, Ordering::Relaxed))
    }

    /// Assigns the next xid and registers it as running.
    pub fn assign_xid(&self) -> Xid {
        let mut s = self.state.lock();
        let xid = s.next_xid;
        s.next_xid = xid.next();
        s.running.insert(xid);
        xid
    }

    /// Removes a finished transaction. Returns whether it was running.
    pub fn remove(&self, xid: Xid) -> bool {
        self.state.lock().running.remove(&xid)
    }

    pub fn is_running(&self, xid: Xid) -> bool {
        self.state.lock().running.contains(&xid)
    }

    /// The next xid to be assigned.
    pub fn next_xid(&self) -> Xid {
        self.state.lock().next_xid
    }

    /// Takes a snapshot of the running set.
    pub fn snapshot(&self) -> Snapshot {
        Self::take(&self.state.lock())
    }

    /// Takes a snapshot for `txn` and registers its xmin, under the same
    /// lock, so a concurrent vacuum horizon cannot pass it.
    pub fn snapshot_for(&self, txn: TxnId) -> Snapshot {
        let mut s = self.state.lock();
        let snap = Self::take(&s);
        s.xmins.insert(txn, snap.xmin);
        snap
    }

    /// Forgets `txn`'s registered snapshot (at transaction end).
    pub fn release_snapshot(&self, txn: TxnId) {
        self.state.lock().xmins.remove(&txn);
    }

    /// The oldest xid any running transaction or registered snapshot may
    /// still consider in progress. A tuple deleted by a transaction that
    /// committed before it is dead to everyone (vacuum horizon).
    pub fn oldest_xmin(&self) -> Xid {
        let s = self.state.lock();
        let mut oldest = s.next_xid;
        if let Some(&x) = s.running.first() {
            oldest = oldest.min(x);
        }
        if let Some(&x) = s.xmins.values().min() {
            oldest = oldest.min(x);
        }
        oldest
    }

    fn take(s: &State) -> Snapshot {
        let running: Vec<Xid> = s.running.iter().copied().collect();
        Snapshot {
            xmin: running.first().copied().unwrap_or(s.next_xid),
            xmax: s.next_xid,
            running,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn assign_snapshot_remove() {
        let pa = ProcArray::new(Xid(10));
        let a = pa.assign_xid();
        let b = pa.assign_xid();
        assert_eq!((a, b), (Xid(10), Xid(11)));
        let s1 = pa.snapshot();
        assert_eq!((s1.xmin, s1.xmax), (Xid(10), Xid(12)));
        assert!(s1.is_running(a) && s1.is_running(b));

        assert!(pa.remove(a));
        assert!(!pa.remove(a));
        let s2 = pa.snapshot();
        assert!(!s2.is_running(a), "finished before the snapshot");
        assert!(s2.is_running(b));
        assert!(s1.is_running(a), "older snapshot is unchanged");

        pa.remove(b);
        let s3 = pa.snapshot();
        assert_eq!((s3.xmin, s3.xmax), (Xid(12), Xid(12)));
        assert!(s3.running.is_empty());
    }

    #[test]
    fn oldest_xmin_follows_running_and_registered_snapshots() {
        let pa = ProcArray::new(Xid(10));
        assert_eq!(pa.oldest_xmin(), Xid(10));
        let a = pa.assign_xid(); // 10
        let reader = TxnId(99);
        let snap = pa.snapshot_for(reader);
        assert_eq!(snap.xmin, a);
        pa.remove(a);
        let b = pa.assign_xid(); // 11
        assert_eq!(
            pa.oldest_xmin(),
            a,
            "the reader's snapshot still sees 10 running"
        );
        pa.release_snapshot(reader);
        assert_eq!(pa.oldest_xmin(), b);
        pa.remove(b);
        assert_eq!(pa.oldest_xmin(), Xid(12));
    }

    #[test]
    fn txn_ids_are_unique() {
        let pa = ProcArray::new(Xid::FIRST);
        let ids: HashSet<TxnId> = (0..100).map(|_| pa.new_txn_id()).collect();
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn concurrent_assign_and_snapshots_are_consistent() {
        let pa = ProcArray::new(Xid::FIRST);
        let mut all: Vec<Xid> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    s.spawn(|| {
                        (0..500)
                            .map(|i| {
                                let x = pa.assign_xid();
                                if i % 2 == 0 {
                                    pa.remove(x);
                                }
                                x
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            s.spawn(|| {
                for _ in 0..2000 {
                    let snap = pa.snapshot();
                    assert!(snap.running.windows(2).all(|w| w[0] < w[1]));
                    assert!(
                        snap.running
                            .iter()
                            .all(|&x| snap.xmin <= x && x < snap.xmax)
                    );
                }
            });
            workers
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 2000, "xids are unique");
        assert_eq!(pa.next_xid(), Xid(Xid::FIRST.0 + 2000));
        assert_eq!(pa.snapshot().running.len(), 1000);
    }
}
