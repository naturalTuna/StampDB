//! Running-transaction set and xid assignment.
//!
//! Assigning an xid and adding it to the running set happen under one lock,
//! as does taking a snapshot, so a snapshot sees every assigned xid either
//! as running or (once removed) as finished — never half-registered.
//! Commit removes the xid only after its commit is durable and recorded in
//! the commit log, so a snapshot cannot see a transaction as finished
//! before the commit log knows its outcome.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::{Snapshot, TxnId, Xid};

#[derive(Debug)]
struct State {
    next_xid: Xid,
    running: BTreeSet<Xid>,
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
        let s = self.state.lock();
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
        assert_eq!(pa.next_xid(), Xid(2001));
        assert_eq!(pa.snapshot().running.len(), 1000);
    }
}
