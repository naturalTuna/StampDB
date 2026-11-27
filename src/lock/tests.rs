use super::*;
use std::sync::Barrier;

const T1: TxnId = TxnId(1);
const T2: TxnId = TxnId(2);
const T3: TxnId = TxnId(3);
const REL: LockTag = LockTag::Relation(RelFileId(100));

fn no_wait() -> WaitPolicy {
    WaitPolicy {
        lock_timeout: Some(Duration::from_millis(30)),
        ..WaitPolicy::default()
    }
}

#[test]
fn conflict_table_matches_postgres() {
    use LockMode::*;
    for a in LockMode::ALL {
        for b in LockMode::ALL {
            assert_eq!(a.conflicts(b), b.conflicts(a), "{a:?} vs {b:?} symmetric");
        }
        assert!(a.conflicts(AccessExclusive));
    }
    assert!(!AccessShare.conflicts(RowExclusive));
    assert!(!RowExclusive.conflicts(RowExclusive));
    assert!(RowExclusive.conflicts(Share));
    assert!(!Share.conflicts(Share));
    assert!(ShareUpdateExclusive.conflicts(ShareUpdateExclusive));
    assert!(!RowShare.conflicts(ShareRowExclusive));
    assert!(Exclusive.conflicts(RowShare));
    assert!(!Exclusive.conflicts(AccessShare));
}

#[test]
fn compatible_modes_share_and_conflicts_time_out() {
    let lm = LockManager::new();
    lm.acquire(T1, REL, LockMode::RowExclusive, no_wait())
        .unwrap();
    lm.acquire(T2, REL, LockMode::RowExclusive, no_wait())
        .unwrap();
    assert_eq!(
        lm.acquire(T3, REL, LockMode::AccessExclusive, no_wait()),
        Err(LockError::Timeout)
    );
    // The timed-out request left the queue: a compatible request is fine.
    lm.acquire(T3, REL, LockMode::AccessShare, no_wait())
        .unwrap();
}

#[test]
fn reacquire_counts_and_release_all() {
    let lm = LockManager::new();
    lm.acquire(T1, REL, LockMode::Share, no_wait()).unwrap();
    lm.acquire(T1, REL, LockMode::Share, no_wait()).unwrap();
    lm.acquire(T1, REL, LockMode::RowExclusive, no_wait())
        .unwrap(); // own modes never conflict
    lm.release(T1, REL, LockMode::Share);
    assert_eq!(
        lm.held(T1, REL),
        vec![LockMode::RowExclusive, LockMode::Share]
    );
    lm.release_all(T1);
    assert!(lm.held(T1, REL).is_empty());
    lm.acquire(T2, REL, LockMode::AccessExclusive, no_wait())
        .unwrap();
}

#[test]
fn release_wakes_waiter() {
    let lm = LockManager::new();
    lm.acquire(T1, REL, LockMode::AccessExclusive, no_wait())
        .unwrap();
    let got = AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            lm.acquire(T2, REL, LockMode::AccessShare, WaitPolicy::default())
                .unwrap();
            got.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(!got.load(Ordering::SeqCst), "still blocked");
        lm.release_all(T1);
    });
    assert!(got.load(Ordering::SeqCst));
    assert_eq!(lm.held(T2, REL), vec![LockMode::AccessShare]);
}

#[test]
fn queued_exclusive_is_not_starved_by_new_shares() {
    let lm = LockManager::new();
    lm.acquire(T1, REL, LockMode::AccessShare, no_wait())
        .unwrap();
    let order = Mutex::new(Vec::new());
    let barrier = Barrier::new(2);
    std::thread::scope(|s| {
        s.spawn(|| {
            barrier.wait();
            lm.acquire(T2, REL, LockMode::AccessExclusive, WaitPolicy::default())
                .unwrap();
            order.lock().push("exclusive");
            lm.release_all(T2);
        });
        barrier.wait();
        std::thread::sleep(Duration::from_millis(30)); // T2 is queued now
        s.spawn(|| {
            // Compatible with T1 but must queue behind T2.
            lm.acquire(T3, REL, LockMode::AccessShare, WaitPolicy::default())
                .unwrap();
            order.lock().push("share");
            lm.release_all(T3);
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(order.lock().is_empty());
        // The holder may still take more locks on what it already holds.
        lm.acquire(T1, REL, LockMode::AccessShare, no_wait())
            .unwrap();
        lm.release_all(T1);
    });
    assert_eq!(*order.lock(), vec!["exclusive", "share"]);
}

#[test]
fn wait_for_transaction_end() {
    let lm = LockManager::new();
    let x = Xid(42);
    lm.acquire(T1, LockTag::Transaction(x), LockMode::Exclusive, no_wait())
        .unwrap();
    assert_eq!(lm.wait_for_xid(T2, x, no_wait()), Err(LockError::Timeout));
    let done = AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            lm.wait_for_xid(T2, x, WaitPolicy::default()).unwrap();
            done.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(30));
        assert!(!done.load(Ordering::SeqCst));
        lm.release_all(T1); // T1 commits
    });
    assert!(done.load(Ordering::SeqCst));
    assert!(lm.held(T2, LockTag::Transaction(x)).is_empty());
    // A finished transaction is waited for without blocking.
    lm.wait_for_xid(T3, Xid(7), no_wait()).unwrap();
}

#[test]
fn many_threads_mutual_exclusion() {
    let lm = LockManager::new();
    let inside = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let (lm, inside) = (&lm, &inside);
            s.spawn(move || {
                for _ in 0..200 {
                    lm.acquire(TxnId(t), REL, LockMode::Exclusive, WaitPolicy::default())
                        .unwrap();
                    assert_eq!(inside.fetch_add(1, Ordering::SeqCst), 0);
                    inside.fetch_sub(1, Ordering::SeqCst);
                    lm.release(TxnId(t), REL, LockMode::Exclusive);
                }
            });
        }
    });
    assert!(lm.table.lock().is_empty(), "entries are cleaned up");
}

fn fast_deadlock() -> WaitPolicy {
    WaitPolicy {
        lock_timeout: Some(Duration::from_secs(10)),
        deadlock_timeout: Duration::from_millis(50),
    }
}

#[test]
fn two_way_deadlock_is_detected() {
    let lm = LockManager::new();
    let a = LockTag::Relation(RelFileId(1));
    let b = LockTag::Relation(RelFileId(2));
    lm.acquire(T1, a.clone(), LockMode::Exclusive, no_wait())
        .unwrap();
    lm.acquire(T2, b.clone(), LockMode::Exclusive, no_wait())
        .unwrap();
    let barrier = Barrier::new(2);
    let results = std::thread::scope(|s| {
        let h1 = s.spawn(|| {
            barrier.wait();
            let r = lm.acquire(T1, b, LockMode::Exclusive, fast_deadlock());
            lm.release_all(T1); // victim or winner: end the transaction
            r
        });
        let h2 = s.spawn(|| {
            barrier.wait();
            let r = lm.acquire(T2, a, LockMode::Exclusive, fast_deadlock());
            lm.release_all(T2);
            r
        });
        [h1.join().unwrap(), h2.join().unwrap()]
    });
    let deadlocks = results
        .iter()
        .filter(|r| **r == Err(LockError::Deadlock))
        .count();
    let oks = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!((deadlocks, oks), (1, 1), "{results:?}");
}

#[test]
fn three_way_cycle_through_transaction_locks() {
    // T1 waits for T2's xid, T2 for T3's xid, T3 for T1's xid.
    let lm = LockManager::new();
    let xs = [Xid(101), Xid(102), Xid(103)];
    let owners = [T1, T2, T3];
    for (o, x) in owners.iter().zip(xs) {
        lm.acquire(*o, LockTag::Transaction(x), LockMode::Exclusive, no_wait())
            .unwrap();
    }
    let barrier = Barrier::new(3);
    let results: Vec<Result<()>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..3)
            .map(|i| {
                let (lm, barrier) = (&lm, &barrier);
                s.spawn(move || {
                    barrier.wait();
                    let r = lm.wait_for_xid(owners[i], xs[(i + 1) % 3], fast_deadlock());
                    lm.release_all(owners[i]);
                    r
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let deadlocks = results
        .iter()
        .filter(|r| **r == Err(LockError::Deadlock))
        .count();
    assert!(deadlocks >= 1, "{results:?}");
    assert!(
        results.iter().any(Result::is_ok),
        "the others proceed: {results:?}"
    );
}

#[test]
fn plain_wait_is_not_a_deadlock() {
    let lm = LockManager::new();
    lm.acquire(T1, REL, LockMode::Exclusive, no_wait()).unwrap();
    std::thread::scope(|s| {
        let h = s.spawn(|| lm.acquire(T2, REL, LockMode::Exclusive, fast_deadlock()));
        std::thread::sleep(Duration::from_millis(150)); // past deadlock_timeout
        lm.release_all(T1);
        assert_eq!(h.join().unwrap(), Ok(()));
    });
}

#[test]
fn holder_upgrade_skips_the_queue_without_deadlock() {
    // T1 holds Share and T2 queues for Exclusive behind it. T1 then asks
    // for Exclusive: no other owner holds the lock, and a holder is not
    // made to wait behind the queue, so T1 proceeds instead of the two
    // waiting for each other.
    let lm = LockManager::new();
    lm.acquire(T1, REL, LockMode::Share, no_wait()).unwrap();
    std::thread::scope(|s| {
        let h2 = s.spawn(|| {
            let r = lm.acquire(T2, REL, LockMode::Exclusive, fast_deadlock());
            lm.release_all(T2);
            r
        });
        std::thread::sleep(Duration::from_millis(20));
        // Upgrade conflicts with no other holder; queue skip applies.
        lm.acquire(T1, REL, LockMode::Exclusive, fast_deadlock())
            .unwrap();
        lm.release_all(T1);
        assert_eq!(h2.join().unwrap(), Ok(()));
    });
}

fn key(k: u8) -> LockTag {
    LockTag::Key {
        index: RelFileId(500),
        key: vec![1, k],
    }
}

#[test]
fn key_locks_share_and_conflict_by_value() {
    let lm = LockManager::new();
    // Children referencing the same parent key share it.
    lm.acquire(T1, key(1), LockMode::Share, no_wait()).unwrap();
    lm.acquire(T2, key(1), LockMode::Share, no_wait()).unwrap();
    // Removing that key waits for them; another key is independent.
    assert_eq!(
        lm.acquire(T3, key(1), LockMode::Exclusive, no_wait()),
        Err(LockError::Timeout)
    );
    lm.acquire(T3, key(2), LockMode::Exclusive, no_wait())
        .unwrap();
    assert_eq!(
        lm.acquire(T1, key(2), LockMode::Share, no_wait()),
        Err(LockError::Timeout)
    );
    assert_eq!(lm.key_locks(), 2, "failed requests leave no entries");
    lm.release_all(T1);
    lm.release_all(T2);
    lm.acquire(T3, key(1), LockMode::Exclusive, no_wait())
        .unwrap();
    lm.release_all(T3);
    assert_eq!(lm.key_locks(), 0);
}

#[test]
fn key_lock_table_is_bounded() {
    let lm = LockManager::with_key_limit(3);
    for k in 0..3 {
        lm.acquire(T1, key(k), LockMode::Share, no_wait()).unwrap();
    }
    // Re-locking a held key needs no new entry.
    lm.acquire(T2, key(0), LockMode::Share, no_wait()).unwrap();
    assert_eq!(
        lm.acquire(T2, key(9), LockMode::Share, no_wait()),
        Err(LockError::OutOfKeyLocks(3))
    );
    // Table and transaction locks are not limited.
    lm.acquire(T2, REL, LockMode::Share, no_wait()).unwrap();
    lm.release(T1, key(1), LockMode::Share);
    lm.acquire(T2, key(9), LockMode::Share, no_wait()).unwrap();
    lm.release_all(T1);
    lm.release_all(T2);
    assert_eq!(lm.key_locks(), 0);
}

#[test]
fn key_lock_deadlocks_are_detected() {
    let lm = LockManager::new();
    lm.acquire(T1, key(1), LockMode::Share, no_wait()).unwrap();
    lm.acquire(T2, key(2), LockMode::Share, no_wait()).unwrap();
    let results = std::thread::scope(|s| {
        let a = s.spawn(|| {
            let r = lm.acquire(T1, key(2), LockMode::Exclusive, fast_deadlock());
            if r.is_err() {
                lm.release_all(T1);
            }
            r
        });
        std::thread::sleep(Duration::from_millis(20));
        let b = lm.acquire(T2, key(1), LockMode::Exclusive, fast_deadlock());
        lm.release_all(T2);
        let a = a.join().unwrap();
        lm.release_all(T1);
        [a, b]
    });
    let deadlocks = results
        .iter()
        .filter(|r| **r == Err(LockError::Deadlock))
        .count();
    assert_eq!(deadlocks, 1, "{results:?}");
}
