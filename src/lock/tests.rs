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
