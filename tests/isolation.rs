//! Isolation scenarios with concurrent sessions (in the spirit of
//! PostgreSQL's isolation tester), on the O_DIRECT backend.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mydb::access::heap::{HeapError, HeapFile, TmResult};
use mydb::access::tuple::{Tid, TupleDesc, deform_tuple, form_tuple};
use mydb::access::visibility::Me;
use mydb::buffer::SingleModeFlush;
use mydb::lock::{LockError, LockManager, WaitPolicy};
use mydb::storage::backend::{FenceToken, FixedFence, RelFileId, StorageBackend};
use mydb::storage::local::LocalDirBackend;
use mydb::txn::{Isolation, Transaction, TransactionError, TransactionManager};
use mydb::types::{DataType, Value};
use mydb::wal::{Engine, WalConfig, initdb};

const FENCE: FenceToken = FenceToken::new(1);
const REL: RelFileId = RelFileId(16384);

struct Env {
    _dir: tempfile::TempDir,
    engine: Engine,
    locks: Arc<LockManager>,
    heap: HeapFile,
}

fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 1 << 20,
    }
}

fn env() -> Env {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let be = |p: &Path| -> Arc<dyn StorageBackend> { Arc::new(LocalDirBackend::open(p).unwrap()) };
    initdb(be(dir.path()), Arc::new(FixedFence(FENCE)), cfg()).unwrap();
    let engine = Engine::start(
        be(dir.path()),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
        128,
    )
    .unwrap();
    engine.create_relation(REL).unwrap();
    let heap = HeapFile::open(REL, engine.pool().clone()).unwrap();
    Env {
        _dir: dir,
        engine,
        locks: Arc::new(LockManager::new()),
        heap,
    }
}

fn desc() -> TupleDesc {
    TupleDesc::new(vec![DataType::Int8, DataType::Int8]).unwrap()
}

impl Env {
    fn begin(&self, iso: Isolation) -> Transaction {
        let tm: Arc<dyn TransactionManager> = self.engine.txn().clone();
        Transaction::begin(
            tm,
            self.locks.clone(),
            iso,
            WaitPolicy {
                lock_timeout: Some(Duration::from_secs(30)),
                deadlock_timeout: Duration::from_millis(50),
            },
        )
    }

    /// Row `(key, value)` written by `txn`'s current command.
    fn row(&self, txn: &mut Transaction, key: i64, value: i64) -> Vec<u8> {
        let xid = txn.write_xid().unwrap();
        form_tuple(
            &desc(),
            &[Value::Int8(key), Value::Int8(value)],
            xid,
            txn.command_id(),
        )
        .unwrap()
    }

    fn insert_committed(&self, key: i64, value: i64) {
        let mut t = self.begin(Isolation::ReadCommitted);
        let r = self.row(&mut t, key, value);
        self.heap.insert(&r).unwrap();
        t.commit().unwrap();
    }

    /// Visible rows `key -> (tid, value)` for `txn`'s current snapshot.
    fn read(&self, txn: &mut Transaction) -> BTreeMap<i64, (Tid, i64)> {
        let snap = txn.snapshot();
        let me = Me {
            xid: txn.xid(),
            cid: txn.command_id(),
        };
        self.heap
            .scan_mvcc(snap, me, txn.tm())
            .unwrap()
            .map(|r| {
                let (tid, t) = r.unwrap();
                let v = deform_tuple(&desc(), &t).unwrap();
                match (&v[0], &v[1]) {
                    (Value::Int8(k), Value::Int8(x)) => (*k, (tid, *x)),
                    _ => panic!("bad row"),
                }
            })
            .collect()
    }

    fn value_at(&self, tid: Tid) -> i64 {
        let t = self.heap.fetch(tid).unwrap().unwrap();
        match deform_tuple(&desc(), &t).unwrap()[1] {
            Value::Int8(x) => x,
            _ => panic!("bad row"),
        }
    }

    /// Adds one to the value of `key`. Under Read Committed a concurrent
    /// update is followed to the newest version and re-applied
    /// (EvalPlanQual); under Snapshot Isolation a serialization failure
    /// restarts the transaction. Returns the number of restarts.
    fn increment(&self, iso: Isolation, key: i64) -> usize {
        let mut restarts = 0;
        'txn: loop {
            let mut t = self.begin(iso);
            t.statement_snapshot();
            let (mut tid, mut value) = self.read(&mut t)[&key];
            loop {
                let new = self.row(&mut t, key, value + 1);
                match self.heap.update_waiting(tid, &new, &mut t) {
                    Ok(TmResult::Ok(_)) => {
                        t.commit().unwrap();
                        return restarts;
                    }
                    Ok(TmResult::Updated { .. }) => {
                        tid = self.heap.latest_version(tid, t.tm()).unwrap().unwrap();
                        value = self.value_at(tid);
                    }
                    Err(HeapError::Transaction(TransactionError::SerializationFailure)) => {
                        t.abort().unwrap();
                        restarts += 1;
                        continue 'txn;
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
    }
}

fn run_counter(iso: Isolation) -> usize {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 60;
    let e = env();
    e.insert_committed(1, 0);
    let restarts = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                for _ in 0..PER_THREAD {
                    restarts.fetch_add(e.increment(iso, 1), Ordering::SeqCst);
                }
            });
        }
    });
    let mut r = e.begin(Isolation::ReadCommitted);
    let rows = e.read(&mut r);
    assert_eq!(rows.len(), 1, "one live version");
    assert_eq!(rows[&1].1, (THREADS * PER_THREAD) as i64, "no lost updates");
    restarts.load(Ordering::SeqCst)
}

#[test]
fn read_committed_has_no_lost_updates() {
    assert_eq!(
        run_counter(Isolation::ReadCommitted),
        0,
        "RC never restarts"
    );
}

#[test]
fn snapshot_isolation_retries_on_conflict() {
    let restarts = run_counter(Isolation::SnapshotIsolation);
    assert!(restarts > 0, "concurrent increments must conflict under SI");
}

#[test]
fn crossing_updates_deadlock_one_victim() {
    let e = env();
    e.insert_committed(1, 0);
    e.insert_committed(2, 0);
    let mut setup = e.begin(Isolation::ReadCommitted);
    let rows = e.read(&mut setup);
    let (a, b) = (rows[&1].0, rows[&2].0);
    drop(setup);

    let barrier = std::sync::Barrier::new(2);
    let results: Vec<String> = std::thread::scope(|s| {
        let run = |first: Tid, second: Tid, key1: i64, key2: i64| {
            let (e, barrier) = (&e, &barrier);
            s.spawn(move || {
                let mut t = e.begin(Isolation::ReadCommitted);
                let r1 = e.row(&mut t, key1, 1);
                assert!(matches!(
                    e.heap.update_waiting(first, &r1, &mut t),
                    Ok(TmResult::Ok(_))
                ));
                barrier.wait();
                let r2 = e.row(&mut t, key2, 1);
                match e.heap.update_waiting(second, &r2, &mut t) {
                    Ok(TmResult::Ok(_)) => {
                        t.commit().unwrap();
                        "committed".to_string()
                    }
                    Err(HeapError::Transaction(TransactionError::Lock(LockError::Deadlock))) => {
                        t.abort().unwrap();
                        "deadlock".to_string()
                    }
                    other => format!("{other:?}"),
                }
            })
        };
        let h1 = run(a, b, 1, 2);
        let h2 = run(b, a, 2, 1);
        vec![h1.join().unwrap(), h2.join().unwrap()]
    });
    let mut sorted = results.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["committed", "deadlock"], "{results:?}");
}

#[test]
fn snapshot_isolation_allows_write_skew() {
    // Two "on call" rows; each transaction checks that both are on call and
    // takes itself off. SI (no SSI, by design) lets both commit.
    let e = env();
    e.insert_committed(1, 1);
    e.insert_committed(2, 1);
    let mut t1 = e.begin(Isolation::SnapshotIsolation);
    let mut t2 = e.begin(Isolation::SnapshotIsolation);
    let r1 = e.read(&mut t1);
    let r2 = e.read(&mut t2);
    assert_eq!(r1.values().map(|v| v.1).sum::<i64>(), 2);
    assert_eq!(r2.values().map(|v| v.1).sum::<i64>(), 2);
    let n1 = e.row(&mut t1, 1, 0);
    assert!(matches!(
        e.heap.update_waiting(r1[&1].0, &n1, &mut t1),
        Ok(TmResult::Ok(_))
    ));
    let n2 = e.row(&mut t2, 2, 0);
    assert!(matches!(
        e.heap.update_waiting(r2[&2].0, &n2, &mut t2),
        Ok(TmResult::Ok(_))
    ));
    t1.commit().unwrap();
    t2.commit().unwrap();
    let mut r = e.begin(Isolation::ReadCommitted);
    assert_eq!(e.read(&mut r).values().map(|v| v.1).sum::<i64>(), 0);
}

#[test]
fn repeatable_reads_only_under_snapshot_isolation() {
    let e = env();
    e.insert_committed(1, 10);
    let mut rc = e.begin(Isolation::ReadCommitted);
    let mut si = e.begin(Isolation::SnapshotIsolation);
    rc.statement_snapshot();
    si.statement_snapshot();
    assert_eq!(e.read(&mut rc)[&1].1, 10);
    assert_eq!(e.read(&mut si)[&1].1, 10);
    e.increment(Isolation::ReadCommitted, 1); // another session commits 11
    rc.statement_snapshot();
    si.statement_snapshot();
    assert_eq!(
        e.read(&mut rc)[&1].1,
        11,
        "RC: next statement sees the commit"
    );
    assert_eq!(e.read(&mut si)[&1].1, 10, "SI: still the first snapshot");
}
