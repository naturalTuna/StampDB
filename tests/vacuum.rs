//! VACUUM of a table with a B+Tree index, on the O_DIRECT backend.

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mydb::access::btree::BTree;
use mydb::access::btree::key::encode_key;
use mydb::access::heap::{HeapFile, TmResult};
use mydb::access::tuple::{Tid, TupleDesc, deform_tuple, form_tuple};
use mydb::access::visibility::Me;
use mydb::buffer::SingleModeFlush;
use mydb::lock::{LockManager, WaitPolicy};
use mydb::storage::backend::{FenceToken, FixedFence, RelFileId, StorageBackend};
use mydb::storage::local::LocalDirBackend;
use mydb::txn::{Isolation, Transaction, TransactionManager};
use mydb::types::{DataType, Value};
use mydb::vacuum::vacuum;
use mydb::wal::{Engine, WalConfig, initdb};

const FENCE: FenceToken = FenceToken::new(1);
const HEAP: RelFileId = RelFileId(16384);
const IDX: RelFileId = RelFileId(16385);

fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 1 << 20,
    }
}

fn backend(p: &Path) -> Arc<dyn StorageBackend> {
    Arc::new(LocalDirBackend::open(p).unwrap())
}

struct Env {
    engine: Engine,
    locks: Arc<LockManager>,
    heap: HeapFile,
    idx: BTree,
}

fn start(dir: &Path) -> Env {
    let engine = Engine::start(
        backend(dir),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
        64,
    )
    .unwrap();
    let heap = HeapFile::open(HEAP, engine.pool().clone()).unwrap();
    let idx = BTree::open(IDX, engine.pool().clone()).unwrap();
    Env {
        engine,
        locks: Arc::new(LockManager::new()),
        heap,
        idx,
    }
}

fn create(dir: &Path) -> Env {
    initdb(backend(dir), Arc::new(FixedFence(FENCE)), cfg()).unwrap();
    let engine = Engine::start(
        backend(dir),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
        64,
    )
    .unwrap();
    engine.create_relation(HEAP).unwrap();
    engine.create_relation(IDX).unwrap();
    BTree::create(IDX, engine.pool().clone()).unwrap();
    // Not tied to a commit yet (catalog step): make the DDL durable.
    engine.wal().flush_all();
    drop(engine);
    start(dir)
}

fn desc() -> TupleDesc {
    TupleDesc::new(vec![DataType::Int8, DataType::Text]).unwrap()
}

fn key(id: i64) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&[DataType::Int8], &[Value::Int8(id)], &mut k).unwrap();
    k
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
                deadlock_timeout: Duration::from_millis(100),
            },
        )
    }

    fn row(&self, t: &mut Transaction, id: i64) -> Vec<u8> {
        let xid = t.write_xid().unwrap();
        let pad = Value::Text("v".repeat(150));
        form_tuple(&desc(), &[Value::Int8(id), pad], xid, t.command_id()).unwrap()
    }

    /// Inserts a row and its index entry.
    fn insert(&self, t: &mut Transaction, id: i64) -> Tid {
        let r = self.row(t, id);
        let tid = self.heap.insert(&r).unwrap();
        self.idx.insert(&key(id), tid).unwrap();
        tid
    }

    fn insert_committed(&self, ids: std::ops::Range<i64>) -> Vec<Tid> {
        let mut t = self.begin(Isolation::ReadCommitted);
        let tids = ids.map(|i| self.insert(&mut t, i)).collect();
        t.commit().unwrap();
        tids
    }

    /// Visible ids via a heap scan.
    fn heap_ids(&self, t: &mut Transaction) -> BTreeSet<i64> {
        let snap = t.snapshot();
        let me = Me {
            xid: t.xid(),
            cid: t.command_id(),
        };
        self.heap
            .scan_mvcc(snap, me, t.tm())
            .unwrap()
            .map(|r| id_of(&r.unwrap().1))
            .collect()
    }

    /// Visible ids via the index (entries rechecked against the heap), and
    /// the number of index entries.
    fn index_ids(&self, t: &mut Transaction) -> (BTreeSet<i64>, usize) {
        let snap = t.snapshot();
        let me = Me {
            xid: t.xid(),
            cid: t.command_id(),
        };
        let entries: Vec<(Vec<u8>, Tid)> = self
            .idx
            .scan(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let ids = entries
            .iter()
            .filter_map(|(k, tid)| {
                let row = self.heap.fetch_visible(*tid, &snap, &me, t.tm()).unwrap()?;
                let id = id_of(&row);
                assert_eq!(*k, key(id));
                Some(id)
            })
            .collect();
        (ids, entries.len())
    }

    fn vacuum(&self) -> mydb::vacuum::VacuumStats {
        let mut t = self.begin(Isolation::ReadCommitted);
        let s = vacuum(&self.heap, &[&self.idx], &mut t).unwrap();
        t.commit().unwrap();
        s
    }
}

fn id_of(t: &[u8]) -> i64 {
    match deform_tuple(&desc(), t).unwrap()[0] {
        Value::Int8(i) => i,
        _ => panic!("bad row"),
    }
}

#[test]
fn vacuum_removes_dead_rows_and_index_entries() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let e = create(dir.path());
    let tids = e.insert_committed(0..3000);
    // Delete ids 500..2500 and update every 50th remaining id.
    let mut t = e.begin(Isolation::ReadCommitted);
    for (i, &tid) in tids.iter().enumerate() {
        if (500..2500).contains(&i) {
            assert_eq!(e.heap.delete_mvcc(tid, &mut t).unwrap(), TmResult::Ok(tid));
        } else if i % 50 == 0 {
            let r = e.row(&mut t, i as i64);
            let TmResult::Ok(new) = e.heap.update_mvcc(tid, &r, &mut t).unwrap() else {
                panic!("update failed");
            };
            e.idx.insert(&key(i as i64), new).unwrap();
        }
    }
    // An aborted insert leaves a dead tuple and index entry too.
    let mut ab = e.begin(Isolation::ReadCommitted);
    e.insert(&mut ab, 9_999);
    ab.abort().unwrap();
    t.commit().unwrap();

    let blocks = e.heap.nblocks().unwrap();
    let stats = e.vacuum();
    let updates = (0..3000)
        .filter(|i| !(500..2500).contains(i) && i % 50 == 0)
        .count();
    assert_eq!(stats.tuples_removed, 2000 + updates + 1);
    assert_eq!(stats.index_entries_removed, 2000 + updates + 1);
    assert!(stats.index_pages_deleted > 0, "{stats:?}");

    let want: BTreeSet<i64> = (0..3000).filter(|i| !(500..2500).contains(i)).collect();
    let mut r = e.begin(Isolation::ReadCommitted);
    assert_eq!(e.heap_ids(&mut r), want);
    let (ids, entries) = e.index_ids(&mut r);
    assert_eq!(ids, want);
    assert_eq!(entries, want.len(), "no index entry left for removed rows");
    drop(r);

    // Freed space is reused instead of extending the heap.
    e.insert_committed(10_000..11_500);
    assert_eq!(e.heap.nblocks().unwrap(), blocks);
    assert_eq!(e.vacuum().tuples_removed, 0, "nothing new to remove");
}

#[test]
fn vacuum_keeps_rows_an_old_snapshot_can_see() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let e = create(dir.path());
    let tids = e.insert_committed(0..100);
    let mut old = e.begin(Isolation::SnapshotIsolation);
    assert_eq!(e.heap_ids(&mut old).len(), 100);
    let mut d = e.begin(Isolation::ReadCommitted);
    for &tid in &tids[..50] {
        e.heap.delete_mvcc(tid, &mut d).unwrap();
    }
    d.commit().unwrap();

    assert_eq!(
        e.vacuum().tuples_removed,
        0,
        "still visible to the old snapshot"
    );
    assert_eq!(e.heap_ids(&mut old).len(), 100);
    assert_eq!(e.index_ids(&mut old).0.len(), 100);
    old.commit().unwrap();
    assert_eq!(e.vacuum().tuples_removed, 50);
    let mut r = e.begin(Isolation::ReadCommitted);
    assert_eq!(e.heap_ids(&mut r).len(), 50);
}

#[test]
fn vacuum_with_concurrent_updates() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let e = create(dir.path());
    e.insert_committed(0..200);
    std::thread::scope(|s| {
        s.spawn(|| {
            // Rewrite every row a few times, one transaction per row.
            for _ in 0..3 {
                for id in 0..200 {
                    let mut t = e.begin(Isolation::ReadCommitted);
                    t.statement_snapshot();
                    let tid = e.index_ids_with_tid(&mut t, id).expect("row exists");
                    let r = e.row(&mut t, id);
                    match e.heap.update_waiting(tid, &r, &mut t).unwrap() {
                        TmResult::Ok(new) => {
                            e.idx.insert(&key(id), new).unwrap();
                            t.commit().unwrap();
                        }
                        other => panic!("unexpected {other:?}"),
                    }
                }
            }
        });
        for _ in 0..5 {
            e.vacuum();
        }
    });
    e.vacuum();
    let mut r = e.begin(Isolation::ReadCommitted);
    let want: BTreeSet<i64> = (0..200).collect();
    assert_eq!(e.heap_ids(&mut r), want);
    let (ids, entries) = e.index_ids(&mut r);
    assert_eq!(ids, want);
    assert_eq!(entries, 200, "only live versions remain indexed");
}

impl Env {
    /// TID of the visible version of `id`, found through the index.
    fn index_ids_with_tid(&self, t: &mut Transaction, id: i64) -> Option<Tid> {
        let snap = t.snapshot();
        let me = Me {
            xid: t.xid(),
            cid: t.command_id(),
        };
        self.idx
            .scan(Bound::Included(key(id)), Bound::Included(key(id)))
            .unwrap()
            .map(|r| r.unwrap().1)
            .find(|tid| {
                self.heap
                    .fetch_visible(*tid, &snap, &me, t.tm())
                    .unwrap()
                    .is_some()
            })
    }
}

#[test]
fn vacuum_survives_crash() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let want: BTreeSet<i64> = (0..300).filter(|i| i % 3 != 0).collect();
    {
        let e = create(dir.path());
        let tids = e.insert_committed(0..300);
        let mut d = e.begin(Isolation::ReadCommitted);
        for (i, &tid) in tids.iter().enumerate() {
            if i % 3 == 0 {
                e.heap.delete_mvcc(tid, &mut d).unwrap();
            }
        }
        d.commit().unwrap();
        e.vacuum();
        e.engine.wal().flush_all();
        // dropped without shutdown: crash
    }
    let e = start(dir.path());
    let mut r = e.begin(Isolation::ReadCommitted);
    assert_eq!(e.heap_ids(&mut r), want);
    let (ids, entries) = e.index_ids(&mut r);
    assert_eq!(ids, want);
    assert_eq!(entries, want.len());
}
