//! System catalogs on the O_DIRECT backend: DDL across a crash, and
//! vacuum of the catalogs.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mydb::buffer::SingleModeFlush;
use mydb::catalog::{Catalog, Column, initdb};
use mydb::lock::{LockManager, WaitPolicy};
use mydb::storage::backend::{FenceToken, StorageBackend};
use mydb::storage::local::LocalDirBackend;
use mydb::txn::{Isolation, Transaction, TransactionManager};
use mydb::types::DataType;
use mydb::wal::{Engine, WalConfig};

const FENCE: FenceToken = FenceToken::new(1);

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
    cat: Catalog,
    locks: Arc<LockManager>,
}

fn start(dir: &Path) -> Env {
    let engine = Engine::start(
        backend(dir),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
        64,
    )
    .unwrap();
    Env {
        cat: Catalog::open(Arc::new(engine)).unwrap(),
        locks: Arc::new(LockManager::new()),
    }
}

impl Env {
    fn begin(&self) -> Transaction {
        let tm: Arc<dyn TransactionManager> = self.cat.engine().txn().clone();
        Transaction::begin(
            tm,
            self.locks.clone(),
            Isolation::ReadCommitted,
            WaitPolicy {
                lock_timeout: Some(Duration::from_secs(10)),
                deadlock_timeout: Duration::from_millis(100),
            },
        )
    }

    fn exists(&self, name: &str) -> bool {
        let mut t = self.begin();
        self.cat.lookup_relation(&mut t, name).unwrap().is_some()
    }
}

fn cols(n: usize) -> Vec<Column> {
    (0..n)
        .map(|i| Column {
            name: format!("c{i}"),
            ty: if i % 2 == 0 {
                DataType::Int8
            } else {
                DataType::Text
            },
            not_null: i == 0,
        })
        .collect()
}

#[test]
fn ddl_is_exact_after_crash() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    initdb(
        backend(dir.path()),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
    )
    .unwrap();
    {
        let e = start(dir.path());
        let mut t = e.begin();
        for i in 0..10 {
            e.cat
                .create_table(&mut t, &format!("t{i}"), &cols(3))
                .unwrap();
            e.cat
                .create_index(&mut t, &format!("t{i}_c0"), &format!("t{i}"), &["c0"], true)
                .unwrap();
        }
        t.commit().unwrap();
        let mut t = e.begin();
        for i in 0..5 {
            e.cat.drop_table(&mut t, &format!("t{i}")).unwrap();
        }
        t.commit().unwrap();
        let mut open = e.begin();
        e.cat
            .create_table(&mut open, "uncommitted", &cols(2))
            .unwrap();
        e.cat.drop_table(&mut open, "t9").unwrap();
        std::mem::forget(open); // crash mid-transaction
    }
    let e = start(dir.path());
    for i in 0..5 {
        assert!(!e.exists(&format!("t{i}")) && !e.exists(&format!("t{i}_c0")));
    }
    for i in 5..10 {
        assert!(e.exists(&format!("t{i}")) && e.exists(&format!("t{i}_c0")));
    }
    assert!(!e.exists("uncommitted"));
    let mut t = e.begin();
    let t9 = e.cat.lookup_relation(&mut t, "t9").unwrap().unwrap();
    assert_eq!(t9.columns, cols(3));
    assert_eq!(e.cat.indexes_of(&mut t, t9.oid).unwrap().len(), 1);
}

#[test]
fn catalog_vacuum_after_ddl_churn() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    initdb(
        backend(dir.path()),
        Arc::new(SingleModeFlush::new(FENCE)),
        cfg(),
    )
    .unwrap();
    let e = start(dir.path());
    for round in 0..40 {
        let mut t = e.begin();
        e.cat.create_table(&mut t, "tmp", &cols(4)).unwrap();
        e.cat
            .create_index(&mut t, "tmp_idx", "tmp", &["c1"], false)
            .unwrap();
        if round % 3 == 0 {
            t.abort().unwrap();
            continue;
        }
        t.commit().unwrap();
        let mut t = e.begin();
        e.cat.drop_table(&mut t, "tmp").unwrap();
        t.commit().unwrap();
    }
    let mut t = e.begin();
    let stats = e.cat.vacuum_system(&mut t).unwrap();
    t.commit().unwrap();
    // Each round left a sys_class row per relation and 4 attribute rows
    // (plus a sys_index row), all dead now.
    assert!(stats.tuples_removed >= 40 * (2 + 4 + 1), "{stats:?}");
    // sys_class rows have two index entries each (oid and relname).
    assert!(
        stats.index_entries_removed > stats.tuples_removed,
        "{stats:?}"
    );
    assert!(e.exists("sys_class") && !e.exists("tmp"));
    let mut t = e.begin();
    e.cat.create_table(&mut t, "tmp", &cols(1)).unwrap();
    t.commit().unwrap();
    assert!(e.exists("tmp"));
}
