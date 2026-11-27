use super::*;
use crate::buffer::SingleModeFlush;
use crate::lock::{LockManager, WaitPolicy};
use crate::storage::backend::FenceToken;
use crate::storage::mem::MemBackend;
use crate::txn::{Isolation, TransactionManager};

const FENCE: FenceToken = FenceToken::new(1);

pub(super) fn cfg() -> WalConfig {
    WalConfig {
        timeline: 1,
        segment_size: 256 * 1024,
    }
}

pub(super) struct Env {
    pub mem: Arc<MemBackend>,
    pub cat: Catalog,
    pub locks: Arc<LockManager>,
}

impl Env {
    pub fn begin(&self) -> Transaction {
        let tm: Arc<dyn TransactionManager> = self.cat.engine().txn().clone();
        Transaction::begin(
            tm,
            self.locks.clone(),
            Isolation::ReadCommitted,
            WaitPolicy {
                lock_timeout: Some(std::time::Duration::from_secs(10)),
                deadlock_timeout: std::time::Duration::from_millis(100),
            },
        )
    }
}

pub(super) fn start(mem: &Arc<MemBackend>) -> Env {
    let engine = Arc::new(
        Engine::start(
            mem.clone(),
            Arc::new(SingleModeFlush::new(FENCE)),
            cfg(),
            64,
        )
        .unwrap(),
    );
    Env {
        mem: mem.clone(),
        cat: Catalog::open(engine).unwrap(),
        locks: Arc::new(LockManager::new()),
    }
}

pub(super) fn fresh() -> Env {
    let mem = Arc::new(MemBackend::default());
    initdb(mem.clone(), Arc::new(SingleModeFlush::new(FENCE)), cfg()).unwrap();
    start(&mem)
}

#[test]
fn bootstrap_describes_the_catalogs() {
    let e = fresh();
    let mut t = e.begin();
    let class = e.cat.lookup_relation(&mut t, "sys_class").unwrap().unwrap();
    assert_eq!(class.oid, SYS_CLASS.0);
    assert_eq!(class.kind, RelKind::Table);
    let names: Vec<&str> = class.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        ["oid", "relname", "relkind", "relfilenode", "relnatts"]
    );
    assert_eq!(class.desc(), class_desc());

    let attr = e.cat.lookup_oid(&mut t, SYS_ATTRIBUTE.0).unwrap().unwrap();
    assert_eq!(attr.name, "sys_attribute");
    assert_eq!(attr.desc(), attribute_desc());

    let idx = e
        .cat
        .lookup_relation(&mut t, "sys_class_relname_index")
        .unwrap()
        .unwrap();
    assert_eq!(idx.kind, RelKind::Index);
    assert!(idx.columns.is_empty());

    let class_indexes = e.cat.indexes_of(&mut t, SYS_CLASS.0).unwrap();
    assert_eq!(class_indexes.len(), 2);
    assert_eq!(class_indexes[0].rel, SYS_CLASS_OID_INDEX);
    assert_eq!(class_indexes[1].columns, vec![2]);
    assert!(class_indexes.iter().all(|i| i.unique));

    assert!(e.cat.lookup_relation(&mut t, "nope").unwrap().is_none());
}

#[test]
fn catalogs_survive_restart() {
    let e = fresh();
    let mem = e.mem.clone();
    drop(e);
    let e = start(&mem);
    let mut t = e.begin();
    assert!(
        e.cat
            .lookup_relation(&mut t, "sys_index")
            .unwrap()
            .is_some()
    );
}

fn cols() -> Vec<Column> {
    vec![
        Column {
            name: "id".into(),
            ty: DataType::Int4,
            not_null: true,
        },
        Column {
            name: "name".into(),
            ty: DataType::Varchar(Some(10)),
            not_null: false,
        },
    ]
}

fn file_exists(e: &Env, rel: RelFileId) -> bool {
    use crate::storage::backend::FileKey;
    e.mem.file_len(FileKey::Relation(rel)).is_ok()
}

#[test]
fn create_table_is_transactional() {
    let e = fresh();
    let mut t1 = e.begin();
    let info = e.cat.create_table(&mut t1, "t", &cols()).unwrap();
    assert!(info.oid >= crate::wal::recovery::FIRST_OID);
    assert_eq!(e.cat.lookup_relation(&mut t1, "t").unwrap().unwrap(), info);
    let mut t2 = e.begin();
    assert!(e.cat.lookup_relation(&mut t2, "t").unwrap().is_none());
    t1.commit().unwrap();
    t2.statement_snapshot();
    let seen = e.cat.lookup_relation(&mut t2, "t").unwrap().unwrap();
    assert_eq!(seen.columns, cols());
    assert!(file_exists(&e, info.rel));
}

#[test]
fn rolled_back_create_disappears() {
    let e = fresh();
    let mut t = e.begin();
    let info = e.cat.create_table(&mut t, "gone", &cols()).unwrap();
    t.abort().unwrap();
    let mut r = e.begin();
    assert!(e.cat.lookup_relation(&mut r, "gone").unwrap().is_none());
    assert!(!file_exists(&e, info.rel));
}

#[test]
fn duplicate_name_is_rejected() {
    let e = fresh();
    let mut t = e.begin();
    e.cat.create_table(&mut t, "a", &cols()).unwrap();
    t.commit().unwrap();
    let mut t = e.begin();
    assert!(matches!(
        e.cat.create_table(&mut t, "a", &cols()),
        Err(CatalogError::DuplicateRelation(n)) if n == "a"
    ));
    t.abort().unwrap();
    let mut t = e.begin();
    assert!(matches!(
        e.cat.create_table(&mut t, "sys_class", &cols()),
        Err(CatalogError::DuplicateRelation(_))
    ));
}

#[test]
fn concurrent_create_waits_for_the_other_transaction() {
    for first_commits in [true, false] {
        let e = fresh();
        let mut t1 = e.begin();
        e.cat.create_table(&mut t1, "x", &cols()).unwrap();
        let r = std::thread::scope(|s| {
            let h = s.spawn(|| {
                let mut t2 = e.begin();
                let r = e.cat.create_table(&mut t2, "x", &cols()).map(|_| ());
                match &r {
                    Ok(()) => t2.commit().unwrap(),
                    Err(_) => t2.abort().unwrap(),
                }
                r
            });
            std::thread::sleep(std::time::Duration::from_millis(50));
            if first_commits {
                t1.commit().unwrap();
            } else {
                t1.abort().unwrap();
            }
            h.join().unwrap()
        });
        if first_commits {
            assert!(matches!(r, Err(CatalogError::DuplicateRelation(_))));
        } else {
            assert!(r.is_ok(), "{r:?}");
        }
        let mut t = e.begin();
        assert!(e.cat.lookup_relation(&mut t, "x").unwrap().is_some());
    }
}

#[test]
fn drop_table_is_transactional() {
    let e = fresh();
    let mut t = e.begin();
    let info = e.cat.create_table(&mut t, "d", &cols()).unwrap();
    t.commit().unwrap();

    let mut t = e.begin();
    e.cat.drop_table(&mut t, "d").unwrap();
    assert!(e.cat.lookup_relation(&mut t, "d").unwrap().is_none());
    t.abort().unwrap();
    let mut r = e.begin();
    assert!(
        e.cat.lookup_relation(&mut r, "d").unwrap().is_some(),
        "drop rolled back"
    );
    assert!(file_exists(&e, info.rel));
    drop(r);

    let mut t = e.begin();
    e.cat.drop_table(&mut t, "d").unwrap();
    t.commit().unwrap();
    assert!(!file_exists(&e, info.rel));
    let mut r = e.begin();
    assert!(e.cat.lookup_relation(&mut r, "d").unwrap().is_none());
    assert!(matches!(
        e.cat.drop_table(&mut r, "d"),
        Err(CatalogError::UndefinedRelation(_))
    ));
    drop(r);

    // The name can be reused.
    let mut t = e.begin();
    let again = e.cat.create_table(&mut t, "d", &cols()).unwrap();
    assert_ne!(again.oid, info.oid);
    t.commit().unwrap();
}

#[test]
fn drop_waits_for_readers() {
    let e = fresh();
    let mut t = e.begin();
    let info = e.cat.create_table(&mut t, "busy", &cols()).unwrap();
    t.commit().unwrap();
    let reader = e.begin();
    reader
        .lock_relation(RelFileId(info.oid), LockMode::AccessShare)
        .unwrap();
    let dropped = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            let mut t = e.begin();
            e.cat.drop_table(&mut t, "busy").unwrap();
            t.commit().unwrap();
            dropped.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !dropped.load(std::sync::atomic::Ordering::SeqCst),
            "blocked by the reader"
        );
        reader.commit().unwrap();
    });
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn ddl_after_crash() {
    let e = fresh();
    let mut t = e.begin();
    e.cat.create_table(&mut t, "kept", &cols()).unwrap();
    t.commit().unwrap();
    let mut open = e.begin();
    e.cat.create_table(&mut open, "lost", &cols()).unwrap();
    let mem = e.mem.clone();
    std::mem::forget(open); // crash with the CREATE uncommitted
    drop(e);
    let e = start(&mem);
    let mut r = e.begin();
    assert!(e.cat.lookup_relation(&mut r, "kept").unwrap().is_some());
    assert!(e.cat.lookup_relation(&mut r, "lost").unwrap().is_none());
    // The name is free again (the crashed creator counts as aborted).
    drop(r);
    let mut t = e.begin();
    e.cat.create_table(&mut t, "lost", &cols()).unwrap();
    t.commit().unwrap();
}
