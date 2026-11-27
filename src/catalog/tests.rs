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
