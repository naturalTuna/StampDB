use super::*;
use crate::buffer::SingleModeFlush;
use crate::lock::{LockManager, WaitPolicy};
use crate::storage::backend::FenceToken;
use crate::storage::mem::MemBackend;
use crate::txn::{Isolation, TransactionManager};
use std::ops::Bound;

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
            default: None,
        },
        Column {
            name: "name".into(),
            ty: DataType::Varchar(Some(10)),
            not_null: false,
            default: None,
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

fn people() -> Vec<Column> {
    vec![
        Column {
            name: "id".into(),
            ty: DataType::Int4,
            not_null: true,
            default: None,
        },
        Column {
            name: "email".into(),
            ty: DataType::Text,
            not_null: false,
            default: None,
        },
    ]
}

/// Inserts a row into `table` and its indexes.
fn put(
    e: &Env,
    t: &mut Transaction,
    table: &RelationInfo,
    id: i32,
    email: Option<&str>,
) -> Result<Tid> {
    let xid = t.write_xid()?;
    let row = vec![
        Value::Int4(id),
        email.map_or(Value::Null, |s| Value::Text(s.into())),
    ];
    let tuple = form_tuple(&table.desc(), &row, xid, t.command_id())?;
    let heap = HeapFile::open(table.rel, e.cat.engine().pool().clone())?;
    let tid = heap.insert(&tuple)?;
    e.cat.insert_index_entries(t, table, &row, tid)?;
    Ok(tid)
}

fn index_entries(e: &Env, idx: &IndexInfo) -> usize {
    BTree::open(idx.rel, e.cat.engine().pool().clone())
        .unwrap()
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .count()
}

#[test]
fn create_index_indexes_existing_rows() {
    let e = fresh();
    let mut t = e.begin();
    let tab = e.cat.create_table(&mut t, "people", &people()).unwrap();
    for i in 0..300 {
        put(&e, &mut t, &tab, i, Some(&format!("u{i}@x"))).unwrap();
    }
    t.commit().unwrap();
    let mut t = e.begin();
    let idx = e
        .cat
        .create_index(&mut t, "people_email", "people", &["email"], false)
        .unwrap();
    t.commit().unwrap();
    assert_eq!(index_entries(&e, &idx), 300);
    let mut r = e.begin();
    let found = e.cat.indexes_of(&mut r, tab.oid).unwrap();
    assert_eq!(found, vec![idx]);
    assert!(matches!(
        e.cat
            .create_index(&mut r, "bad", "people", &["nope"], false),
        Err(CatalogError::UndefinedColumn(_))
    ));
}

#[test]
fn unique_indexes_reject_duplicates() {
    let e = fresh();
    let mut t = e.begin();
    let tab = e.cat.create_table(&mut t, "people", &people()).unwrap();
    put(&e, &mut t, &tab, 1, Some("a@x")).unwrap();
    put(&e, &mut t, &tab, 2, Some("a@x")).unwrap();
    t.commit().unwrap();

    // Building over duplicate data fails, and leaves nothing behind.
    let mut t = e.begin();
    assert!(matches!(
        e.cat.create_index(&mut t, "people_email_key", "people", &["email"], true),
        Err(CatalogError::UniqueViolation(n)) if n == "people_email_key"
    ));
    t.abort().unwrap();
    let mut r = e.begin();
    assert!(
        e.cat
            .lookup_relation(&mut r, "people_email_key")
            .unwrap()
            .is_none()
    );
    drop(r);

    let mut t = e.begin();
    let idx = e
        .cat
        .create_index(&mut t, "people_id_key", "people", &["id"], true)
        .unwrap();
    t.commit().unwrap();

    let mut t = e.begin();
    let dup = put(&e, &mut t, &tab, 1, Some("b@x"));
    assert!(matches!(dup, Err(CatalogError::UniqueViolation(n)) if n == "people_id_key"));
    t.abort().unwrap();

    // NULLs never conflict; a deleted (committed) row frees its key.
    let mut t = e.begin();
    let heap = HeapFile::open(tab.rel, e.cat.engine().pool().clone()).unwrap();
    let mut snap_t = e.begin();
    let me = Me {
        xid: None,
        cid: snap_t.command_id(),
    };
    let snap = snap_t.snapshot();
    let first = heap
        .scan_mvcc(snap, me, snap_t.tm())
        .unwrap()
        .map(|r| r.unwrap())
        .find(|(_, row)| deform_tuple(&tab.desc(), row).unwrap()[0] == Value::Int4(1))
        .unwrap()
        .0;
    drop(snap_t);
    heap.delete_mvcc(first, &mut t).unwrap();
    t.commit().unwrap();
    let mut t = e.begin();
    put(&e, &mut t, &tab, 1, None).unwrap();
    t.commit().unwrap();
    assert_eq!(
        index_entries(&e, &idx),
        3,
        "old dead entry stays until vacuum"
    );
}

#[test]
fn drop_table_drops_its_indexes_and_drop_index_works() {
    let e = fresh();
    let mut t = e.begin();
    let tab = e.cat.create_table(&mut t, "people", &people()).unwrap();
    let i1 = e
        .cat
        .create_index(&mut t, "p_id", "people", &["id"], true)
        .unwrap();
    let i2 = e
        .cat
        .create_index(&mut t, "p_email", "people", &["email"], false)
        .unwrap();
    t.commit().unwrap();

    let mut t = e.begin();
    e.cat.drop_index(&mut t, "p_email").unwrap();
    t.commit().unwrap();
    assert!(!file_exists(&e, i2.rel));
    let mut r = e.begin();
    assert_eq!(e.cat.indexes_of(&mut r, tab.oid).unwrap(), vec![i1.clone()]);
    drop(r);

    let mut t = e.begin();
    e.cat.drop_table(&mut t, "people").unwrap();
    t.commit().unwrap();
    assert!(!file_exists(&e, tab.rel) && !file_exists(&e, i1.rel));
    let mut r = e.begin();
    assert!(e.cat.lookup_relation(&mut r, "p_id").unwrap().is_none());
}

#[test]
fn create_index_waits_for_writers() {
    let e = fresh();
    let mut t = e.begin();
    let tab = e.cat.create_table(&mut t, "w", &people()).unwrap();
    t.commit().unwrap();
    let mut writer = e.begin();
    writer
        .lock_relation(RelFileId(tab.oid), LockMode::RowExclusive)
        .unwrap();
    put(&e, &mut writer, &tab, 7, Some("w@x")).unwrap();
    let done = std::sync::atomic::AtomicBool::new(false);
    let idx = std::thread::scope(|s| {
        let h = s.spawn(|| {
            let mut t = e.begin();
            let idx = e
                .cat
                .create_index(&mut t, "w_id", "w", &["id"], true)
                .unwrap();
            t.commit().unwrap();
            done.store(true, std::sync::atomic::Ordering::SeqCst);
            idx
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!done.load(std::sync::atomic::Ordering::SeqCst));
        writer.commit().unwrap();
        h.join().unwrap()
    });
    assert_eq!(index_entries(&e, &idx), 1, "the committed row is indexed");
}

#[test]
fn constraints_are_stored_and_enforced_by_indexes() {
    let e = fresh();
    let mut t = e.begin();
    let mut cols = vec![
        Column::new("id", DataType::Int4),
        Column::new("code", DataType::Text),
        Column::new("qty", DataType::Int4),
    ];
    cols[2].default = Some("1 + 1".to_owned());
    let cons = [
        NewConstraint {
            name: "t_pkey".into(),
            kind: ConstraintKind::PrimaryKey(vec!["id".into()]),
        },
        NewConstraint {
            name: "t_code_key".into(),
            kind: ConstraintKind::Unique(vec!["code".into(), "qty".into()]),
        },
        NewConstraint {
            name: "t_qty_check".into(),
            kind: ConstraintKind::Check("qty > 0".into()),
        },
    ];
    let info = e.cat.create_table_with(&mut t, "t", &cols, &cons).unwrap();
    assert!(info.columns[0].not_null, "primary key columns are NOT NULL");
    t.commit().unwrap();

    let mut t = e.begin();
    let info = e.cat.lookup_relation(&mut t, "t").unwrap().unwrap();
    assert!(info.columns[0].not_null);
    assert_eq!(info.columns[2].default.as_deref(), Some("1 + 1"));
    let cs = e.cat.constraints_of(&mut t, info.oid).unwrap();
    let idx = e.cat.indexes_of(&mut t, info.oid).unwrap();
    assert_eq!(cs.len(), 3);
    assert_eq!(idx.len(), 2);
    assert!(cs[0].primary && cs[0].index == Some(idx[0].oid) && cs[0].columns == [1]);
    assert!(idx[0].primary && idx[0].unique && idx[0].name == "t_pkey");
    assert_eq!((cs[1].columns.clone(), cs[1].primary), (vec![2, 3], false));
    assert_eq!(cs[2].check.as_deref(), Some("qty > 0"));
    assert!(matches!(
        e.cat.drop_index(&mut t, "t_pkey"),
        Err(CatalogError::IndexRequiredByConstraint { .. })
    ));
    drop(t);

    // Definition errors.
    let mut t = e.begin();
    let pk = |c: &str| NewConstraint {
        name: format!("{c}_pk"),
        kind: ConstraintKind::PrimaryKey(vec![c.into()]),
    };
    assert!(matches!(
        e.cat
            .create_table_with(&mut t, "u", &cols, &[pk("id"), pk("code")]),
        Err(CatalogError::MultiplePrimaryKeys(_))
    ));
    assert!(matches!(
        e.cat.create_table_with(&mut t, "u", &cols, &[pk("nope")]),
        Err(CatalogError::KeyColumnMissing(_))
    ));
    assert!(matches!(
        e.cat
            .create_table_with(&mut t, "u", &cols, &[pk("id"), pk("id")]),
        Err(CatalogError::DuplicateConstraint { .. })
    ));
    drop(t);

    // Dropping the table removes its constraints.
    let mut t = e.begin();
    e.cat.drop_table(&mut t, "t").unwrap();
    assert!(e.cat.constraints_of(&mut t, info.oid).unwrap().is_empty());
    t.commit().unwrap();
    assert_eq!(
        e.cat
            .choose_relation_name(&mut e.begin(), "t_pkey")
            .unwrap(),
        "t_pkey"
    );
}
