//! Referential integrity (DESIGN r13): foreign key checks with key locks.
//!
//! Child side (a row inserted, or its key columns changed): share-lock the
//! referenced parent key, then look for a parent row with that key in the
//! parent's unique index. Parent side (a referenced row deleted, or its
//! key changed): lock the key exclusively, which waits for children that
//! share-locked it, then look for live referencing child rows.
//!
//! "Live" is judged on the latest committed state rather than a snapshot
//! (PostgreSQL's RI queries use a fresh snapshot under Read Committed).
//! Under Snapshot Isolation, a parent row the snapshot cannot see does not
//! count, a visible one deleted or updated since is a serialization
//! failure, and a referencing child committed after the snapshot is one
//! too (PostgreSQL's crosscheck snapshot).

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use super::error::{Result, SqlError, code};
use super::expr::{CastMode, cast, compare};
use crate::access::btree::BTree;
use crate::access::btree::key::encode_key;
use crate::access::heap::HeapFile;
use crate::access::tuple::{Tid, TupleHeader, deform_tuple, infomask};
use crate::access::visibility::{Me, satisfies_mvcc};
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, FkAction, IndexInfo, RelationInfo};
use crate::lock::LockMode;
use crate::storage::backend::RelFileId;
use crate::storage::page::BlockNumber;
use crate::txn::{Isolation, Transaction, TransactionManager, Xid, XidStatus};
use crate::types::{DataType, Value};

/// A foreign key of this table (it is the child).
#[derive(Debug, Clone)]
pub struct FkOut {
    pub name: String,
    /// Child columns (0-based) in the order of the parent index columns.
    pub cols: Vec<usize>,
    pub parent: RelationInfo,
    /// The referenced unique index of the parent.
    pub index: IndexInfo,
    /// Parent key column types, in index order.
    pub key_types: Vec<DataType>,
}

/// A foreign key referencing this table (it is the parent).
#[derive(Debug, Clone)]
pub struct FkIn {
    pub name: String,
    pub child: RelationInfo,
    /// Child columns (0-based) in the order of `index`'s columns.
    pub child_cols: Vec<usize>,
    /// This table's referenced columns (0-based), in index order.
    pub cols: Vec<usize>,
    /// This table's unique index the key belongs to.
    pub index: IndexInfo,
    pub key_types: Vec<DataType>,
    /// A child index whose leading columns are the foreign key columns:
    /// the index and, per leading index column, the position in the key.
    pub child_index: Option<(IndexInfo, Vec<usize>)>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

/// The foreign keys of `table` in both directions, locking the other
/// tables (RowShare for checks; RowExclusive on children a referential
/// action may change).
pub fn foreign_keys(
    cat: &Catalog,
    txn: &mut Transaction,
    table: &RelationInfo,
) -> Result<(Vec<FkOut>, Vec<FkIn>)> {
    let mut out = Vec::new();
    for c in cat.constraints_of(txn, table.oid)? {
        let (Some(f), Some(index_oid)) = (&c.foreign, c.index) else {
            continue;
        };
        let parent = relation(cat, txn, f.parent)?;
        if parent.oid != table.oid {
            txn.lock_relation(RelFileId(parent.oid), LockMode::RowShare)?;
        }
        let index = index_of(cat, txn, &parent, index_oid)?;
        out.push(FkOut {
            name: c.name.clone(),
            cols: c.columns.iter().map(|&a| usize::from(a) - 1).collect(),
            key_types: key_types(&parent, &index),
            parent,
            index,
        });
    }
    let mut inn = Vec::new();
    for c in cat.referencing(txn, table.oid)? {
        let (Some(f), Some(index_oid)) = (&c.foreign, c.index) else {
            continue;
        };
        let child = relation(cat, txn, c.table)?;
        let acts = [f.on_delete, f.on_update];
        let changes_child = acts
            .iter()
            .any(|a| !matches!(a, FkAction::NoAction | FkAction::Restrict));
        if child.oid != table.oid {
            let mode = if changes_child {
                LockMode::RowExclusive
            } else {
                LockMode::RowShare
            };
            txn.lock_relation(RelFileId(child.oid), mode)?;
        }
        let index = index_of(cat, txn, table, index_oid)?;
        let child_cols: Vec<usize> = c.columns.iter().map(|&a| usize::from(a) - 1).collect();
        let child_index = cat.indexes_of(txn, child.oid)?.into_iter().find_map(|i| {
            let lead: Vec<usize> = i
                .columns
                .iter()
                .take(child_cols.len())
                .map(|&a| usize::from(a) - 1)
                .collect();
            let pos: Option<Vec<usize>> = lead
                .iter()
                .map(|l| child_cols.iter().position(|c| c == l))
                .collect();
            match pos {
                Some(p) if p.len() == child_cols.len() => Some((i, p)),
                _ => None,
            }
        });
        inn.push(FkIn {
            name: c.name.clone(),
            cols: f
                .parent_columns
                .iter()
                .map(|&a| usize::from(a) - 1)
                .collect(),
            key_types: key_types(table, &index),
            child,
            child_cols,
            index,
            child_index,
            on_delete: f.on_delete,
            on_update: f.on_update,
        });
    }
    Ok((out, inn))
}

fn relation(cat: &Catalog, txn: &mut Transaction, oid: u32) -> Result<RelationInfo> {
    cat.lookup_oid(txn, oid)?
        .ok_or_else(|| SqlError::internal("foreign key to a missing table"))
}

fn index_of(cat: &Catalog, txn: &mut Transaction, t: &RelationInfo, oid: u32) -> Result<IndexInfo> {
    cat.indexes_of(txn, t.oid)?
        .into_iter()
        .find(|i| i.oid == oid)
        .ok_or_else(|| SqlError::internal("foreign key without its index"))
}

fn key_types(t: &RelationInfo, index: &IndexInfo) -> Vec<DataType> {
    index
        .columns
        .iter()
        .map(|&a| t.columns[usize::from(a) - 1].ty)
        .collect()
}

/// Converts key values to `types`; `None` if a value is NULL or cannot
/// be represented (then no row can match it).
fn convert(vals: &[Value], types: &[DataType]) -> Option<Vec<Value>> {
    vals.iter()
        .zip(types)
        .map(|(v, t)| {
            if v.is_null() {
                return None;
            }
            let v = cast(v.clone(), *t, CastMode::Assignment).ok()?;
            t.validate(&v).ok()?;
            Some(v)
        })
        .collect()
}

fn encode(vals: &[Value], types: &[DataType]) -> Result<Vec<u8>> {
    let mut k = Vec::new();
    encode_key(types, vals, &mut k)?;
    Ok(k)
}

/// State of a tuple version's inserter or deleter relative to `me`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum By {
    Nobody,
    Me,
    Running(Xid),
    Committed,
    Aborted,
}

fn xmin_of(h: &TupleHeader, me: Option<Xid>, tm: &dyn TransactionManager) -> Result<By> {
    if Some(h.xmin) == me {
        return Ok(By::Me);
    }
    if h.infomask & infomask::XMIN_INVALID != 0 {
        return Ok(By::Aborted);
    }
    if h.infomask & infomask::XMIN_COMMITTED != 0 {
        return Ok(By::Committed);
    }
    Ok(match tm.status(h.xmin)? {
        XidStatus::Committed => By::Committed,
        XidStatus::Aborted => By::Aborted,
        XidStatus::InProgress => By::Running(h.xmin),
    })
}

fn xmax_of(h: &TupleHeader, me: Option<Xid>, tm: &dyn TransactionManager) -> Result<By> {
    if !h.xmax.is_valid() || h.infomask & infomask::XMAX_INVALID != 0 {
        return Ok(By::Nobody);
    }
    if Some(h.xmax) == me {
        return Ok(By::Me);
    }
    if h.infomask & infomask::XMAX_COMMITTED != 0 {
        return Ok(By::Committed);
    }
    Ok(match tm.status(h.xmax)? {
        XidStatus::Committed => By::Committed,
        XidStatus::Aborted => By::Nobody,
        XidStatus::InProgress => By::Running(h.xmax),
    })
}

fn serialization_failure() -> SqlError {
    SqlError::new(
        code::SERIALIZATION_FAILURE,
        "could not serialize access due to concurrent update",
    )
}

/// Child side: the parent key referenced by `row` must exist.
pub fn check_reference(
    pool: &Arc<BufferPool>,
    txn: &mut Transaction,
    child: &RelationInfo,
    fk: &FkOut,
    row: &[Value],
) -> Result<()> {
    let vals: Vec<Value> = fk.cols.iter().map(|&c| row[c].clone()).collect();
    if vals.iter().any(Value::is_null) {
        return Ok(()); // MATCH SIMPLE: a NULL column means no reference
    }
    let violation = || {
        SqlError::new(
            code::FOREIGN_KEY_VIOLATION,
            format!(
                "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                child.name, fk.name
            ),
        )
    };
    let Some(key_vals) = convert(&vals, &fk.key_types) else {
        return Err(violation());
    };
    let key = encode(&key_vals, &fk.key_types)?;
    txn.lock_key(fk.index.rel, key.clone(), LockMode::Share)?;

    let heap = HeapFile::open(fk.parent.rel, pool.clone())?;
    let tree = BTree::open(fk.index.rel, pool.clone())?;
    let snap = txn.snapshot();
    let me = Me {
        xid: txn.xid(),
        cid: txn.command_id(),
    };
    let si = txn.isolation() == Isolation::SnapshotIsolation;
    let tm = txn.tm();
    for e in tree.scan(Bound::Included(key.clone()), Bound::Included(key))? {
        let (_, tid) = e?;
        let Some(t) = heap.fetch(tid)? else {
            continue;
        };
        let h = TupleHeader::decode(&t)?;
        let inserted = xmin_of(&h, me.xid, tm)?;
        let deleted = xmax_of(&h, me.xid, tm)?;
        if inserted == By::Me {
            if deleted != By::Me {
                return Ok(());
            }
            continue;
        }
        if inserted != By::Committed {
            continue;
        }
        if si && !satisfies_mvcc(&h, &snap, &me, tm)?.visible {
            // Committed after our snapshot (inserted), or deleted before
            // it: not ours to see.
            continue;
        }
        match deleted {
            // A running xmax is an update keeping the key (removing it
            // would hold the key lock we just got).
            By::Nobody | By::Running(_) | By::Aborted => return Ok(()),
            By::Me => continue,
            By::Committed if si => return Err(serialization_failure()),
            By::Committed => continue,
        }
    }
    Err(violation())
}

/// Parent side: locks the key `parent_row` holds for `fk` exclusively and
/// returns the live child rows referencing it. Children inserted by
/// transactions still running are not counted (they have not checked
/// their reference yet; that check will fail once we commit).
pub fn referencing_rows(
    pool: &Arc<BufferPool>,
    txn: &mut Transaction,
    fk: &FkIn,
    parent_row: &[Value],
) -> Result<Vec<(Tid, Vec<Value>)>> {
    let vals: Vec<Value> = fk.cols.iter().map(|&c| parent_row[c].clone()).collect();
    let Some(key_vals) = convert(&vals, &fk.key_types) else {
        return Ok(Vec::new());
    };
    let key = encode(&key_vals, &fk.key_types)?;
    txn.lock_key(fk.index.rel, key, LockMode::Exclusive)?;

    let heap = HeapFile::open(fk.child.rel, pool.clone())?;
    let desc = fk.child.desc();
    'retry: loop {
        let candidates: Vec<(Tid, Vec<u8>)> = match child_key_bounds(fk, &key_vals)? {
            Some((index, lower, upper)) => {
                let tree = BTree::open(index, pool.clone())?;
                let mut v = Vec::new();
                for e in tree.scan(lower, upper)? {
                    let (_, tid) = e?;
                    if let Some(t) = heap.fetch(tid)? {
                        v.push((tid, t));
                    }
                }
                v
            }
            None => {
                let mut v = Vec::new();
                for blk in 0..heap.nblocks()? {
                    v.extend(heap.page_tuples(BlockNumber(blk))?);
                }
                v
            }
        };
        let snap = txn.snapshot();
        let mine = txn.xid();
        let si = txn.isolation() == Isolation::SnapshotIsolation;
        let mut live = Vec::new();
        for (tid, t) in candidates {
            let row = deform_tuple(&desc, &t)?;
            let child_vals: Vec<Value> = fk.child_cols.iter().map(|&c| row[c].clone()).collect();
            let Some(cv) = convert(&child_vals, &fk.key_types) else {
                continue;
            };
            let same = cv
                .iter()
                .zip(&key_vals)
                .all(|(a, b)| compare(a, b) == Some(Ordering::Equal));
            if !same {
                continue;
            }
            let h = TupleHeader::decode(&t)?;
            let tm = txn.tm();
            let inserted = xmin_of(&h, mine, tm)?;
            if !matches!(inserted, By::Me | By::Committed) {
                continue;
            }
            match xmax_of(&h, mine, tm)? {
                By::Me | By::Committed => continue,
                By::Running(x) => {
                    // Being deleted or updated: wait for the outcome.
                    txn.wait_for(x)?;
                    continue 'retry;
                }
                By::Nobody | By::Aborted => {}
            }
            if si && inserted == By::Committed && snap.is_running(h.xmin) {
                return Err(serialization_failure());
            }
            live.push((tid, row));
        }
        return Ok(live);
    }
}

type KeyRange = (RelFileId, Bound<Vec<u8>>, Bound<Vec<u8>>);

/// Key range of the child index for the referenced key (prefix match).
fn child_key_bounds(fk: &FkIn, key_vals: &[Value]) -> Result<Option<KeyRange>> {
    let Some((index, pos)) = &fk.child_index else {
        return Ok(None);
    };
    let types: Vec<DataType> = pos
        .iter()
        .map(|&p| match fk.child.columns[fk.child_cols[p]].ty {
            DataType::Varchar(_) => DataType::Text,
            t => t,
        })
        .collect();
    let vals: Vec<Value> = pos.iter().map(|&p| key_vals[p].clone()).collect();
    let Some(vals) = convert(&vals, &types) else {
        return Ok(None);
    };
    let prefix = encode(&vals, &types)?;
    let mut upper = prefix.clone();
    let upper = loop {
        match upper.pop() {
            Some(b) if b < u8::MAX => {
                upper.push(b + 1);
                break Bound::Excluded(upper);
            }
            Some(_) => {}
            None => break Bound::Unbounded,
        }
    };
    Ok(Some((index.rel, Bound::Included(prefix), upper)))
}

/// The NO ACTION / RESTRICT error for a parent change.
pub fn still_referenced(parent: &RelationInfo, fk: &FkIn) -> SqlError {
    SqlError::new(
        code::FOREIGN_KEY_VIOLATION,
        format!(
            "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
            parent.name, fk.name, fk.child.name
        ),
    )
}

/// Whether the referenced key columns of `fk` differ between two rows.
pub fn key_changed(cols: &[usize], old: &[Value], new: &[Value]) -> bool {
    cols.iter().any(|&c| match (&old[c], &new[c]) {
        (Value::Null, Value::Null) => false,
        (a, b) => compare(a, b) != Some(Ordering::Equal),
    })
}
