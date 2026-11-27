//! Row writes with everything that goes with them: NOT NULL and CHECK
//! constraints, index entries (unique indexes enforced), and foreign keys
//! in both directions, including referential actions on child rows.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use super::binder::{Binder, Rules};
use super::error::{Result, SqlError, code};
use super::ri::{self, FkIn};
use crate::access::btree::BTree;
use crate::access::heap::{HeapFile, TmResult};
use crate::access::tuple::{Tid, form_tuple};
use crate::buffer::BufferPool;
use crate::catalog::{Catalog, FkAction, IndexInfo, RelationInfo};
use crate::txn::Transaction;
use crate::types::Value;

/// One table opened for writing.
pub struct TableWriter {
    pub table: RelationInfo,
    pub rules: Rules,
    heap: HeapFile,
    indexes: Vec<(IndexInfo, BTree)>,
}

/// Writes rows of one statement (tables opened on first use).
pub struct Writer<'a> {
    cat: &'a Catalog,
    pool: Arc<BufferPool>,
    tables: HashMap<u32, Rc<TableWriter>>,
}

impl<'a> Writer<'a> {
    pub fn new(cat: &'a Catalog) -> Self {
        Self {
            cat,
            pool: cat.engine().pool().clone(),
            tables: HashMap::new(),
        }
    }

    /// Opens `table` with its rules (bound now if not given).
    pub fn table(
        &mut self,
        txn: &mut Transaction,
        table: &RelationInfo,
        rules: Option<Rules>,
    ) -> Result<Rc<TableWriter>> {
        if let Some(t) = self.tables.get(&table.oid) {
            return Ok(t.clone());
        }
        let rules = match rules {
            Some(r) => r,
            None => Binder::new(self.cat, txn).table_rules(table)?,
        };
        let t = Rc::new(TableWriter {
            heap: HeapFile::open(table.rel, self.pool.clone())?,
            indexes: self.cat.table_indexes(txn, table)?,
            table: table.clone(),
            rules,
        });
        self.tables.insert(table.oid, t.clone());
        Ok(t)
    }

    fn add_index_entries(
        &self,
        txn: &mut Transaction,
        tw: &TableWriter,
        row: &[Value],
        tid: Tid,
    ) -> Result<()> {
        for (idx, tree) in &tw.indexes {
            self.cat
                .insert_entry(txn, &tw.table, idx, tree, &tw.heap, row, tid)?;
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        txn: &mut Transaction,
        tw: &TableWriter,
        row: &[Value],
    ) -> Result<Tid> {
        check_row(&tw.table, &tw.rules, row)?;
        let xid = txn.write_xid()?;
        let t = form_tuple(&tw.table.desc(), row, xid, txn.command_id())?;
        let tid = tw.heap.insert(&t)?;
        self.add_index_entries(txn, tw, row, tid)?;
        for fk in &tw.rules.fk_out {
            ri::check_reference(&self.pool, txn, &tw.table, fk, row)?;
        }
        Ok(tid)
    }

    /// Replaces the version at `tid` (whose values are `old`) by `new`,
    /// waiting for concurrent writers; see [`HeapFile::update_waiting`].
    pub fn update(
        &mut self,
        txn: &mut Transaction,
        tw: &TableWriter,
        tid: Tid,
        old: &[Value],
        new: &[Value],
    ) -> Result<TmResult> {
        check_row(&tw.table, &tw.rules, new)?;
        let xid = txn.write_xid()?;
        let t = form_tuple(&tw.table.desc(), new, xid, txn.command_id())?;
        let r = tw.heap.update_waiting(tid, &t, txn)?;
        let TmResult::Ok(new_tid) = r else {
            return Ok(r);
        };
        self.add_index_entries(txn, tw, new, new_tid)?;
        for fk in &tw.rules.fk_in {
            if ri::key_changed(&fk.cols, old, new) {
                self.parent_changed(txn, tw, fk, old, Some(new))?;
            }
        }
        for fk in &tw.rules.fk_out {
            if ri::key_changed(&fk.cols, old, new) {
                ri::check_reference(&self.pool, txn, &tw.table, fk, new)?;
            }
        }
        Ok(r)
    }

    /// Deletes the version at `tid` (whose values are `old`).
    pub fn delete(
        &mut self,
        txn: &mut Transaction,
        tw: &TableWriter,
        tid: Tid,
        old: &[Value],
    ) -> Result<TmResult> {
        let r = tw.heap.delete_waiting(tid, txn)?;
        if let TmResult::Ok(_) = r {
            for fk in &tw.rules.fk_in {
                self.parent_changed(txn, tw, fk, old, None)?;
            }
        }
        Ok(r)
    }

    /// A referenced key of `parent` was deleted (`new` is `None`) or
    /// changed: applies the foreign key's action to referencing rows.
    fn parent_changed(
        &mut self,
        txn: &mut Transaction,
        parent: &TableWriter,
        fk: &FkIn,
        old: &[Value],
        new: Option<&[Value]>,
    ) -> Result<()> {
        let children = ri::referencing_rows(&self.pool, txn, fk, old)?;
        if children.is_empty() {
            return Ok(());
        }
        let action = match new {
            Some(_) => fk.on_update,
            None => fk.on_delete,
        };
        match action {
            FkAction::NoAction | FkAction::Restrict => Err(ri::still_referenced(&parent.table, fk)),
            _ => Err(SqlError::unsupported(
                "referential actions other than NO ACTION and RESTRICT",
            )),
        }
    }
}

/// NOT NULL and CHECK constraints for a row about to be stored.
pub fn check_row(table: &RelationInfo, rules: &Rules, row: &[Value]) -> Result<()> {
    for &c in &rules.not_null {
        if row[c].is_null() {
            return Err(SqlError::new(
                code::NOT_NULL_VIOLATION,
                format!(
                    "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                    table.columns[c].name, table.name
                ),
            ));
        }
    }
    for (name, e) in &rules.checks {
        // Only FALSE fails; NULL passes.
        if e.eval(row)? == Value::Bool(false) {
            return Err(SqlError::new(
                code::CHECK_VIOLATION,
                format!(
                    "new row for relation \"{}\" violates check constraint \"{name}\"",
                    table.name
                ),
            ));
        }
    }
    Ok(())
}
