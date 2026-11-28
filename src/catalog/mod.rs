//! System catalogs (DESIGN: catalog, r9/r10).
//!
//! The catalogs are ordinary MVCC heap tables with B+Tree indexes, so DDL
//! is transactional: creating or dropping a relation inserts or deletes
//! catalog rows inside the user's transaction, and relation files follow
//! the transaction outcome (pending deletes). Lookups read the catalogs
//! with the transaction's snapshot through the catalog indexes (no cache
//! yet), so a transaction sees its own uncommitted DDL.

pub mod schema;

use std::ops::Bound;
use std::sync::Arc;

use crate::access::btree::key::{KeyError, encode_key};
use crate::access::btree::{BTree, BTreeError, InsertOutcome};
use crate::access::heap::TmResult;
use crate::access::heap::{HeapError, HeapFile};
use crate::access::tuple::{Tid, TupleDesc, TupleError, deform_tuple, form_tuple};
use crate::access::visibility::Me;
use crate::buffer::FlushControl;
use crate::lock::LockMode;
use crate::storage::backend::{FenceSource, RelFileId, StorageBackend};
use crate::txn::{CommandId, Transaction, TransactionError, Xid};
use crate::types::{DataType, Value};
use crate::wal::{Engine, RecoveryError, WalConfig};

use schema::*;

/// Errors from catalog operations.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Heap(#[from] HeapError),

    #[error(transparent)]
    BTree(#[from] BTreeError),

    #[error(transparent)]
    Tuple(#[from] TupleError),

    #[error(transparent)]
    Key(#[from] KeyError),

    #[error(transparent)]
    Transaction(#[from] TransactionError),

    #[error(transparent)]
    Recovery(#[from] RecoveryError),

    #[error(transparent)]
    Vacuum(#[from] crate::vacuum::VacuumError),

    #[error("relation \"{0}\" already exists")]
    DuplicateRelation(String),

    #[error("relation \"{0}\" does not exist")]
    UndefinedRelation(String),

    #[error("duplicate key value violates unique constraint \"{0}\"")]
    UniqueViolation(String),

    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),

    #[error("constraint \"{name}\" for relation \"{table}\" already exists")]
    DuplicateConstraint { name: String, table: String },

    #[error("multiple primary keys for table \"{0}\" are not allowed")]
    MultiplePrimaryKeys(String),

    #[error("column \"{0}\" named in key does not exist")]
    KeyColumnMissing(String),

    #[error("cannot drop index {index} because constraint {index} on table {table} requires it")]
    IndexRequiredByConstraint { index: String, table: String },

    #[error("{0}")]
    InvalidForeignKey(String),

    #[error(
        "foreign key constraint \"{name}\" cannot be implemented: key columns \"{child}\" and \"{parent}\" are of incompatible types: {child_ty} and {parent_ty}"
    )]
    ForeignKeyTypes {
        name: String,
        child: String,
        parent: String,
        child_ty: DataType,
        parent_ty: DataType,
    },

    #[error("cannot drop {what} because other objects depend on it")]
    DependentObjects { what: String },

    #[error("corrupt catalog: {0}")]
    Corrupt(&'static str),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Kind of a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelKind {
    Table,
    Index,
}

/// A column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: DataType,
    pub not_null: bool,
    /// DEFAULT expression as SQL text.
    pub default: Option<String>,
}

impl Column {
    /// A nullable column without default.
    pub fn new(name: &str, ty: DataType) -> Self {
        Self {
            name: name.to_owned(),
            ty,
            not_null: false,
            default: None,
        }
    }
}

/// Kind of a table constraint (sys_constraint.contype).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintKind {
    /// Columns (by name) of the primary key.
    PrimaryKey(Vec<String>),
    /// Columns of a UNIQUE constraint.
    Unique(Vec<String>),
    /// CHECK expression as SQL text.
    Check(String),
    /// FOREIGN KEY (columns) REFERENCES parent (parent_columns; its
    /// primary key if `None`).
    ForeignKey {
        columns: Vec<String>,
        parent: String,
        parent_columns: Option<Vec<String>>,
        on_delete: FkAction,
        on_update: FkAction,
    },
}

/// Referential action (PostgreSQL's confdeltype/confupdtype letters).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl FkAction {
    fn letter(self) -> &'static str {
        match self {
            FkAction::NoAction => "a",
            FkAction::Restrict => "r",
            FkAction::Cascade => "c",
            FkAction::SetNull => "n",
            FkAction::SetDefault => "d",
        }
    }

    fn from_letter(s: &str) -> Result<Self> {
        Ok(match s {
            "a" => FkAction::NoAction,
            "r" => FkAction::Restrict,
            "c" => FkAction::Cascade,
            "n" => FkAction::SetNull,
            "d" => FkAction::SetDefault,
            _ => return Err(CatalogError::Corrupt("bad foreign key action")),
        })
    }
}

/// The referencing side of a foreign key as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignInfo {
    /// Referenced table OID.
    pub parent: u32,
    /// Referenced columns of the parent, 1-based, in the order of the
    /// referenced unique index (`ConstraintInfo::columns` matches them).
    pub parent_columns: Vec<u16>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

/// A constraint to create with a table (names already chosen).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewConstraint {
    pub name: String,
    pub kind: ConstraintKind,
}

/// A constraint as stored in sys_constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintInfo {
    pub oid: u32,
    pub name: String,
    pub table: u32,
    /// Index enforcing a PRIMARY KEY or UNIQUE constraint.
    pub index: Option<u32>,
    /// Constrained columns, 1-based (empty for CHECK).
    pub columns: Vec<u16>,
    pub primary: bool,
    /// CHECK expression as SQL text.
    pub check: Option<String>,
    /// For a FOREIGN KEY: what it references (`index` is the referenced
    /// unique index).
    pub foreign: Option<ForeignInfo>,
}

/// A relation as described by sys_class (and sys_attribute for tables).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationInfo {
    pub oid: u32,
    pub name: String,
    pub kind: RelKind,
    pub rel: RelFileId,
    /// Columns in order (empty for indexes).
    pub columns: Vec<Column>,
}

impl RelationInfo {
    pub fn desc(&self) -> TupleDesc {
        TupleDesc {
            attrs: self.columns.iter().map(|c| c.ty).collect(),
        }
    }
}

/// An index as described by sys_index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub oid: u32,
    pub name: String,
    pub rel: RelFileId,
    pub table: u32,
    /// Indexed columns of the table, 1-based.
    pub columns: Vec<u16>,
    pub unique: bool,
    pub primary: bool,
}

/// Creates a new cluster with its system catalogs (initdb).
pub fn initdb(
    backend: Arc<dyn StorageBackend>,
    flush: Arc<dyn FlushControl>,
    cfg: WalConfig,
) -> Result<()> {
    let fence: Arc<dyn FenceSource> = flush.clone();
    crate::wal::initdb(backend.clone(), fence, cfg)?;
    let engine = Arc::new(Engine::start(backend, flush, cfg, 64)?);
    for (rel, _, _) in sys_tables() {
        engine.create_relation(rel)?;
    }
    for idx in SYS_INDEXES {
        engine.create_relation(idx.rel)?;
        BTree::create(idx.rel, engine.pool().clone())?;
    }
    let cat = Catalog::open(engine.clone())?;
    let (xid, cid) = (Xid::BOOTSTRAP, CommandId(0));
    for (rel, name, cols) in sys_tables() {
        cat.insert_row(
            SYS_CLASS,
            &class_row(rel.0, name, RELKIND_TABLE, rel, cols.len()),
            xid,
            cid,
        )?;
        for (i, (cname, ty)) in cols.iter().enumerate() {
            cat.insert_row(
                SYS_ATTRIBUTE,
                &attribute_row(rel.0, i + 1, &Column::new(cname, *ty)),
                xid,
                cid,
            )?;
        }
    }
    for idx in SYS_INDEXES {
        cat.insert_row(
            SYS_CLASS,
            &class_row(
                idx.rel.0,
                idx.name,
                RELKIND_INDEX,
                idx.rel,
                idx.columns.len(),
            ),
            xid,
            cid,
        )?;
        cat.insert_row(
            SYS_INDEX,
            &index_row(idx.rel.0, idx.table.0, idx.columns, idx.unique, false),
            xid,
            cid,
        )?;
    }
    drop(cat);
    engine.shutdown()?;
    Ok(())
}

fn class_row(oid: u32, name: &str, kind: &str, rel: RelFileId, natts: usize) -> Vec<Value> {
    vec![
        Value::Int8(i64::from(oid)),
        Value::Text(name.to_owned()),
        Value::Text(kind.to_owned()),
        Value::Int8(i64::from(rel.0)),
        Value::Int4(natts as i32),
    ]
}

fn attribute_row(relid: u32, attnum: usize, c: &Column) -> Vec<Value> {
    let (typid, typmod) = type_to_oid(c.ty);
    vec![
        Value::Int8(i64::from(relid)),
        Value::Int4(attnum as i32),
        Value::Text(c.name.clone()),
        Value::Int4(typid),
        Value::Int4(typmod),
        Value::Bool(c.not_null),
        c.default.clone().map_or(Value::Null, Value::Text),
    ]
}

fn constraint_row(oid: u32, c: &ConstraintInfo) -> Vec<Value> {
    let contype = match (&c.check, &c.foreign, c.primary) {
        (Some(_), _, _) => CONTYPE_CHECK,
        (None, Some(_), _) => CONTYPE_FOREIGN,
        (None, None, true) => CONTYPE_PRIMARY,
        (None, None, false) => CONTYPE_UNIQUE,
    };
    let f = c.foreign.as_ref();
    vec![
        Value::Int8(i64::from(oid)),
        Value::Text(c.name.clone()),
        Value::Text(contype.to_owned()),
        Value::Int8(i64::from(c.table)),
        Value::Int8(i64::from(c.index.unwrap_or(0))),
        Value::Text(format_indkey(&c.columns)),
        c.check.clone().map_or(Value::Null, Value::Text),
        Value::Int8(f.map_or(0, |f| i64::from(f.parent))),
        Value::Text(f.map_or_else(String::new, |f| format_indkey(&f.parent_columns))),
        Value::Text(f.map_or("", |f| f.on_update.letter()).to_owned()),
        Value::Text(f.map_or("", |f| f.on_delete.letter()).to_owned()),
    ]
}

fn index_row(
    indexrelid: u32,
    indrelid: u32,
    columns: &[u16],
    unique: bool,
    primary: bool,
) -> Vec<Value> {
    vec![
        Value::Int8(i64::from(indexrelid)),
        Value::Int8(i64::from(indrelid)),
        Value::Text(format_indkey(columns)),
        Value::Bool(unique),
        Value::Bool(primary),
    ]
}

/// Key column types a foreign key can compare (after implicit casts).
fn compatible_key_types(a: DataType, b: DataType) -> bool {
    let text = |t| matches!(t, DataType::Text | DataType::Varchar(_));
    let num = |t| matches!(t, DataType::Int4 | DataType::Int8 | DataType::Float8);
    a == b || (text(a) && text(b)) || (num(a) && num(b))
}

/// The foreign key part of a sys_constraint row.
fn foreign_of(r: &[Value]) -> Result<Option<ForeignInfo>> {
    if text(r, 2)? != CONTYPE_FOREIGN {
        return Ok(None);
    }
    Ok(Some(ForeignInfo {
        parent: int(r, 7)? as u32,
        parent_columns: parse_indkey(&text(r, 8)?)?,
        on_update: FkAction::from_letter(&text(r, 9)?)?,
        on_delete: FkAction::from_letter(&text(r, 10)?)?,
    }))
}

fn desc_of(table: RelFileId) -> Result<TupleDesc> {
    Ok(match table {
        SYS_CLASS => class_desc(),
        SYS_ATTRIBUTE => attribute_desc(),
        SYS_INDEX => index_desc(),
        SYS_CONSTRAINT => constraint_desc(),
        _ => return Err(CatalogError::Corrupt("not a system table")),
    })
}

/// Access to the system catalogs.
#[derive(Debug)]
pub struct Catalog {
    engine: Arc<Engine>,
    class: HeapFile,
    attribute: HeapFile,
    index: HeapFile,
    constraint: HeapFile,
    indexes: Vec<(SysIndex, BTree)>,
}

impl Catalog {
    pub fn open(engine: Arc<Engine>) -> Result<Self> {
        let pool = engine.pool().clone();
        let indexes = SYS_INDEXES
            .iter()
            .map(|i| Ok((*i, BTree::open(i.rel, pool.clone())?)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            class: HeapFile::open(SYS_CLASS, pool.clone())?,
            attribute: HeapFile::open(SYS_ATTRIBUTE, pool.clone())?,
            index: HeapFile::open(SYS_INDEX, pool.clone())?,
            constraint: HeapFile::open(SYS_CONSTRAINT, pool)?,
            indexes,
            engine,
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    fn heap(&self, table: RelFileId) -> Result<&HeapFile> {
        Ok(match table {
            SYS_CLASS => &self.class,
            SYS_ATTRIBUTE => &self.attribute,
            SYS_INDEX => &self.index,
            SYS_CONSTRAINT => &self.constraint,
            _ => return Err(CatalogError::Corrupt("not a system table")),
        })
    }

    fn sys_index(&self, rel: RelFileId) -> Result<&BTree> {
        self.indexes
            .iter()
            .find(|(i, _)| i.rel == rel)
            .map(|(_, t)| t)
            .ok_or(CatalogError::Corrupt("not a system index"))
    }

    /// Encodes the key of system index `idx` for a row of its table.
    fn key_for(idx: &SysIndex, row: &[Value]) -> Result<Vec<u8>> {
        let desc = desc_of(idx.table)?;
        let cols: Vec<usize> = idx.columns.iter().map(|&c| usize::from(c) - 1).collect();
        let types: Vec<DataType> = cols.iter().map(|&c| desc.attrs[c]).collect();
        let values: Vec<Value> = cols.iter().map(|&c| row[c].clone()).collect();
        let mut k = Vec::new();
        encode_key(&types, &values, &mut k)?;
        Ok(k)
    }

    /// Inserts a catalog row and its entries in the table's system indexes
    /// (no uniqueness check; used by bootstrap and after explicit checks).
    fn insert_row(&self, table: RelFileId, row: &[Value], xid: Xid, cid: CommandId) -> Result<Tid> {
        let t = form_tuple(&desc_of(table)?, row, xid, cid)?;
        let tid = self.heap(table)?.insert(&t)?;
        for (idx, tree) in &self.indexes {
            if idx.table == table {
                tree.insert(&Self::key_for(idx, row)?, tid)?;
            }
        }
        Ok(tid)
    }

    /// Rows of `table` visible to `txn` whose system index `idx` key starts
    /// with the encoding of `prefix` (typed as the index's first columns).
    fn lookup(
        &self,
        txn: &mut Transaction,
        idx: RelFileId,
        prefix_types: &[DataType],
        prefix: &[Value],
    ) -> Result<Vec<(Tid, Vec<Value>)>> {
        let tree = self.sys_index(idx)?;
        let table = SYS_INDEXES
            .iter()
            .find(|i| i.rel == idx)
            .map(|i| i.table)
            .ok_or(CatalogError::Corrupt("not a system index"))?;
        let heap = self.heap(table)?;
        let desc = desc_of(table)?;
        let mut key = Vec::new();
        encode_key(prefix_types, prefix, &mut key)?;
        let snap = txn.snapshot();
        let me = Me {
            xid: txn.xid(),
            cid: txn.command_id(),
        };
        let mut out = Vec::new();
        for e in tree.scan(Bound::Included(key.clone()), Bound::Unbounded)? {
            let (k, tid) = e?;
            if !k.starts_with(&key) {
                break;
            }
            if let Some(row) = heap.fetch_visible(tid, &snap, &me, txn.tm())? {
                out.push((tid, deform_tuple(&desc, &row)?));
            }
        }
        Ok(out)
    }

    /// Inserts a catalog row for `txn`, checking its unique system indexes
    /// (waiting for transactions whose rows are in flux). On a duplicate,
    /// the transaction must be aborted (its heap row is left behind).
    fn insert_row_checked(
        &self,
        txn: &mut Transaction,
        table: RelFileId,
        row: &[Value],
        what: &str,
    ) -> Result<Tid> {
        let xid = txn.write_xid()?;
        let t = form_tuple(&desc_of(table)?, row, xid, txn.command_id())?;
        let heap = self.heap(table)?;
        let tid = heap.insert(&t)?;
        txn.count_insert(table.0);
        for (idx, tree) in &self.indexes {
            if idx.table != table {
                continue;
            }
            let key = Self::key_for(idx, row)?;
            if !idx.unique {
                tree.insert(&key, tid)?;
                continue;
            }
            loop {
                let check = |t| {
                    heap.unique_check(t, Some(xid), txn.tm())
                        .map_err(|e| BTreeError::External(Box::new(e)))
                };
                match tree.insert_unique(&key, tid, &check)? {
                    InsertOutcome::Inserted => break,
                    InsertOutcome::Duplicate => {
                        return Err(CatalogError::DuplicateRelation(what.to_owned()));
                    }
                    InsertOutcome::Wait(x) => txn.wait_for(x)?,
                }
            }
        }
        Ok(tid)
    }

    /// Creates table `name` within `txn` (visible to others once `txn`
    /// commits; its file is deleted if `txn` aborts).
    pub fn create_table(
        &self,
        txn: &mut Transaction,
        name: &str,
        columns: &[Column],
    ) -> Result<RelationInfo> {
        self.create_table_with(txn, name, columns, &[])
    }

    /// Creates table `name` with `constraints`: PRIMARY KEY and UNIQUE get
    /// their unique index (named like the constraint), PRIMARY KEY columns
    /// become NOT NULL, CHECK expressions are stored as given.
    pub fn create_table_with(
        &self,
        txn: &mut Transaction,
        name: &str,
        columns: &[Column],
        constraints: &[NewConstraint],
    ) -> Result<RelationInfo> {
        let mut columns = columns.to_vec();
        let attnums = |cols: &[String], columns: &[Column]| -> Result<Vec<u16>> {
            cols.iter()
                .map(|c| {
                    columns
                        .iter()
                        .position(|col| col.name == *c)
                        .map(|i| (i + 1) as u16)
                        .ok_or_else(|| CatalogError::KeyColumnMissing(c.clone()))
                })
                .collect()
        };
        let mut primary = false;
        for (i, c) in constraints.iter().enumerate() {
            if constraints[..i].iter().any(|d| d.name == c.name) {
                return Err(CatalogError::DuplicateConstraint {
                    name: c.name.clone(),
                    table: name.to_owned(),
                });
            }
            match &c.kind {
                ConstraintKind::PrimaryKey(cols) => {
                    if primary {
                        return Err(CatalogError::MultiplePrimaryKeys(name.to_owned()));
                    }
                    primary = true;
                    for a in attnums(cols, &columns)? {
                        columns[usize::from(a) - 1].not_null = true;
                    }
                }
                ConstraintKind::Unique(cols) => {
                    attnums(cols, &columns)?;
                }
                ConstraintKind::Check(_) => {}
                ConstraintKind::ForeignKey { columns: cols, .. } => {
                    attnums(cols, &columns)?;
                }
            }
        }
        let columns = &columns[..];
        txn.write_xid()?;
        let oid = self.engine.allocate_oid()?;
        let rel = RelFileId(oid);
        self.insert_row_checked(
            txn,
            SYS_CLASS,
            &class_row(oid, name, RELKIND_TABLE, rel, columns.len()),
            name,
        )?;
        for (i, c) in columns.iter().enumerate() {
            self.insert_row_checked(txn, SYS_ATTRIBUTE, &attribute_row(oid, i + 1, c), name)?;
        }
        self.engine.create_relation(rel)?;
        txn.drop_on_abort(rel);
        txn.lock_relation(rel, LockMode::AccessExclusive)?;
        txn.next_command();
        let info = RelationInfo {
            oid,
            name: name.to_owned(),
            kind: RelKind::Table,
            rel,
            columns: columns.to_vec(),
        };
        for c in constraints {
            let mut con = ConstraintInfo {
                oid: 0,
                name: c.name.clone(),
                table: oid,
                index: None,
                columns: Vec::new(),
                primary: false,
                check: None,
                foreign: None,
            };
            match &c.kind {
                ConstraintKind::PrimaryKey(cols) | ConstraintKind::Unique(cols) => {
                    con.columns = attnums(cols, columns)?;
                    con.primary = matches!(c.kind, ConstraintKind::PrimaryKey(_));
                    let idx = self.build_index(
                        txn,
                        &c.name,
                        &info,
                        con.columns.clone(),
                        true,
                        con.primary,
                    )?;
                    con.index = Some(idx.oid);
                }
                ConstraintKind::Check(expr) => con.check = Some(expr.clone()),
                ConstraintKind::ForeignKey {
                    columns: cols,
                    parent,
                    parent_columns,
                    on_delete,
                    on_update,
                } => {
                    let (child_cols, index, foreign) = self.resolve_foreign_key(
                        txn,
                        &info,
                        &c.name,
                        &attnums(cols, columns)?,
                        parent,
                        parent_columns.as_deref(),
                    )?;
                    con.columns = child_cols;
                    con.index = Some(index);
                    con.foreign = Some(ForeignInfo {
                        on_delete: *on_delete,
                        on_update: *on_update,
                        ..foreign
                    });
                }
            }
            let coid = self.engine.allocate_oid()?;
            self.insert_row_checked(txn, SYS_CONSTRAINT, &constraint_row(coid, &con), &c.name)?;
            // Later constraints (a self-referencing foreign key) see it.
            txn.next_command();
        }
        txn.next_command();
        Ok(info)
    }

    /// Constraints of the table with OID `table`, by OID.
    pub fn constraints_of(&self, txn: &mut Transaction, table: u32) -> Result<Vec<ConstraintInfo>> {
        let rows = self.lookup(
            txn,
            SYS_CONSTRAINT_CONRELID_INDEX,
            &[DataType::Int8],
            &[Value::Int8(i64::from(table))],
        )?;
        let mut out = Vec::new();
        for (_, r) in rows {
            let index = int(&r, 4)? as u32;
            out.push(ConstraintInfo {
                oid: int(&r, 0)? as u32,
                name: text(&r, 1)?,
                table: int(&r, 3)? as u32,
                index: (index != 0).then_some(index),
                columns: parse_indkey(&text(&r, 5)?)?,
                primary: text(&r, 2)? == CONTYPE_PRIMARY,
                check: opt_text(&r, 6)?,
                foreign: foreign_of(&r)?,
            });
        }
        out.sort_by_key(|c| c.oid);
        Ok(out)
    }

    /// Foreign keys (of any table) referencing the table with OID
    /// `parent`, by OID. (sys_constraint has no index on confrelid; it is
    /// scanned.)
    pub fn referencing(&self, txn: &mut Transaction, parent: u32) -> Result<Vec<ConstraintInfo>> {
        let snap = txn.snapshot();
        let me = Me {
            xid: txn.xid(),
            cid: txn.command_id(),
        };
        let mut out = Vec::new();
        for r in self.heap(SYS_CONSTRAINT)?.scan_mvcc(snap, me, txn.tm())? {
            let r = deform_tuple(&constraint_desc(), &r?.1)?;
            let Some(foreign) = foreign_of(&r)? else {
                continue;
            };
            if foreign.parent != parent {
                continue;
            }
            let index = int(&r, 4)? as u32;
            out.push(ConstraintInfo {
                oid: int(&r, 0)? as u32,
                name: text(&r, 1)?,
                table: int(&r, 3)? as u32,
                index: (index != 0).then_some(index),
                columns: parse_indkey(&text(&r, 5)?)?,
                primary: false,
                check: None,
                foreign: Some(foreign),
            });
        }
        out.sort_by_key(|c| c.oid);
        Ok(out)
    }

    /// Finds the unique index of `parent` a foreign key references and
    /// orders the key columns like it: (child columns, index OID, info).
    fn resolve_foreign_key(
        &self,
        txn: &mut Transaction,
        child: &RelationInfo,
        name: &str,
        child_cols: &[u16],
        parent: &str,
        parent_columns: Option<&[String]>,
    ) -> Result<(Vec<u16>, u32, ForeignInfo)> {
        let p = if parent == child.name {
            child.clone()
        } else {
            let p = self
                .lookup_relation(txn, parent)?
                .filter(|r| r.kind == RelKind::Table)
                .ok_or_else(|| CatalogError::UndefinedRelation(parent.to_owned()))?;
            // Keeps the parent's definition stable (PostgreSQL takes
            // ShareRowExclusive on the referenced table).
            txn.lock_relation(RelFileId(p.oid), LockMode::ShareRowExclusive)?;
            p
        };
        let parent_cols: Vec<u16> = match parent_columns {
            Some(names) => names
                .iter()
                .map(|n| {
                    p.columns
                        .iter()
                        .position(|c| c.name == *n)
                        .map(|i| (i + 1) as u16)
                        .ok_or_else(|| CatalogError::KeyColumnMissing(n.clone()))
                })
                .collect::<Result<_>>()?,
            None => self
                .constraints_of(txn, p.oid)?
                .into_iter()
                .find(|c| c.primary)
                .map(|c| c.columns)
                .ok_or_else(|| {
                    CatalogError::InvalidForeignKey(format!(
                        "there is no primary key for referenced table \"{}\"",
                        p.name
                    ))
                })?,
        };
        if parent_cols.len() != child_cols.len() {
            return Err(CatalogError::InvalidForeignKey(
                "number of referencing and referenced columns for foreign key disagree".into(),
            ));
        }
        let mut wanted = parent_cols.clone();
        wanted.sort_unstable();
        let index = self
            .indexes_of(txn, p.oid)?
            .into_iter()
            .find(|i| {
                let mut cols = i.columns.clone();
                cols.sort_unstable();
                i.unique && cols == wanted
            })
            .ok_or_else(|| {
                CatalogError::InvalidForeignKey(format!(
                    "there is no unique constraint matching given keys for referenced table \"{}\"",
                    p.name
                ))
            })?;
        // Order the pairs like the index columns.
        let mut ordered_child = Vec::new();
        for ic in &index.columns {
            let k = parent_cols
                .iter()
                .position(|c| c == ic)
                .ok_or(CatalogError::Corrupt("index column not in key"))?;
            let (cc, pc) = (child_cols[k], *ic);
            let ct = &child.columns[usize::from(cc) - 1];
            let pt = &p.columns[usize::from(pc) - 1];
            if !compatible_key_types(ct.ty, pt.ty) {
                return Err(CatalogError::ForeignKeyTypes {
                    name: name.to_owned(),
                    child: ct.name.clone(),
                    parent: pt.name.clone(),
                    child_ty: ct.ty,
                    parent_ty: pt.ty,
                });
            }
            ordered_child.push(cc);
        }
        Ok((
            ordered_child,
            index.oid,
            ForeignInfo {
                parent: p.oid,
                parent_columns: index.columns.clone(),
                on_delete: FkAction::NoAction,
                on_update: FkAction::NoAction,
            },
        ))
    }

    /// A free relation name: `base`, else `base1`, `base2`, ...
    /// (PostgreSQL's ChooseRelationName).
    pub fn choose_relation_name(&self, txn: &mut Transaction, base: &str) -> Result<String> {
        let mut n = base.to_owned();
        let mut k = 0;
        while self.lookup_relation(txn, &n)?.is_some() {
            k += 1;
            n = format!("{base}{k}");
        }
        Ok(n)
    }

    /// Drops table `name` and its indexes within `txn` (files are deleted
    /// when `txn` commits).
    pub fn drop_table(&self, txn: &mut Transaction, name: &str) -> Result<()> {
        let info = self
            .lookup_relation(txn, name)?
            .filter(|r| r.kind == RelKind::Table)
            .ok_or_else(|| CatalogError::UndefinedRelation(name.to_owned()))?;
        txn.lock_relation(RelFileId(info.oid), LockMode::AccessExclusive)?;
        // It may have been dropped while we waited for the lock.
        txn.statement_snapshot();
        let info = self
            .lookup_relation(txn, name)?
            .filter(|r| r.kind == RelKind::Table && r.oid == info.oid)
            .ok_or_else(|| CatalogError::UndefinedRelation(name.to_owned()))?;
        if self
            .referencing(txn, info.oid)?
            .iter()
            .any(|c| c.table != info.oid)
        {
            return Err(CatalogError::DependentObjects {
                what: format!("table {name}"),
            });
        }
        for idx in self.indexes_of(txn, info.oid)? {
            self.drop_index_rows(txn, idx.oid)?;
            txn.drop_on_commit(idx.rel);
        }
        for (tid, _) in self.lookup(
            txn,
            SYS_CONSTRAINT_CONRELID_INDEX,
            &[DataType::Int8],
            &[Value::Int8(i64::from(info.oid))],
        )? {
            self.delete_row(txn, SYS_CONSTRAINT, tid)?;
        }
        let oid = Value::Int8(i64::from(info.oid));
        for (tid, _) in self.lookup(
            txn,
            SYS_ATTRIBUTE_RELID_ATTNUM_INDEX,
            &[DataType::Int8],
            std::slice::from_ref(&oid),
        )? {
            self.delete_row(txn, SYS_ATTRIBUTE, tid)?;
        }
        for (tid, _) in self.lookup(txn, SYS_CLASS_OID_INDEX, &[DataType::Int8], &[oid])? {
            self.delete_row(txn, SYS_CLASS, tid)?;
        }
        txn.drop_on_commit(info.rel);
        txn.next_command();
        Ok(())
    }

    /// Creates index `name` on table `table` over `columns` within `txn`,
    /// indexing the table's existing rows. Takes a Share lock on the table
    /// (writers wait; DESIGN r10). A unique index fails on duplicate data.
    pub fn create_index(
        &self,
        txn: &mut Transaction,
        name: &str,
        table: &str,
        columns: &[&str],
        unique: bool,
    ) -> Result<IndexInfo> {
        let t = self
            .lookup_relation(txn, table)?
            .filter(|r| r.kind == RelKind::Table)
            .ok_or_else(|| CatalogError::UndefinedRelation(table.to_owned()))?;
        txn.lock_relation(RelFileId(t.oid), LockMode::Share)?;
        txn.statement_snapshot();
        let t = self
            .lookup_relation(txn, table)?
            .filter(|r| r.kind == RelKind::Table && r.oid == t.oid)
            .ok_or_else(|| CatalogError::UndefinedRelation(table.to_owned()))?;
        let attnums = columns
            .iter()
            .map(|c| {
                t.columns
                    .iter()
                    .position(|col| col.name == *c)
                    .map(|i| (i + 1) as u16)
                    .ok_or_else(|| CatalogError::UndefinedColumn((*c).to_owned()))
            })
            .collect::<Result<Vec<u16>>>()?;

        self.build_index(txn, name, &t, attnums, unique, false)
    }

    /// Creates and fills index `name` on locked table `t`.
    fn build_index(
        &self,
        txn: &mut Transaction,
        name: &str,
        t: &RelationInfo,
        attnums: Vec<u16>,
        unique: bool,
        primary: bool,
    ) -> Result<IndexInfo> {
        txn.write_xid()?;
        let oid = self.engine.allocate_oid()?;
        let rel = RelFileId(oid);
        self.insert_row_checked(
            txn,
            SYS_CLASS,
            &class_row(oid, name, RELKIND_INDEX, rel, attnums.len()),
            name,
        )?;
        self.insert_row_checked(
            txn,
            SYS_INDEX,
            &index_row(oid, t.oid, &attnums, unique, primary),
            name,
        )?;
        self.engine.create_relation(rel)?;
        txn.drop_on_abort(rel);
        let tree = BTree::create(rel, self.engine.pool().clone())?;
        let info = IndexInfo {
            oid,
            name: name.to_owned(),
            rel,
            table: t.oid,
            columns: attnums,
            unique,
            primary,
        };

        // Index every version some snapshot may still see.
        let heap = HeapFile::open(t.rel, self.engine.pool().clone())?;
        let horizon = txn.tm().oldest_xmin();
        let desc = t.desc();
        for blk in 0..heap.nblocks()? {
            for (tid, tuple) in heap.page_tuples(crate::storage::page::BlockNumber(blk))? {
                let h =
                    crate::access::tuple::TupleHeader::decode(&tuple).map_err(HeapError::from)?;
                if crate::access::heap::satisfies_vacuum(&h, horizon, txn.tm())?
                    == crate::access::heap::VacuumState::Dead
                {
                    continue;
                }
                let row = deform_tuple(&desc, &tuple)?;
                self.insert_entry(txn, t, &info, &tree, &heap, &row, tid)?;
            }
        }
        txn.next_command();
        Ok(info)
    }

    /// Drops index `name` within `txn` (file deleted at commit).
    pub fn drop_index(&self, txn: &mut Transaction, name: &str) -> Result<()> {
        let idx = self
            .lookup_relation(txn, name)?
            .filter(|r| r.kind == RelKind::Index)
            .ok_or_else(|| CatalogError::UndefinedRelation(name.to_owned()))?;
        let table = self
            .index_table(txn, idx.oid)?
            .ok_or(CatalogError::Corrupt("index without sys_index row"))?;
        txn.lock_relation(RelFileId(table), LockMode::AccessExclusive)?;
        if self
            .constraints_of(txn, table)?
            .iter()
            .any(|c| c.index == Some(idx.oid))
        {
            let table = self
                .lookup_oid(txn, table)?
                .map_or_else(String::new, |t| t.name);
            return Err(CatalogError::IndexRequiredByConstraint {
                index: name.to_owned(),
                table,
            });
        }
        if self
            .referencing(txn, table)?
            .iter()
            .any(|c| c.index == Some(idx.oid))
        {
            return Err(CatalogError::DependentObjects {
                what: format!("index {name}"),
            });
        }
        self.drop_index_rows(txn, idx.oid)?;
        txn.drop_on_commit(idx.rel);
        txn.next_command();
        Ok(())
    }

    /// The table an index belongs to.
    fn index_table(&self, txn: &mut Transaction, index_oid: u32) -> Result<Option<u32>> {
        let snap = txn.snapshot();
        let me = Me {
            xid: txn.xid(),
            cid: txn.command_id(),
        };
        for r in self.heap(SYS_INDEX)?.scan_mvcc(snap, me, txn.tm())? {
            let row = deform_tuple(&index_desc(), &r?.1)?;
            if int(&row, 0)? == i64::from(index_oid) {
                return Ok(Some(int(&row, 1)? as u32));
            }
        }
        Ok(None)
    }

    /// Encodes `idx`'s key for a row of `table`.
    pub fn index_key(table: &RelationInfo, idx: &IndexInfo, row: &[Value]) -> Result<Vec<u8>> {
        let mut types = Vec::with_capacity(idx.columns.len());
        let mut values = Vec::with_capacity(idx.columns.len());
        for &c in &idx.columns {
            let i = usize::from(c) - 1;
            types.push(
                table
                    .columns
                    .get(i)
                    .ok_or(CatalogError::Corrupt("indkey out of range"))?
                    .ty,
            );
            values.push(
                row.get(i)
                    .cloned()
                    .ok_or(CatalogError::Corrupt("row too short"))?,
            );
        }
        let mut k = Vec::new();
        encode_key(&types, &values, &mut k)?;
        Ok(k)
    }

    /// Adds the index entries for a row just inserted at `tid` into
    /// `table`, enforcing unique indexes (for the executor's INSERT and
    /// UPDATE). On a violation the transaction must be aborted.
    pub fn insert_index_entries(
        &self,
        txn: &mut Transaction,
        table: &RelationInfo,
        row: &[Value],
        tid: Tid,
    ) -> Result<()> {
        let heap = HeapFile::open(table.rel, self.engine.pool().clone())?;
        for idx in self.indexes_of(txn, table.oid)? {
            let tree = BTree::open(idx.rel, self.engine.pool().clone())?;
            self.insert_entry(txn, table, &idx, &tree, &heap, row, tid)?;
        }
        Ok(())
    }

    /// The indexes of `table`, opened (for statements maintaining them row
    /// by row).
    pub fn table_indexes(
        &self,
        txn: &mut Transaction,
        table: &RelationInfo,
    ) -> Result<Vec<(IndexInfo, BTree)>> {
        self.indexes_of(txn, table.oid)?
            .into_iter()
            .map(|i| {
                let tree = BTree::open(i.rel, self.engine.pool().clone())?;
                Ok((i, tree))
            })
            .collect()
    }

    /// Adds the entry for a row at `tid` to one index of `table`, enforcing
    /// uniqueness; see [`Catalog::insert_index_entries`].
    #[allow(clippy::too_many_arguments)]
    pub fn insert_entry(
        &self,
        txn: &mut Transaction,
        table: &RelationInfo,
        idx: &IndexInfo,
        tree: &BTree,
        heap: &HeapFile,
        row: &[Value],
        tid: Tid,
    ) -> Result<()> {
        let key = Self::index_key(table, idx, row)?;
        // NULLs never conflict in a unique index (as in PostgreSQL).
        let has_null = idx
            .columns
            .iter()
            .any(|&c| row.get(usize::from(c) - 1).is_some_and(Value::is_null));
        if !idx.unique || has_null {
            tree.insert(&key, tid)?;
            return Ok(());
        }
        let xid = txn.write_xid()?;
        loop {
            let check = |t| {
                heap.unique_check(t, Some(xid), txn.tm())
                    .map_err(|e| BTreeError::External(Box::new(e)))
            };
            match tree.insert_unique(&key, tid, &check)? {
                InsertOutcome::Inserted => return Ok(()),
                InsertOutcome::Duplicate => {
                    return Err(CatalogError::UniqueViolation(idx.name.clone()));
                }
                InsertOutcome::Wait(x) => txn.wait_for(x)?,
            }
        }
    }

    /// Deletes the sys_index and sys_class rows of index `oid`.
    fn drop_index_rows(&self, txn: &mut Transaction, oid: u32) -> Result<()> {
        let v = Value::Int8(i64::from(oid));
        for (tid, _) in self.lookup(
            txn,
            SYS_CLASS_OID_INDEX,
            &[DataType::Int8],
            std::slice::from_ref(&v),
        )? {
            self.delete_row(txn, SYS_CLASS, tid)?;
        }
        // sys_index has no index on indexrelid: scan its rows for the table.
        let heap = self.heap(SYS_INDEX)?;
        let snap = txn.snapshot();
        let me = Me {
            xid: txn.xid(),
            cid: txn.command_id(),
        };
        let mut tids = Vec::new();
        for r in heap.scan_mvcc(snap, me, txn.tm())? {
            let (tid, row) = r?;
            if int(&deform_tuple(&index_desc(), &row)?, 0)? == i64::from(oid) {
                tids.push(tid);
            }
        }
        for tid in tids {
            self.delete_row(txn, SYS_INDEX, tid)?;
        }
        Ok(())
    }

    fn delete_row(&self, txn: &mut Transaction, table: RelFileId, tid: Tid) -> Result<()> {
        match self.heap(table)?.delete_waiting(tid, txn)? {
            TmResult::Ok(_) => {
                txn.count_delete(table.0);
                Ok(())
            }
            _ => Err(CatalogError::Corrupt("catalog row changed concurrently")),
        }
    }

    /// Vacuums the system tables and their indexes (dead catalog rows left
    /// by DROP and by aborted or repeated DDL).
    pub fn vacuum_system(&self, txn: &mut Transaction) -> Result<crate::vacuum::VacuumStats> {
        let mut total = crate::vacuum::VacuumStats::default();
        for (table, heap) in [
            (SYS_CLASS, &self.class),
            (SYS_ATTRIBUTE, &self.attribute),
            (SYS_INDEX, &self.index),
            (SYS_CONSTRAINT, &self.constraint),
        ] {
            let trees: Vec<&BTree> = self
                .indexes
                .iter()
                .filter(|(i, _)| i.table == table)
                .map(|(_, t)| t)
                .collect();
            let s = crate::vacuum::vacuum(heap, &trees, txn)?;
            total.heap_pages += s.heap_pages;
            total.tuples_removed += s.tuples_removed;
            total.index_entries_removed += s.index_entries_removed;
            total.index_pages_deleted += s.index_pages_deleted;
        }
        Ok(total)
    }

    /// The relation named `name`, as `txn` sees the catalogs.
    pub fn lookup_relation(
        &self,
        txn: &mut Transaction,
        name: &str,
    ) -> Result<Option<RelationInfo>> {
        let rows = self.lookup(
            txn,
            SYS_CLASS_RELNAME_INDEX,
            &[DataType::Text],
            &[Value::Text(name.to_owned())],
        )?;
        match rows.as_slice() {
            [] => Ok(None),
            [(_, row)] => Ok(Some(self.relation_from_row(txn, row)?)),
            _ => Err(CatalogError::Corrupt("two visible relations with one name")),
        }
    }

    /// The relation with OID `oid`.
    pub fn lookup_oid(&self, txn: &mut Transaction, oid: u32) -> Result<Option<RelationInfo>> {
        let rows = self.lookup(
            txn,
            SYS_CLASS_OID_INDEX,
            &[DataType::Int8],
            &[Value::Int8(i64::from(oid))],
        )?;
        match rows.as_slice() {
            [] => Ok(None),
            [(_, row)] => Ok(Some(self.relation_from_row(txn, row)?)),
            _ => Err(CatalogError::Corrupt("two visible relations with one oid")),
        }
    }

    fn relation_from_row(&self, txn: &mut Transaction, row: &[Value]) -> Result<RelationInfo> {
        let oid = int(row, 0)? as u32;
        let kind = match text(row, 2)?.as_str() {
            RELKIND_TABLE => RelKind::Table,
            RELKIND_INDEX => RelKind::Index,
            _ => return Err(CatalogError::Corrupt("unknown relkind")),
        };
        let mut columns = Vec::new();
        if kind == RelKind::Table {
            let mut attrs = self.lookup(
                txn,
                SYS_ATTRIBUTE_RELID_ATTNUM_INDEX,
                &[DataType::Int8],
                &[Value::Int8(i64::from(oid))],
            )?;
            attrs.sort_by_key(|(_, a)| int(a, 1).unwrap_or(0));
            for (_, a) in attrs {
                columns.push(Column {
                    name: text(&a, 2)?,
                    ty: oid_to_type(int(&a, 3)? as i32, int(&a, 4)? as i32)?,
                    not_null: boolean(&a, 5)?,
                    default: opt_text(&a, 6)?,
                });
            }
        }
        Ok(RelationInfo {
            oid,
            name: text(row, 1)?,
            kind,
            rel: RelFileId(int(row, 3)? as u32),
            columns,
        })
    }

    /// Indexes of the table with OID `table`.
    pub fn indexes_of(&self, txn: &mut Transaction, table: u32) -> Result<Vec<IndexInfo>> {
        let rows = self.lookup(
            txn,
            SYS_INDEX_INDRELID_INDEX,
            &[DataType::Int8],
            &[Value::Int8(i64::from(table))],
        )?;
        let mut out = Vec::new();
        for (_, r) in rows {
            let oid = int(&r, 0)? as u32;
            let rel = self
                .lookup_oid(txn, oid)?
                .ok_or(CatalogError::Corrupt("index without sys_class row"))?;
            out.push(IndexInfo {
                oid,
                name: rel.name,
                rel: rel.rel,
                table: int(&r, 1)? as u32,
                columns: parse_indkey(&text(&r, 2)?)?,
                unique: boolean(&r, 3)?,
                primary: boolean(&r, 4)?,
            });
        }
        out.sort_by_key(|i| i.oid);
        Ok(out)
    }
}

#[cfg(test)]
mod tests;
