//! Binder: resolves names against the catalog, checks and coerces types
//! with PostgreSQL's rules, and builds a logical [`Plan`] (DESIGN r11).
//!
//! Tables are looked up, locked, and looked up again (a concurrent DDL may
//! have committed while we waited for the lock); under Read Committed the
//! statement snapshot is retaken after each lock.

use sqlparser::ast;

use super::datum::parse_value;
use super::error::{Result, SqlError, code};
use super::expr::{BinOp, CastMode, Expr, Func, cast};
use super::plan::{AggCall, AggFunc, JoinKind, OutCol, Plan, SortKey};
use crate::catalog::{Catalog, Column, RelKind, RelationInfo};
use crate::lock::LockMode;
use crate::storage::backend::RelFileId;
use crate::txn::{Isolation, Transaction};
use crate::types::{DataType, Value};
use crate::wal::recovery::FIRST_OID as FIRST_USER_OID;

/// Aggregate references are bound as `Column(AGG_MARK + i)` until the
/// aggregation layout is known (no real row is this wide).
const AGG_MARK: usize = 1 << 40;

/// A bound statement.
#[derive(Debug)]
pub enum Bound {
    Query {
        plan: Plan,
        columns: Vec<OutCol>,
    },
    /// `source` yields complete rows of `table` (already coerced).
    Insert {
        table: RelationInfo,
        source: Plan,
    },
    /// `scan` is a scan of `table` with the WHERE clause as its filter;
    /// assignments are (column index, expression over the old row).
    Update {
        table: RelationInfo,
        scan: Plan,
        assignments: Vec<(usize, Expr)>,
    },
    Delete {
        table: RelationInfo,
        scan: Plan,
    },
}

/// Statements that need no binding against the catalog.
#[derive(Debug, Clone, PartialEq)]
pub enum Utility {
    Begin(Option<Isolation>),
    Commit,
    Rollback,
    SetTransaction(Isolation),
    CreateTable {
        name: String,
        columns: Vec<Column>,
        if_not_exists: bool,
    },
    DropTable {
        names: Vec<String>,
        if_exists: bool,
    },
    CreateIndex {
        name: Option<String>,
        table: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        names: Vec<String>,
        if_exists: bool,
    },
}

/// Classified statement.
#[derive(Debug)]
pub enum Kind<'a> {
    Utility(Utility),
    /// Needs binding (query or DML).
    Plannable(&'a ast::Statement),
}

/// The type of a bound expression before coercion.
#[derive(Debug, Clone, PartialEq)]
enum Ty {
    Known(DataType),
    /// A string literal or NULL, typed by context (PostgreSQL's unknown).
    Unknown,
    /// A decimal literal (numeric in PostgreSQL), with its text.
    Numeric(String),
}

impl Ty {
    fn name(&self) -> String {
        match self {
            Ty::Known(t) => t.to_string(),
            Ty::Unknown => "unknown".to_owned(),
            Ty::Numeric(_) => "numeric".to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
struct BExpr {
    expr: Expr,
    ty: Ty,
}

impl BExpr {
    fn known(expr: Expr, ty: DataType) -> Self {
        Self {
            expr,
            ty: Ty::Known(ty),
        }
    }
}

#[derive(Debug, Clone)]
struct ScopeCol {
    table: String,
    name: String,
    ty: DataType,
}

#[derive(Debug, Clone, Default)]
struct Scope {
    cols: Vec<ScopeCol>,
}

fn err(code: &'static str, msg: impl Into<String>) -> SqlError {
    SqlError::new(code, msg)
}

fn numeric_unsupported() -> SqlError {
    err(
        code::FEATURE_NOT_SUPPORTED,
        "type numeric is not supported yet",
    )
}

/// An identifier as PostgreSQL folds it: unquoted names are lowercased.
pub fn ident(i: &ast::Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_ascii_lowercase()
    }
}

fn object_name(n: &ast::ObjectName) -> Result<String> {
    let parts: Vec<&ast::Ident> = n.0.iter().filter_map(|p| p.as_ident()).collect();
    match parts.as_slice() {
        [i] => Ok(ident(i)),
        [schema, i] if ident(schema) == "public" => Ok(ident(i)),
        _ => Err(SqlError::unsupported(format!("qualified name {n}"))),
    }
}

/// Maps a SQL type name to a column type.
pub fn data_type(t: &ast::DataType) -> Result<DataType> {
    use ast::DataType as T;
    Ok(match t {
        T::Int(None) | T::Integer(None) | T::Int4(None) => DataType::Int4,
        T::BigInt(None) | T::Int8(None) => DataType::Int8,
        T::Bool | T::Boolean => DataType::Bool,
        T::Text => DataType::Text,
        T::Varchar(len) | T::CharacterVarying(len) | T::CharVarying(len) => {
            DataType::Varchar(match len {
                None => None,
                Some(ast::CharacterLength::IntegerLength { length, .. }) => {
                    if *length == 0 || *length > 10_485_760 {
                        return Err(err(
                            code::INVALID_PARAMETER_VALUE,
                            "length for type varchar must be between 1 and 10485760",
                        ));
                    }
                    Some(*length as u32)
                }
                Some(ast::CharacterLength::Max) => {
                    return Err(SqlError::unsupported("VARCHAR(MAX)"));
                }
            })
        }
        T::DoublePrecision | T::Float8 | T::Double(ast::ExactNumberInfo::None) => DataType::Float8,
        T::Float(ast::ExactNumberInfo::None) => DataType::Float8,
        T::Float(ast::ExactNumberInfo::Precision(p)) if (25..=53).contains(p) => DataType::Float8,
        T::Timestamp(None, ast::TimezoneInfo::None | ast::TimezoneInfo::WithoutTimeZone) => {
            DataType::Timestamp
        }
        T::Numeric(_) | T::Decimal(_) => return Err(numeric_unsupported()),
        other => {
            return Err(err(
                code::FEATURE_NOT_SUPPORTED,
                format!("type \"{other}\" is not supported"),
            ));
        }
    })
}

fn isolation(level: &ast::TransactionIsolationLevel) -> Result<Isolation> {
    use ast::TransactionIsolationLevel as L;
    match level {
        L::ReadUncommitted | L::ReadCommitted => Ok(Isolation::ReadCommitted),
        L::RepeatableRead | L::Snapshot => Ok(Isolation::SnapshotIsolation),
        L::Serializable => Err(SqlError::unsupported(
            "SERIALIZABLE isolation (use REPEATABLE READ)",
        )),
    }
}

fn modes_isolation(modes: &[ast::TransactionMode]) -> Result<Option<Isolation>> {
    let mut iso = None;
    for m in modes {
        match m {
            ast::TransactionMode::IsolationLevel(l) => iso = Some(isolation(l)?),
            ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadWrite) => {}
            ast::TransactionMode::AccessMode(_) => {
                return Err(SqlError::unsupported("READ ONLY transactions"));
            }
        }
    }
    Ok(iso)
}

fn column_def(c: &ast::ColumnDef) -> Result<Column> {
    for o in &c.options {
        match o.option {
            ast::ColumnOption::Null => {}
            _ => {
                return Err(SqlError::unsupported(format!(
                    "column constraint {} (until step 10)",
                    o.option
                )));
            }
        }
    }
    Ok(Column {
        name: ident(&c.name),
        ty: data_type(&c.data_type)?,
        not_null: false,
    })
}

/// Splits off statements that need no catalog binding.
pub fn classify(stmt: &ast::Statement) -> Result<Kind<'_>> {
    use ast::Statement as S;
    Ok(Kind::Utility(match stmt {
        S::Query(_) | S::Insert(_) | S::Update(_) | S::Delete(_) => {
            return Ok(Kind::Plannable(stmt));
        }
        S::StartTransaction { modes, .. } => Utility::Begin(modes_isolation(modes)?),
        S::Commit { chain: false, .. } => Utility::Commit,
        S::Rollback {
            chain: false,
            savepoint: None,
        } => Utility::Rollback,
        S::Set(ast::Set::SetTransaction {
            modes,
            snapshot: None,
            session: false,
        }) => match modes_isolation(modes)? {
            Some(i) => Utility::SetTransaction(i),
            None => return Err(SqlError::unsupported(format!("statement {stmt}"))),
        },
        S::CreateTable(ct) => {
            if ct.query.is_some() || !ct.constraints.is_empty() || ct.temporary {
                return Err(SqlError::unsupported(format!("statement {stmt}")));
            }
            let columns = ct
                .columns
                .iter()
                .map(column_def)
                .collect::<Result<Vec<_>>>()?;
            for (i, c) in columns.iter().enumerate() {
                if columns[..i].iter().any(|d| d.name == c.name) {
                    return Err(err(
                        code::DUPLICATE_COLUMN,
                        format!("column \"{}\" specified more than once", c.name),
                    ));
                }
            }
            Utility::CreateTable {
                name: object_name(&ct.name)?,
                columns,
                if_not_exists: ct.if_not_exists,
            }
        }
        S::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            let names = names.iter().map(object_name).collect::<Result<Vec<_>>>()?;
            let if_exists = *if_exists;
            match object_type {
                ast::ObjectType::Table => Utility::DropTable { names, if_exists },
                ast::ObjectType::Index => Utility::DropIndex { names, if_exists },
                _ => return Err(SqlError::unsupported(format!("statement {stmt}"))),
            }
        }
        S::CreateIndex(ci) => {
            if ci.concurrently || ci.predicate.is_some() || !ci.include.is_empty() {
                return Err(SqlError::unsupported(format!("statement {stmt}")));
            }
            if let Some(u) = &ci.using
                && !u.to_string().eq_ignore_ascii_case("btree")
            {
                return Err(SqlError::unsupported(format!("index method {u}")));
            }
            let mut columns = Vec::new();
            for c in &ci.columns {
                let o = &c.column.options;
                let default_order = matches!(o.sort, None | Some(ast::OrderBySort::Asc))
                    && o.nulls_first != Some(true);
                match (&c.column.expr, default_order, &c.operator_class) {
                    (ast::Expr::Identifier(i), true, None) => columns.push(ident(i)),
                    _ => return Err(SqlError::unsupported(format!("index column {}", c.column))),
                }
            }
            Utility::CreateIndex {
                name: ci.name.as_ref().map(object_name).transpose()?,
                table: object_name(&ci.table_name)?,
                columns,
                unique: ci.unique,
                if_not_exists: ci.if_not_exists,
            }
        }
        _ => return Err(SqlError::unsupported(format!("statement {stmt}"))),
    }))
}

/// Numeric ordering of the number types for implicit promotion.
fn numeric_rank(t: DataType) -> Option<u8> {
    match t {
        DataType::Int4 => Some(1),
        DataType::Int8 => Some(2),
        DataType::Float8 => Some(3),
        _ => None,
    }
}

fn is_text(t: DataType) -> bool {
    matches!(t, DataType::Text | DataType::Varchar(_))
}

/// Coerces `b` to `to`. Implicit coercions are those PostgreSQL applies
/// in operators and comparisons; assignment adds the casts allowed when
/// storing into a column; explicit adds the rest of the cast table.
fn coerce(b: BExpr, to: DataType, mode: Coercion) -> Result<Expr> {
    let fail = |b: &BExpr| match mode {
        Coercion::Explicit => err(
            code::CANNOT_COERCE,
            format!("cannot cast type {} to {to}", b.ty.name()),
        ),
        _ => err(
            code::DATATYPE_MISMATCH,
            format!("expected type {to}, found type {}", b.ty.name()),
        ),
    };
    let cast_mode = if mode == Coercion::Explicit {
        CastMode::Explicit
    } else {
        CastMode::Assignment
    };
    match &b.ty {
        Ty::Unknown => match b.expr {
            Expr::Const(Value::Null) => Ok(Expr::Const(Value::Null)),
            Expr::Const(Value::Text(s)) => {
                let base = match to {
                    DataType::Varchar(_) => DataType::Text,
                    t => t,
                };
                let v = parse_value(base, &s)?;
                Ok(Expr::Const(cast(v, to, cast_mode)?))
            }
            _ => Err(SqlError::internal("unknown-typed non-literal")),
        },
        Ty::Numeric(text) => match to {
            DataType::Float8 => Ok(b.expr),
            DataType::Int4 | DataType::Int8 if mode != Coercion::Implicit => {
                let Expr::Const(Value::Float8(f)) = b.expr else {
                    return Err(SqlError::internal("numeric literal"));
                };
                // numeric to integer rounds half away from zero.
                Ok(Expr::Const(cast(
                    Value::Float8(f.round()),
                    to,
                    CastMode::Explicit,
                )?))
            }
            t if is_text(t) && mode != Coercion::Implicit => {
                Ok(Expr::Const(cast(Value::Text(text.clone()), to, cast_mode)?))
            }
            _ => Err(numeric_unsupported()),
        },
        Ty::Known(from) => {
            let from = *from;
            if from == to {
                return Ok(b.expr);
            }
            let implicit = match (from, to) {
                (f, t) if is_text(f) && is_text(t) => {
                    // A length limit only matters when storing.
                    if matches!(t, DataType::Varchar(Some(_))) && mode != Coercion::Implicit {
                        return Ok(Expr::Cast {
                            arg: Box::new(b.expr),
                            to,
                            mode: cast_mode,
                        });
                    }
                    return Ok(b.expr);
                }
                (f, t) => matches!((numeric_rank(f), numeric_rank(t)), (Some(x), Some(y)) if x < y),
            };
            let assignment = implicit
                || matches!((numeric_rank(from), numeric_rank(to)), (Some(_), Some(_)))
                || is_text(to);
            let explicit = assignment
                || is_text(from)
                || matches!(
                    (from, to),
                    (DataType::Bool, DataType::Int4) | (DataType::Int4, DataType::Bool)
                );
            let ok = match mode {
                Coercion::Implicit => implicit,
                Coercion::Assignment => assignment,
                Coercion::Explicit => explicit,
            };
            if !ok {
                return Err(fail(&b));
            }
            // Fold casts of constants (so the planner sees `col = const`).
            if let Expr::Const(v) = b.expr {
                return Ok(Expr::Const(cast(v, to, cast_mode)?));
            }
            Ok(Expr::Cast {
                arg: Box::new(b.expr),
                to,
                mode: cast_mode,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coercion {
    Implicit,
    Assignment,
    Explicit,
}

/// The common type of several expressions (PostgreSQL's
/// select_common_type for UNION/CASE/IN/COALESCE).
fn common_type(tys: &[&Ty], what: &str) -> Result<Ty> {
    let mut result: Option<DataType> = None;
    let mut numeric = false;
    for t in tys {
        match t {
            Ty::Unknown => {}
            Ty::Numeric(_) => numeric = true,
            Ty::Known(k) => {
                result = Some(match result {
                    None => *k,
                    Some(r) if r == *k => r,
                    Some(r) if is_text(r) && is_text(*k) => DataType::Text,
                    Some(r) => match (numeric_rank(r), numeric_rank(*k)) {
                        (Some(a), Some(b)) => {
                            if a >= b {
                                r
                            } else {
                                *k
                            }
                        }
                        _ => {
                            return Err(err(
                                code::DATATYPE_MISMATCH,
                                format!("{what} types {r} and {k} cannot be matched"),
                            ));
                        }
                    },
                });
            }
        }
    }
    Ok(match (result, numeric) {
        (None, false) => Ty::Known(DataType::Text),
        (None, true) => return Err(numeric_unsupported()),
        (Some(DataType::Float8), true) => Ty::Known(DataType::Float8),
        (Some(r), true) if numeric_rank(r).is_some() => return Err(numeric_unsupported()),
        (Some(r), true) => {
            return Err(err(
                code::DATATYPE_MISMATCH,
                format!("{what} types {r} and numeric cannot be matched"),
            ));
        }
        (Some(r), false) => Ty::Known(r),
    })
}

fn known(t: &Ty) -> Result<DataType> {
    match t {
        Ty::Known(k) => Ok(*k),
        Ty::Unknown => Ok(DataType::Text),
        Ty::Numeric(_) => Err(numeric_unsupported()),
    }
}

/// Where an expression is bound (for aggregate rules and messages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Clause {
    Where,
    On,
    GroupBy,
    Values,
    Update,
    Limit,
    /// SELECT list, HAVING, ORDER BY: aggregates allowed.
    Select,
}

impl Clause {
    fn name(self) -> &'static str {
        match self {
            Clause::Where => "WHERE",
            Clause::On => "JOIN conditions",
            Clause::GroupBy => "GROUP BY",
            Clause::Values => "VALUES",
            Clause::Update => "UPDATE",
            Clause::Limit => "LIMIT",
            Clause::Select => "SELECT",
        }
    }
}

/// One SELECT list entry after `*` expansion.
struct Item {
    expr: BExpr,
    name: String,
    /// The AST, for GROUP BY ordinal/alias references.
    ast: Option<ast::Expr>,
}

pub struct Binder<'a> {
    cat: &'a Catalog,
    txn: &'a mut Transaction,
    aggs: Vec<AggCall>,
    in_agg: bool,
}

impl<'a> Binder<'a> {
    pub fn new(cat: &'a Catalog, txn: &'a mut Transaction) -> Self {
        Self {
            cat,
            txn,
            aggs: Vec::new(),
            in_agg: false,
        }
    }

    pub fn bind(&mut self, stmt: &ast::Statement) -> Result<Bound> {
        use ast::Statement as S;
        match stmt {
            S::Query(q) => {
                let (plan, columns) = self.bind_query(q)?;
                Ok(Bound::Query { plan, columns })
            }
            S::Insert(i) => self.bind_insert(i),
            S::Update(u) => self.bind_update(u),
            S::Delete(d) => self.bind_delete(d),
            _ => Err(SqlError::unsupported(format!("statement {stmt}"))),
        }
    }

    /// Looks up, locks, and re-checks table `name`.
    fn open_table(&mut self, name: &str, mode: LockMode) -> Result<RelationInfo> {
        for _ in 0..16 {
            let Some(info) = self.cat.lookup_relation(self.txn, name)? else {
                return Err(err(
                    code::UNDEFINED_TABLE,
                    format!("relation \"{name}\" does not exist"),
                ));
            };
            if info.kind != RelKind::Table {
                return Err(err(
                    code::WRONG_OBJECT_TYPE,
                    format!("\"{name}\" is not a table"),
                ));
            }
            self.txn.lock_relation(RelFileId(info.oid), mode)?;
            self.txn.statement_snapshot();
            if self.cat.lookup_relation(self.txn, name)?.as_ref() == Some(&info) {
                return Ok(info);
            }
        }
        Err(SqlError::internal(format!(
            "relation \"{name}\" keeps changing"
        )))
    }

    fn open_target(&mut self, name: &ast::ObjectName) -> Result<RelationInfo> {
        let name = object_name(name)?;
        let t = self.open_table(&name, LockMode::RowExclusive)?;
        if t.oid < FIRST_USER_OID {
            return Err(err("42501", format!("permission denied for table {name}")));
        }
        Ok(t)
    }

    // ---- FROM ----

    fn bind_from(&mut self, from: &[ast::TableWithJoins]) -> Result<(Plan, Scope)> {
        let mut acc: Option<(Plan, Scope)> = None;
        for twj in from {
            let (mut plan, mut scope) = self.bind_factor(&twj.relation)?;
            for j in &twj.joins {
                let (rplan, rscope) = self.bind_factor(&j.relation)?;
                let (kind, constraint) = match &j.join_operator {
                    ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) => {
                        (JoinKind::Inner, Some(c))
                    }
                    ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => {
                        (JoinKind::Left, Some(c))
                    }
                    ast::JoinOperator::CrossJoin(ast::JoinConstraint::None) => {
                        (JoinKind::Inner, None)
                    }
                    other => {
                        return Err(SqlError::unsupported(format!("join type {other:?}")));
                    }
                };
                let joined = merge_scopes(scope, rscope)?;
                let on = match constraint {
                    None | Some(ast::JoinConstraint::None) => None,
                    Some(ast::JoinConstraint::On(e)) => {
                        Some(self.bind_bool(e, &joined, Clause::On)?)
                    }
                    Some(other) => {
                        return Err(SqlError::unsupported(format!("join constraint {other:?}")));
                    }
                };
                if kind == JoinKind::Left && on.is_none() {
                    return Err(err(
                        code::SYNTAX_ERROR,
                        "LEFT JOIN requires a join condition",
                    ));
                }
                plan = Plan::NestedLoop {
                    left: Box::new(plan),
                    right: Box::new(rplan),
                    kind,
                    on,
                };
                scope = joined;
            }
            acc = Some(match acc {
                None => (plan, scope),
                Some((lp, ls)) => (
                    Plan::NestedLoop {
                        left: Box::new(lp),
                        right: Box::new(plan),
                        kind: JoinKind::Inner,
                        on: None,
                    },
                    merge_scopes(ls, scope)?,
                ),
            });
        }
        Ok(acc.unwrap_or((
            Plan::Values {
                rows: vec![Vec::new()],
                width: 0,
            },
            Scope::default(),
        )))
    }

    fn bind_factor(&mut self, f: &ast::TableFactor) -> Result<(Plan, Scope)> {
        match f {
            ast::TableFactor::Table {
                name,
                alias,
                args: None,
                with_hints,
                version: None,
                ..
            } if with_hints.is_empty() => {
                let tname = object_name(name)?;
                let table = self.open_table(&tname, LockMode::AccessShare)?;
                let mut label = tname;
                let mut names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
                if let Some(a) = alias {
                    label = ident(&a.name);
                    if a.columns.len() > names.len() {
                        return Err(err(
                            code::INVALID_COLUMN_REFERENCE,
                            format!(
                                "table \"{label}\" has {} columns available but {} columns specified",
                                names.len(),
                                a.columns.len()
                            ),
                        ));
                    }
                    for (n, c) in names.iter_mut().zip(&a.columns) {
                        *n = ident(&c.name);
                    }
                }
                let cols = table
                    .columns
                    .iter()
                    .zip(names)
                    .map(|(c, name)| ScopeCol {
                        table: label.clone(),
                        name,
                        ty: c.ty,
                    })
                    .collect();
                Ok((
                    Plan::Scan {
                        table,
                        filter: None,
                    },
                    Scope { cols },
                ))
            }
            other => Err(SqlError::unsupported(format!("FROM item {other}"))),
        }
    }

    // ---- SELECT ----

    fn bind_query(&mut self, q: &ast::Query) -> Result<(Plan, Vec<OutCol>)> {
        if q.with.is_some()
            || q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            return Err(SqlError::unsupported(format!("query {q}")));
        }
        let order: &[ast::OrderByExpr] = match &q.order_by {
            None => &[],
            Some(ast::OrderBy {
                kind: ast::OrderByKind::Expressions(e),
                interpolate: None,
            }) => e,
            Some(o) => return Err(SqlError::unsupported(format!("{o}"))),
        };
        let (limit, offset) = self.bind_limit(q.limit_clause.as_ref())?;
        let (mut plan, columns) = match q.body.as_ref() {
            ast::SetExpr::Select(s) => self.bind_select(s, order)?,
            ast::SetExpr::Values(v) => {
                if !order.is_empty() {
                    return Err(SqlError::unsupported("ORDER BY on VALUES"));
                }
                self.bind_values(v)?
            }
            ast::SetExpr::Query(inner) if order.is_empty() => self.bind_query(inner)?,
            other => return Err(SqlError::unsupported(format!("query {other}"))),
        };
        if limit.is_some() || offset > 0 {
            plan = Plan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }
        Ok((plan, columns))
    }

    fn bind_limit(&mut self, l: Option<&ast::LimitClause>) -> Result<(Option<u64>, u64)> {
        let Some(l) = l else {
            return Ok((None, 0));
        };
        let (limit, offset) = match l {
            ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } if limit_by.is_empty() => (limit.as_ref(), offset.as_ref().map(|o| &o.value)),
            other => return Err(SqlError::unsupported(format!("{other}"))),
        };
        let eval = |b: &mut Self, e: Option<&ast::Expr>, what: &str| -> Result<Option<u64>> {
            let Some(e) = e else { return Ok(None) };
            let be = b.bind_expr(e, &Scope::default(), Clause::Limit)?;
            let x = coerce(be, DataType::Int8, Coercion::Assignment)?;
            match x.eval(&[])? {
                Value::Null => Ok(None),
                Value::Int8(n) if n < 0 => Err(err(
                    if what == "LIMIT" { "2201W" } else { "2201X" },
                    format!("{what} must not be negative"),
                )),
                Value::Int8(n) => Ok(Some(n as u64)),
                _ => Err(SqlError::internal("limit value")),
            }
        };
        let lim = eval(self, limit, "LIMIT")?;
        let off = eval(self, offset, "OFFSET")?.unwrap_or(0);
        Ok((lim, off))
    }

    fn bind_values(&mut self, v: &ast::Values) -> Result<(Plan, Vec<OutCol>)> {
        let width = v.rows.first().map_or(0, |r| r.len());
        let mut rows: Vec<Vec<BExpr>> = Vec::new();
        for r in &v.rows {
            if r.len() != width {
                return Err(err(
                    code::SYNTAX_ERROR,
                    "VALUES lists must all be the same length",
                ));
            }
            rows.push(
                r.iter()
                    .map(|e| self.bind_expr(e, &Scope::default(), Clause::Values))
                    .collect::<Result<_>>()?,
            );
        }
        let mut columns = Vec::new();
        let mut out: Vec<Vec<Expr>> = vec![Vec::new(); rows.len()];
        for c in 0..width {
            let tys: Vec<&Ty> = rows.iter().map(|r| &r[c].ty).collect();
            let ty = known(&common_type(&tys, "VALUES")?)?;
            for (i, r) in rows.iter().enumerate() {
                out[i].push(coerce(r[c].clone(), ty, Coercion::Implicit)?);
            }
            columns.push(OutCol {
                name: format!("column{}", c + 1),
                ty,
            });
        }
        Ok((Plan::Values { rows: out, width }, columns))
    }

    fn expand_items(&mut self, s: &ast::Select, scope: &Scope) -> Result<Vec<Item>> {
        let mut items = Vec::new();
        let col_item = |i: usize, c: &ScopeCol| Item {
            expr: BExpr::known(Expr::Column(i), c.ty),
            name: c.name.clone(),
            ast: None,
        };
        for p in &s.projection {
            match p {
                ast::SelectItem::UnnamedExpr(e) => items.push(Item {
                    expr: self.bind_expr(e, scope, Clause::Select)?,
                    name: column_name(e),
                    ast: Some(e.clone()),
                }),
                ast::SelectItem::ExprWithAlias { expr, alias } => items.push(Item {
                    expr: self.bind_expr(expr, scope, Clause::Select)?,
                    name: ident(alias),
                    ast: Some(expr.clone()),
                }),
                ast::SelectItem::Wildcard(o) if wildcard_plain(o) => {
                    if scope.cols.is_empty() {
                        return Err(err(
                            code::SYNTAX_ERROR,
                            "SELECT * with no tables specified is not valid",
                        ));
                    }
                    for (i, c) in scope.cols.iter().enumerate() {
                        items.push(col_item(i, c));
                    }
                }
                ast::SelectItem::QualifiedWildcard(
                    ast::SelectItemQualifiedWildcardKind::ObjectName(n),
                    o,
                ) if wildcard_plain(o) => {
                    let t = object_name(n)?;
                    let mut any = false;
                    for (i, c) in scope.cols.iter().enumerate() {
                        if c.table == t {
                            any = true;
                            items.push(col_item(i, c));
                        }
                    }
                    if !any {
                        return Err(err(
                            code::UNDEFINED_TABLE,
                            format!("missing FROM-clause entry for table \"{t}\""),
                        ));
                    }
                }
                other => return Err(SqlError::unsupported(format!("select item {other}"))),
            }
        }
        Ok(items)
    }

    fn bind_select(
        &mut self,
        s: &ast::Select,
        order: &[ast::OrderByExpr],
    ) -> Result<(Plan, Vec<OutCol>)> {
        if s.top.is_some()
            || s.into.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
            || !s.named_window.is_empty()
            || s.qualify.is_some()
            || !s.connect_by.is_empty()
        {
            return Err(SqlError::unsupported(format!("query {s}")));
        }
        let distinct = match &s.distinct {
            None | Some(ast::Distinct::All) => false,
            Some(ast::Distinct::Distinct) => true,
            Some(other) => return Err(SqlError::unsupported(format!("{other}"))),
        };
        let (mut plan, scope) = self.bind_from(&s.from)?;
        if let Some(w) = &s.selection {
            let pred = self.bind_bool(w, &scope, Clause::Where)?;
            plan = Plan::Filter {
                input: Box::new(plan),
                pred,
            };
        }

        self.aggs.clear();
        let items = self.expand_items(s, &scope)?;

        // GROUP BY: input columns first, then output names and ordinals.
        let group_asts: &[ast::Expr] = match &s.group_by {
            ast::GroupByExpr::Expressions(e, m) if m.is_empty() => e,
            other => return Err(SqlError::unsupported(format!("{other}"))),
        };
        let mut groups: Vec<Expr> = Vec::new();
        for g in group_asts {
            let e = match self.output_ref(g, &items, &scope)? {
                Some(i) => {
                    let ast = items[i].ast.clone().ok_or_else(|| {
                        SqlError::internal("GROUP BY reference to a wildcard column")
                    });
                    match ast {
                        Ok(a) => self.bind_expr(&a, &scope, Clause::GroupBy)?.expr,
                        Err(_) => items[i].expr.expr.clone(),
                    }
                }
                None => self.bind_expr(g, &scope, Clause::GroupBy)?.expr,
            };
            if !groups.contains(&e) {
                groups.push(e);
            }
        }

        let having = match &s.having {
            Some(h) => Some(self.bind_bool(h, &scope, Clause::Select)?),
            None => None,
        };

        // ORDER BY: output column (ordinal or name) or an input expression.
        enum Key {
            Out(usize),
            Expr(Expr),
        }
        let mut keys = Vec::new();
        for o in order {
            if o.with_fill.is_some() {
                return Err(SqlError::unsupported("WITH FILL"));
            }
            let desc = match o.options.sort {
                None | Some(ast::OrderBySort::Asc) => false,
                Some(ast::OrderBySort::Desc) => true,
                Some(ast::OrderBySort::Using(_)) => {
                    return Err(SqlError::unsupported("ORDER BY USING"));
                }
            };
            let nulls_first = o.options.nulls_first.unwrap_or(desc);
            let key = match self.order_ref(&o.expr, &items)? {
                Some(i) => Key::Out(i),
                None => {
                    let b = self.bind_expr(&o.expr, &scope, Clause::Select)?;
                    known(&b.ty)?;
                    Key::Expr(b.expr)
                }
            };
            keys.push((key, desc, nulls_first));
        }

        let aggregated = !groups.is_empty() || !self.aggs.is_empty() || having.is_some();
        let mut columns = Vec::new();
        let mut exprs = Vec::new();
        for it in &items {
            let ty = match &it.expr.ty {
                Ty::Unknown => {
                    // Output literals resolve to text.
                    DataType::Text
                }
                t => known(t)?,
            };
            let mut e = match &it.expr.ty {
                Ty::Unknown => coerce(it.expr.clone(), DataType::Text, Coercion::Implicit)?,
                _ => it.expr.expr.clone(),
            };
            if aggregated {
                e = self.finish_agg(&e, &groups, &scope)?;
            }
            exprs.push(e);
            columns.push(OutCol {
                name: it.name.clone(),
                ty,
            });
        }
        let visible = exprs.len();
        let mut sort_keys = Vec::new();
        for (k, desc, nulls_first) in keys {
            let col = match k {
                Key::Out(i) => i,
                Key::Expr(e) => {
                    let e = if aggregated {
                        self.finish_agg(&e, &groups, &scope)?
                    } else {
                        e
                    };
                    match exprs.iter().position(|x| *x == e) {
                        Some(i) => i,
                        None => {
                            if distinct {
                                return Err(err(
                                    code::INVALID_COLUMN_REFERENCE,
                                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                                ));
                            }
                            exprs.push(e);
                            exprs.len() - 1
                        }
                    }
                }
            };
            sort_keys.push(SortKey {
                expr: Expr::Column(col),
                desc,
                nulls_first,
            });
        }

        if aggregated {
            let having = match having {
                Some(h) => Some(self.finish_agg(&h, &groups, &scope)?),
                None => None,
            };
            plan = Plan::Aggregate {
                input: Box::new(plan),
                group_by: groups,
                aggs: std::mem::take(&mut self.aggs),
            };
            if let Some(pred) = having {
                plan = Plan::Filter {
                    input: Box::new(plan),
                    pred,
                };
            }
        }
        let hidden = exprs.len() > visible;
        plan = Plan::Project {
            input: Box::new(plan),
            exprs,
        };
        if distinct {
            plan = Plan::Distinct {
                input: Box::new(plan),
            };
        }
        if !sort_keys.is_empty() {
            plan = Plan::Sort {
                input: Box::new(plan),
                keys: sort_keys,
            };
        }
        if hidden {
            plan = Plan::Project {
                input: Box::new(plan),
                exprs: (0..visible).map(Expr::Column).collect(),
            };
        }
        Ok((plan, columns))
    }

    /// For GROUP BY: an output column referenced by ordinal, or by name
    /// when no input column has that name.
    fn output_ref(&self, e: &ast::Expr, items: &[Item], scope: &Scope) -> Result<Option<usize>> {
        match e {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => ordinal(n, items.len(), "GROUP BY").map(Some),
                _ => Ok(None),
            },
            ast::Expr::Identifier(i) => {
                let name = ident(i);
                if scope.cols.iter().any(|c| c.name == name) {
                    return Ok(None);
                }
                Ok(items.iter().position(|it| it.name == name))
            }
            _ => Ok(None),
        }
    }

    /// For ORDER BY: an output column referenced by ordinal or by name
    /// (output names take precedence over input columns).
    fn order_ref(&self, e: &ast::Expr, items: &[Item]) -> Result<Option<usize>> {
        match e {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => ordinal(n, items.len(), "ORDER BY").map(Some),
                _ => Ok(None),
            },
            ast::Expr::Identifier(i) => {
                let name = ident(i);
                let hits: Vec<usize> = items
                    .iter()
                    .enumerate()
                    .filter(|(_, it)| it.name == name)
                    .map(|(k, _)| k)
                    .collect();
                match hits.as_slice() {
                    [] => Ok(None),
                    [k] => Ok(Some(*k)),
                    [first, rest @ ..] => {
                        // Duplicates referring to the same expression are fine.
                        if rest
                            .iter()
                            .all(|k| items[*k].expr.expr == items[*first].expr.expr)
                        {
                            Ok(Some(*first))
                        } else {
                            Err(err(
                                code::AMBIGUOUS_COLUMN,
                                format!("ORDER BY \"{name}\" is ambiguous"),
                            ))
                        }
                    }
                }
            }
            _ => Ok(None),
        }
    }

    /// Rewrites an expression over the input row into one over the
    /// aggregation output (group values, then aggregates).
    fn finish_agg(&self, e: &Expr, groups: &[Expr], scope: &Scope) -> Result<Expr> {
        if let Some(i) = groups.iter().position(|g| g == e) {
            return Ok(Expr::Column(i));
        }
        match e {
            Expr::Column(c) if *c >= AGG_MARK => Ok(Expr::Column(groups.len() + c - AGG_MARK)),
            Expr::Column(c) => {
                let col = &scope.cols[*c];
                Err(err(
                    code::GROUPING_ERROR,
                    format!(
                        "column \"{}.{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                        col.table, col.name
                    ),
                ))
            }
            other => other.rebuild(&mut |c| self.finish_agg(c, groups, scope)),
        }
    }

    // ---- DML ----

    fn bind_insert(&mut self, i: &ast::Insert) -> Result<Bound> {
        if i.or.is_some()
            || i.ignore
            || i.overwrite
            || !i.assignments.is_empty()
            || i.partitioned.is_some()
            || !i.after_columns.is_empty()
            || i.on.is_some()
            || i.returning.is_some()
            || i.table_alias.is_some()
        {
            return Err(SqlError::unsupported(format!("statement {i}")));
        }
        let ast::TableObject::TableName(name) = &i.table else {
            return Err(SqlError::unsupported(format!("statement {i}")));
        };
        let table = self.open_target(name)?;
        let targets: Vec<usize> = if i.columns.is_empty() {
            (0..table.columns.len()).collect()
        } else {
            let mut t = Vec::new();
            for c in &i.columns {
                let n = object_name(c)?;
                let idx = table
                    .columns
                    .iter()
                    .position(|col| col.name == n)
                    .ok_or_else(|| {
                        err(
                            code::UNDEFINED_COLUMN,
                            format!(
                                "column \"{n}\" of relation \"{}\" does not exist",
                                table.name
                            ),
                        )
                    })?;
                if t.contains(&idx) {
                    return Err(err(
                        code::DUPLICATE_COLUMN,
                        format!("column \"{n}\" specified more than once"),
                    ));
                }
                t.push(idx);
            }
            t
        };
        let Some(src) = &i.source else {
            return Err(SqlError::unsupported("INSERT DEFAULT VALUES"));
        };
        let check_width = |n: usize| -> Result<()> {
            if n > targets.len() {
                Err(err(
                    code::SYNTAX_ERROR,
                    "INSERT has more expressions than target columns",
                ))
            } else if n < targets.len() {
                Err(err(
                    code::SYNTAX_ERROR,
                    "INSERT has more target columns than expressions",
                ))
            } else {
                Ok(())
            }
        };
        let assign = |b: BExpr, col: usize| -> Result<Expr> {
            let c = &table.columns[col];
            let found = b.ty.name();
            coerce(b, c.ty, Coercion::Assignment).map_err(|e| {
                if e.code == code::DATATYPE_MISMATCH {
                    err(
                        code::DATATYPE_MISMATCH,
                        format!(
                            "column \"{}\" is of type {} but expression is of type {found}",
                            c.name, c.ty
                        ),
                    )
                } else {
                    e
                }
            })
        };
        let full_row = |vals: Vec<Expr>| -> Vec<Expr> {
            let mut row = vec![Expr::Const(Value::Null); table.columns.len()];
            for (v, &col) in vals.into_iter().zip(&targets) {
                row[col] = v;
            }
            row
        };
        let simple_values = match src.body.as_ref() {
            ast::SetExpr::Values(v)
                if src.order_by.is_none() && src.limit_clause.is_none() && src.with.is_none() =>
            {
                Some(v)
            }
            _ => None,
        };
        let source = if let Some(v) = simple_values {
            // VALUES rows are coerced straight to the target columns.
            let mut rows = Vec::new();
            for r in &v.rows {
                check_width(r.len())?;
                let mut vals = Vec::new();
                for (e, &col) in r.iter().zip(&targets) {
                    let b = self.bind_expr(e, &Scope::default(), Clause::Values)?;
                    vals.push(assign(b, col)?);
                }
                rows.push(full_row(vals));
            }
            Plan::Values {
                rows,
                width: table.columns.len(),
            }
        } else {
            let (plan, cols) = self.bind_query(src)?;
            check_width(cols.len())?;
            let vals = cols
                .iter()
                .enumerate()
                .zip(&targets)
                .map(|((k, c), &col)| assign(BExpr::known(Expr::Column(k), c.ty), col))
                .collect::<Result<Vec<_>>>()?;
            Plan::Project {
                input: Box::new(plan),
                exprs: full_row(vals),
            }
        };
        Ok(Bound::Insert { table, source })
    }

    fn table_scope(table: &RelationInfo, label: &str) -> Scope {
        Scope {
            cols: table
                .columns
                .iter()
                .map(|c| ScopeCol {
                    table: label.to_owned(),
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect(),
        }
    }

    fn bind_update(&mut self, u: &ast::Update) -> Result<Bound> {
        if u.from.is_some()
            || u.returning.is_some()
            || !u.table.joins.is_empty()
            || u.or.is_some()
            || !u.order_by.is_empty()
            || u.limit.is_some()
        {
            return Err(SqlError::unsupported(format!("statement {u}")));
        }
        let ast::TableFactor::Table { name, alias, .. } = &u.table.relation else {
            return Err(SqlError::unsupported(format!("statement {u}")));
        };
        let table = self.open_target(name)?;
        let label = alias
            .as_ref()
            .map_or(table.name.clone(), |a| ident(&a.name));
        let scope = Self::table_scope(&table, &label);
        let mut assignments: Vec<(usize, Expr)> = Vec::new();
        for a in &u.assignments {
            let ast::AssignmentTarget::ColumnName(cn) = &a.target else {
                return Err(SqlError::unsupported("multi-column assignment"));
            };
            let n = object_name(cn)?;
            let col = table
                .columns
                .iter()
                .position(|c| c.name == n)
                .ok_or_else(|| {
                    err(
                        code::UNDEFINED_COLUMN,
                        format!(
                            "column \"{n}\" of relation \"{}\" does not exist",
                            table.name
                        ),
                    )
                })?;
            if assignments.iter().any(|(c, _)| *c == col) {
                return Err(err(
                    code::SYNTAX_ERROR,
                    format!("multiple assignments to same column \"{n}\""),
                ));
            }
            let b = self.bind_expr(&a.value, &scope, Clause::Update)?;
            let c = &table.columns[col];
            let found = b.ty.name();
            let e = coerce(b, c.ty, Coercion::Assignment).map_err(|e| {
                if e.code == code::DATATYPE_MISMATCH {
                    err(
                        code::DATATYPE_MISMATCH,
                        format!(
                            "column \"{}\" is of type {} but expression is of type {found}",
                            c.name, c.ty
                        ),
                    )
                } else {
                    e
                }
            })?;
            assignments.push((col, e));
        }
        let filter = match &u.selection {
            Some(w) => Some(self.bind_bool(w, &scope, Clause::Where)?),
            None => None,
        };
        Ok(Bound::Update {
            scan: Plan::Scan {
                table: table.clone(),
                filter,
            },
            table,
            assignments,
        })
    }

    fn bind_delete(&mut self, d: &ast::Delete) -> Result<Bound> {
        let from = match &d.from {
            ast::FromTable::WithFromKeyword(f) | ast::FromTable::WithoutKeyword(f) => f,
        };
        if !d.tables.is_empty()
            || d.using.is_some()
            || d.returning.is_some()
            || !d.order_by.is_empty()
            || d.limit.is_some()
            || from.len() != 1
            || !from[0].joins.is_empty()
        {
            return Err(SqlError::unsupported(format!("statement {d}")));
        }
        let ast::TableFactor::Table { name, alias, .. } = &from[0].relation else {
            return Err(SqlError::unsupported(format!("statement {d}")));
        };
        let table = self.open_target(name)?;
        let label = alias
            .as_ref()
            .map_or(table.name.clone(), |a| ident(&a.name));
        let scope = Self::table_scope(&table, &label);
        let filter = match &d.selection {
            Some(w) => Some(self.bind_bool(w, &scope, Clause::Where)?),
            None => None,
        };
        Ok(Bound::Delete {
            scan: Plan::Scan {
                table: table.clone(),
                filter,
            },
            table,
        })
    }

    // ---- expressions ----

    fn bind_bool(&mut self, e: &ast::Expr, scope: &Scope, clause: Clause) -> Result<Expr> {
        let b = self.bind_expr(e, scope, clause)?;
        let what = match clause {
            Clause::On => "JOIN/ON".to_owned(),
            Clause::Select => "HAVING".to_owned(),
            c => c.name().to_owned(),
        };
        to_bool(b, &what)
    }

    fn resolve_column(&self, parts: &[String], scope: &Scope) -> Result<BExpr> {
        let (table, name) = match parts {
            [n] => (None, n),
            [t, n] => (Some(t), n),
            _ => {
                return Err(SqlError::unsupported(format!(
                    "column reference {}",
                    parts.join(".")
                )));
            }
        };
        if let Some(t) = table
            && !scope.cols.iter().any(|c| &c.table == t)
        {
            return Err(err(
                code::UNDEFINED_TABLE,
                format!("missing FROM-clause entry for table \"{t}\""),
            ));
        }
        let hits: Vec<usize> = scope
            .cols
            .iter()
            .enumerate()
            .filter(|(_, c)| &c.name == name && table.is_none_or(|t| &c.table == t))
            .map(|(i, _)| i)
            .collect();
        let full = parts.join(".");
        match hits.as_slice() {
            [i] => Ok(BExpr::known(Expr::Column(*i), scope.cols[*i].ty)),
            [] => Err(err(
                code::UNDEFINED_COLUMN,
                format!("column \"{full}\" does not exist"),
            )),
            _ => Err(err(
                code::AMBIGUOUS_COLUMN,
                format!("column reference \"{full}\" is ambiguous"),
            )),
        }
    }

    fn bind_expr(&mut self, e: &ast::Expr, scope: &Scope, clause: Clause) -> Result<BExpr> {
        use ast::Expr as E;
        Ok(match e {
            E::Identifier(i) => self.resolve_column(&[ident(i)], scope)?,
            E::CompoundIdentifier(parts) => {
                let p: Vec<String> = parts.iter().map(ident).collect();
                self.resolve_column(&p, scope)?
            }
            E::Nested(inner) => self.bind_expr(inner, scope, clause)?,
            E::Value(v) => literal(&v.value, false)?,
            E::TypedString(ts) => {
                let ty = data_type(&ts.data_type)?;
                let s = match &ts.value.value {
                    ast::Value::SingleQuotedString(s) => s.clone(),
                    other => return Err(SqlError::unsupported(format!("literal {other}"))),
                };
                BExpr::known(
                    coerce(
                        BExpr {
                            expr: Expr::Const(Value::Text(s)),
                            ty: Ty::Unknown,
                        },
                        ty,
                        Coercion::Explicit,
                    )?,
                    ty,
                )
            }
            E::UnaryOp { op, expr } => match op {
                ast::UnaryOperator::Minus => {
                    if let E::Value(v) = expr.as_ref()
                        && let ast::Value::Number(..) = v.value
                    {
                        return literal(&v.value, true);
                    }
                    let b = self.bind_expr(expr, scope, clause)?;
                    match b.ty {
                        Ty::Known(t) if numeric_rank(t).is_some() => {
                            BExpr::known(Expr::Neg(Box::new(b.expr)), t)
                        }
                        Ty::Numeric(_) => return Err(numeric_unsupported()),
                        t => {
                            return Err(err(
                                code::UNDEFINED_FUNCTION,
                                format!("operator does not exist: - {}", t.name()),
                            ));
                        }
                    }
                }
                ast::UnaryOperator::Plus => {
                    let b = self.bind_expr(expr, scope, clause)?;
                    match &b.ty {
                        Ty::Known(t) if numeric_rank(*t).is_some() => b,
                        Ty::Numeric(_) => b,
                        t => {
                            return Err(err(
                                code::UNDEFINED_FUNCTION,
                                format!("operator does not exist: + {}", t.name()),
                            ));
                        }
                    }
                }
                ast::UnaryOperator::Not => {
                    let b = self.bind_expr(expr, scope, clause)?;
                    BExpr::known(Expr::Not(Box::new(to_bool(b, "NOT")?)), DataType::Bool)
                }
                other => return Err(SqlError::unsupported(format!("operator {other}"))),
            },
            E::BinaryOp { left, op, right } => {
                let l = self.bind_expr(left, scope, clause)?;
                let r = self.bind_expr(right, scope, clause)?;
                binary(op, l, r)?
            }
            E::IsNull(a) | E::IsNotNull(a) => {
                let b = self.bind_expr(a, scope, clause)?;
                BExpr::known(
                    Expr::IsNull {
                        arg: Box::new(b.expr),
                        negated: matches!(e, E::IsNotNull(_)),
                    },
                    DataType::Bool,
                )
            }
            E::IsTrue(a) | E::IsNotTrue(a) | E::IsFalse(a) | E::IsNotFalse(a) => {
                let b = to_bool(self.bind_expr(a, scope, clause)?, "IS")?;
                let b = if matches!(e, E::IsFalse(_) | E::IsNotFalse(_)) {
                    Expr::Not(Box::new(b))
                } else {
                    b
                };
                let is = Expr::Coalesce(vec![b, Expr::Const(Value::Bool(false))]);
                let is = if matches!(e, E::IsNotTrue(_) | E::IsNotFalse(_)) {
                    Expr::Not(Box::new(is))
                } else {
                    is
                };
                BExpr::known(is, DataType::Bool)
            }
            E::IsDistinctFrom(a, b) | E::IsNotDistinctFrom(a, b) => {
                let l = self.bind_expr(a, scope, clause)?;
                let r = self.bind_expr(b, scope, clause)?;
                let eq = binary(&ast::BinaryOperator::Eq, l.clone(), r.clone())?.expr;
                let ln = Expr::IsNull {
                    arg: Box::new(match &eq {
                        Expr::Binary { l, .. } => (**l).clone(),
                        _ => l.expr.clone(),
                    }),
                    negated: false,
                };
                let rn = Expr::IsNull {
                    arg: Box::new(match &eq {
                        Expr::Binary { r, .. } => (**r).clone(),
                        _ => r.expr.clone(),
                    }),
                    negated: false,
                };
                // NOT DISTINCT: both NULL, or neither NULL and equal.
                let not_distinct = Expr::Case {
                    whens: vec![
                        (
                            Expr::And(Box::new(ln.clone()), Box::new(rn.clone())),
                            Expr::Const(Value::Bool(true)),
                        ),
                        (
                            Expr::Or(Box::new(ln), Box::new(rn)),
                            Expr::Const(Value::Bool(false)),
                        ),
                    ],
                    otherwise: Some(Box::new(eq)),
                };
                let out = if matches!(e, E::IsDistinctFrom(..)) {
                    Expr::Not(Box::new(not_distinct))
                } else {
                    not_distinct
                };
                BExpr::known(out, DataType::Bool)
            }
            E::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let x = self.bind_expr(expr, scope, clause)?;
                let lo = self.bind_expr(low, scope, clause)?;
                let hi = self.bind_expr(high, scope, clause)?;
                let ge = binary(&ast::BinaryOperator::GtEq, x.clone(), lo)?.expr;
                let le = binary(&ast::BinaryOperator::LtEq, x, hi)?.expr;
                let both = Expr::And(Box::new(ge), Box::new(le));
                BExpr::known(
                    if *negated {
                        Expr::Not(Box::new(both))
                    } else {
                        both
                    },
                    DataType::Bool,
                )
            }
            E::In {
                expr,
                target: ast::InTarget::List(list),
                negated,
                ..
            } => {
                let x = self.bind_expr(expr, scope, clause)?;
                let items = list
                    .iter()
                    .map(|i| self.bind_expr(i, scope, clause))
                    .collect::<Result<Vec<_>>>()?;
                let mut tys = vec![&x.ty];
                tys.extend(items.iter().map(|i| &i.ty));
                let ct = common_type(&tys, "IN")?;
                let ty = comparison_type(ct)?;
                BExpr::known(
                    Expr::InList {
                        arg: Box::new(coerce(x, ty, Coercion::Implicit)?),
                        list: items
                            .into_iter()
                            .map(|i| coerce(i, ty, Coercion::Implicit))
                            .collect::<Result<_>>()?,
                        negated: *negated,
                    },
                    DataType::Bool,
                )
            }
            E::Like {
                negated,
                any: false,
                expr,
                pattern,
                escape_char: None,
            }
            | E::ILike {
                negated,
                any: false,
                expr,
                pattern,
                escape_char: None,
            } => {
                let x = to_text(self.bind_expr(expr, scope, clause)?, "LIKE")?;
                let p = to_text(self.bind_expr(pattern, scope, clause)?, "LIKE")?;
                BExpr::known(
                    Expr::Like {
                        arg: Box::new(x),
                        pattern: Box::new(p),
                        negated: *negated,
                        case_insensitive: matches!(e, E::ILike { .. }),
                    },
                    DataType::Bool,
                )
            }
            E::Cast {
                kind: ast::CastKind::Cast | ast::CastKind::DoubleColon,
                expr,
                data_type: dt,
                format: None,
            } => {
                let to = data_type(dt)?;
                let b = self.bind_expr(expr, scope, clause)?;
                BExpr::known(coerce(b, to, Coercion::Explicit)?, to)
            }
            E::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.bind_case(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                scope,
                clause,
            )?,
            E::Function(f) => self.bind_function(f, scope, clause)?,
            E::Subquery(_) | E::Exists { .. } | E::In { .. } => {
                return Err(SqlError::unsupported("subqueries"));
            }
            other => return Err(SqlError::unsupported(format!("expression {other}"))),
        })
    }

    fn bind_case(
        &mut self,
        operand: Option<&ast::Expr>,
        conditions: &[ast::CaseWhen],
        else_result: Option<&ast::Expr>,
        scope: &Scope,
        clause: Clause,
    ) -> Result<BExpr> {
        let op = match operand {
            Some(o) => Some(self.bind_expr(o, scope, clause)?),
            None => None,
        };
        let mut conds = Vec::new();
        let mut results = Vec::new();
        for w in conditions {
            let c = self.bind_expr(&w.condition, scope, clause)?;
            let c = match &op {
                Some(o) => binary(&ast::BinaryOperator::Eq, o.clone(), c)?.expr,
                None => to_bool(c, "CASE/WHEN")?,
            };
            conds.push(c);
            results.push(self.bind_expr(&w.result, scope, clause)?);
        }
        let otherwise = match else_result {
            Some(e) => Some(self.bind_expr(e, scope, clause)?),
            None => None,
        };
        let mut tys: Vec<&Ty> = results.iter().map(|r| &r.ty).collect();
        if let Some(o) = &otherwise {
            tys.push(&o.ty);
        }
        let ty = known(&common_type(&tys, "CASE")?)?;
        let whens = conds
            .into_iter()
            .zip(results)
            .map(|(c, r)| Ok((c, coerce(r, ty, Coercion::Implicit)?)))
            .collect::<Result<Vec<_>>>()?;
        let otherwise = match otherwise {
            Some(o) => Some(Box::new(coerce(o, ty, Coercion::Implicit)?)),
            None => None,
        };
        Ok(BExpr::known(Expr::Case { whens, otherwise }, ty))
    }

    fn bind_function(&mut self, f: &ast::Function, scope: &Scope, clause: Clause) -> Result<BExpr> {
        let name = object_name(&f.name)?;
        if f.filter.is_some() || f.over.is_some() || !f.within_group.is_empty() {
            return Err(SqlError::unsupported(format!("function call {f}")));
        }
        let ast::FunctionArguments::List(list) = &f.args else {
            return Err(SqlError::unsupported(format!("function call {f}")));
        };
        if !list.clauses.is_empty() {
            return Err(SqlError::unsupported(format!("function call {f}")));
        }
        let distinct = matches!(
            list.duplicate_treatment,
            Some(ast::DuplicateTreatment::Distinct)
        );
        let star = matches!(
            list.args.as_slice(),
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)]
        );
        let mut arg_asts = Vec::new();
        if !star {
            for a in &list.args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => arg_asts.push(e),
                    _ => return Err(SqlError::unsupported(format!("function call {f}"))),
                }
            }
        }
        if matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
            return self.bind_aggregate(&name, star, distinct, &arg_asts, scope, clause);
        }
        if star || distinct {
            return Err(err(
                code::WRONG_OBJECT_TYPE,
                format!("{name}(*) specified, but {name} is not an aggregate function"),
            ));
        }
        let args = arg_asts
            .iter()
            .map(|a| self.bind_expr(a, scope, clause))
            .collect::<Result<Vec<_>>>()?;
        let no_func = |args: &[BExpr]| {
            let tys: Vec<String> = args.iter().map(|a| a.ty.name()).collect();
            err(
                code::UNDEFINED_FUNCTION,
                format!("function {name}({}) does not exist", tys.join(", ")),
            )
        };
        Ok(match (name.as_str(), args.len()) {
            ("lower" | "upper" | "length", 1) => {
                let a = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| SqlError::internal("arg"))?;
                if !matches!(a.ty, Ty::Unknown) && !matches!(a.ty, Ty::Known(t) if is_text(t)) {
                    return Err(no_func(&[a]));
                }
                let a = coerce(a, DataType::Text, Coercion::Implicit)?;
                let (func, ty) = match name.as_str() {
                    "lower" => (Func::Lower, DataType::Text),
                    "upper" => (Func::Upper, DataType::Text),
                    _ => (Func::Length, DataType::Int4),
                };
                BExpr::known(
                    Expr::Func {
                        func,
                        args: vec![a],
                    },
                    ty,
                )
            }
            ("abs", 1) => {
                let a = args
                    .into_iter()
                    .next()
                    .ok_or_else(|| SqlError::internal("arg"))?;
                let ty = match &a.ty {
                    Ty::Known(t) if numeric_rank(*t).is_some() => *t,
                    Ty::Numeric(_) => return Err(numeric_unsupported()),
                    Ty::Unknown => DataType::Float8,
                    _ => return Err(no_func(&[a])),
                };
                let a = coerce(a, ty, Coercion::Implicit)?;
                BExpr::known(
                    Expr::Func {
                        func: Func::Abs,
                        args: vec![a],
                    },
                    ty,
                )
            }
            ("coalesce", n) if n > 0 => {
                let tys: Vec<&Ty> = args.iter().map(|a| &a.ty).collect();
                let ty = known(&common_type(&tys, "COALESCE")?)?;
                let args = args
                    .into_iter()
                    .map(|a| coerce(a, ty, Coercion::Implicit))
                    .collect::<Result<Vec<_>>>()?;
                BExpr::known(Expr::Coalesce(args), ty)
            }
            ("nullif", 2) => {
                let mut it = args.into_iter();
                let (Some(a), Some(b)) = (it.next(), it.next()) else {
                    return Err(SqlError::internal("nullif args"));
                };
                let ty = known(&a.ty)?;
                let a_expr = coerce(a.clone(), ty, Coercion::Implicit)?;
                let eq = binary(&ast::BinaryOperator::Eq, a, b)?;
                BExpr::known(
                    Expr::Case {
                        whens: vec![(eq.expr, Expr::Const(Value::Null))],
                        otherwise: Some(Box::new(a_expr)),
                    },
                    ty,
                )
            }
            _ => return Err(no_func(&args)),
        })
    }

    fn bind_aggregate(
        &mut self,
        name: &str,
        star: bool,
        distinct: bool,
        args: &[&ast::Expr],
        scope: &Scope,
        clause: Clause,
    ) -> Result<BExpr> {
        if clause != Clause::Select {
            return Err(err(
                code::GROUPING_ERROR,
                format!("aggregate functions are not allowed in {}", clause.name()),
            ));
        }
        if self.in_agg {
            return Err(err(
                code::GROUPING_ERROR,
                "aggregate function calls cannot be nested",
            ));
        }
        let call = if star {
            if name != "count" {
                return Err(err(
                    code::UNDEFINED_FUNCTION,
                    format!("function {name}(*) does not exist"),
                ));
            }
            AggCall {
                func: AggFunc::CountStar,
                arg: None,
                distinct: false,
                ty: DataType::Int8,
            }
        } else {
            let [a] = args else {
                return Err(err(
                    code::UNDEFINED_FUNCTION,
                    format!(
                        "function {name} with {} arguments does not exist",
                        args.len()
                    ),
                ));
            };
            self.in_agg = true;
            let b = self.bind_expr(a, scope, Clause::Select);
            self.in_agg = false;
            let b = b?;
            let arg_ty = match &b.ty {
                Ty::Unknown if name == "count" => DataType::Text,
                Ty::Unknown => DataType::Text,
                Ty::Numeric(_) if name == "count" => DataType::Float8,
                t => known(t)?,
            };
            let no_func = || {
                err(
                    code::UNDEFINED_FUNCTION,
                    format!("function {name}({arg_ty}) does not exist"),
                )
            };
            let (func, ty) = match name {
                "count" => (AggFunc::Count, DataType::Int8),
                "sum" => match arg_ty {
                    DataType::Int4 => (AggFunc::Sum, DataType::Int8),
                    DataType::Float8 => (AggFunc::Sum, DataType::Float8),
                    DataType::Int8 => return Err(numeric_unsupported()),
                    _ => return Err(no_func()),
                },
                "avg" => match arg_ty {
                    DataType::Float8 => (AggFunc::Avg, DataType::Float8),
                    DataType::Int4 | DataType::Int8 => return Err(numeric_unsupported()),
                    _ => return Err(no_func()),
                },
                "min" | "max" => {
                    if arg_ty == DataType::Bool {
                        return Err(no_func());
                    }
                    let f = if name == "min" {
                        AggFunc::Min
                    } else {
                        AggFunc::Max
                    };
                    let ty = if is_text(arg_ty) {
                        DataType::Text
                    } else {
                        arg_ty
                    };
                    (f, ty)
                }
                _ => return Err(no_func()),
            };
            AggCall {
                func,
                arg: Some(coerce(b, arg_ty, Coercion::Implicit)?),
                distinct,
                ty,
            }
        };
        let ty = call.ty;
        let i = match self.aggs.iter().position(|c| *c == call) {
            Some(i) => i,
            None => {
                self.aggs.push(call);
                self.aggs.len() - 1
            }
        };
        Ok(BExpr::known(Expr::Column(AGG_MARK + i), ty))
    }
}

fn wildcard_plain(o: &ast::WildcardAdditionalOptions) -> bool {
    o.opt_ilike.is_none()
        && o.opt_exclude.is_none()
        && o.opt_except.is_none()
        && o.opt_replace.is_none()
        && o.opt_rename.is_none()
}

fn ordinal(n: &str, len: usize, what: &str) -> Result<usize> {
    match n.parse::<usize>() {
        Ok(k) if (1..=len).contains(&k) => Ok(k - 1),
        _ => Err(err(
            code::INVALID_COLUMN_REFERENCE,
            format!("{what} position {n} is not in select list"),
        )),
    }
}

fn merge_scopes(mut a: Scope, b: Scope) -> Result<Scope> {
    for c in &b.cols {
        if a.cols.iter().any(|x| x.table == c.table) {
            return Err(err(
                code::DUPLICATE_ALIAS,
                format!("table name \"{}\" specified more than once", c.table),
            ));
        }
    }
    a.cols.extend(b.cols);
    Ok(a)
}

/// The name PostgreSQL gives an unaliased output column (FigureColname).
fn column_name(e: &ast::Expr) -> String {
    use ast::Expr as E;
    match e {
        E::Identifier(i) => ident(i),
        E::CompoundIdentifier(p) => p.last().map(ident).unwrap_or_default(),
        E::Nested(inner) => column_name(inner),
        E::Function(f) => f
            .name
            .0
            .last()
            .and_then(|p| p.as_ident())
            .map(ident)
            .unwrap_or_default(),
        E::Cast {
            expr, data_type, ..
        } => match column_name(expr).as_str() {
            "?column?" => match data_type_name(data_type) {
                Some(n) => n.to_owned(),
                None => "?column?".to_owned(),
            },
            n => n.to_owned(),
        },
        E::TypedString(ts) => data_type_name(&ts.data_type)
            .unwrap_or("?column?")
            .to_owned(),
        E::Case { .. } => "case".to_owned(),
        _ => "?column?".to_owned(),
    }
}

/// PostgreSQL's internal type name (as used for column names of casts).
fn data_type_name(t: &ast::DataType) -> Option<&'static str> {
    Some(match data_type(t).ok()? {
        DataType::Int4 => "int4",
        DataType::Int8 => "int8",
        DataType::Bool => "bool",
        DataType::Float8 => "float8",
        DataType::Timestamp => "timestamp",
        DataType::Text => "text",
        DataType::Varchar(_) => "varchar",
    })
}

fn literal(v: &ast::Value, negate: bool) -> Result<BExpr> {
    Ok(match v {
        ast::Value::Number(n, _) => {
            let text = if negate { format!("-{n}") } else { n.clone() };
            let integral = n.bytes().all(|b| b.is_ascii_digit());
            if integral {
                if let Ok(x) = text.parse::<i32>() {
                    return Ok(BExpr::known(Expr::Const(Value::Int4(x)), DataType::Int4));
                }
                if let Ok(x) = text.parse::<i64>() {
                    return Ok(BExpr::known(Expr::Const(Value::Int8(x)), DataType::Int8));
                }
            }
            let f: f64 = text
                .parse()
                .map_err(|_| err(code::SYNTAX_ERROR, format!("invalid number {text}")))?;
            BExpr {
                expr: Expr::Const(Value::Float8(f)),
                ty: Ty::Numeric(text),
            }
        }
        ast::Value::SingleQuotedString(s) | ast::Value::EscapedStringLiteral(s) => BExpr {
            expr: Expr::Const(Value::Text(s.clone())),
            ty: Ty::Unknown,
        },
        ast::Value::Boolean(b) => BExpr::known(Expr::Const(Value::Bool(*b)), DataType::Bool),
        ast::Value::Null => BExpr {
            expr: Expr::Const(Value::Null),
            ty: Ty::Unknown,
        },
        other => return Err(SqlError::unsupported(format!("literal {other}"))),
    })
}

fn to_bool(b: BExpr, what: &str) -> Result<Expr> {
    match &b.ty {
        Ty::Known(DataType::Bool) | Ty::Unknown => coerce(b, DataType::Bool, Coercion::Implicit),
        t => Err(err(
            code::DATATYPE_MISMATCH,
            format!(
                "argument of {what} must be type boolean, not type {}",
                t.name()
            ),
        )),
    }
}

fn to_text(b: BExpr, what: &str) -> Result<Expr> {
    match &b.ty {
        Ty::Unknown => coerce(b, DataType::Text, Coercion::Implicit),
        Ty::Known(t) if is_text(*t) => Ok(b.expr),
        t => Err(err(
            code::UNDEFINED_FUNCTION,
            format!("operator does not exist: {} ~~ text ({what})", t.name()),
        )),
    }
}

/// The type both sides of a comparison are converted to.
fn comparison_type(t: Ty) -> Result<DataType> {
    match t {
        Ty::Known(k) if is_text(k) => Ok(DataType::Text),
        Ty::Known(k) => Ok(k),
        Ty::Unknown => Ok(DataType::Text),
        Ty::Numeric(_) => Ok(DataType::Float8),
    }
}

fn binary(op: &ast::BinaryOperator, l: BExpr, r: BExpr) -> Result<BExpr> {
    use ast::BinaryOperator as B;
    let bop = match op {
        B::Plus => BinOp::Add,
        B::Minus => BinOp::Sub,
        B::Multiply => BinOp::Mul,
        B::Divide => BinOp::Div,
        B::Modulo => BinOp::Mod,
        B::Eq => BinOp::Eq,
        B::NotEq => BinOp::NotEq,
        B::Lt => BinOp::Lt,
        B::LtEq => BinOp::LtEq,
        B::Gt => BinOp::Gt,
        B::GtEq => BinOp::GtEq,
        B::StringConcat => BinOp::Concat,
        B::And | B::Or => {
            let what = if matches!(op, B::And) { "AND" } else { "OR" };
            let a = Box::new(to_bool(l, what)?);
            let b = Box::new(to_bool(r, what)?);
            let e = if matches!(op, B::And) {
                Expr::And(a, b)
            } else {
                Expr::Or(a, b)
            };
            return Ok(BExpr::known(e, DataType::Bool));
        }
        other => return Err(SqlError::unsupported(format!("operator {other}"))),
    };
    let no_op = |l: &Ty, r: &Ty| {
        err(
            code::UNDEFINED_FUNCTION,
            format!(
                "operator does not exist: {} {} {}",
                l.name(),
                bop.symbol(),
                r.name()
            ),
        )
    };
    let mk = |l: Expr, r: Expr| Expr::Binary {
        op: bop,
        l: Box::new(l),
        r: Box::new(r),
    };
    if bop == BinOp::Concat {
        // text || anything (anytextcat); unknown || unknown is text.
        let textish = |t: &Ty| matches!(t, Ty::Unknown) || matches!(t, Ty::Known(k) if is_text(*k));
        if !textish(&l.ty) && !textish(&r.ty) {
            return Err(no_op(&l.ty, &r.ty));
        }
        let lt = coerce(l, DataType::Text, Coercion::Explicit)?;
        let rt = coerce(r, DataType::Text, Coercion::Explicit)?;
        return Ok(BExpr::known(mk(lt, rt), DataType::Text));
    }
    // Operand type: an unknown side takes the other side's type.
    let ty = match (&l.ty, &r.ty) {
        (Ty::Unknown, Ty::Unknown) => {
            if bop.is_comparison() {
                Ty::Known(DataType::Text)
            } else {
                return Err(err(
                    code::UNDEFINED_FUNCTION,
                    format!("operator is not unique: unknown {} unknown", bop.symbol()),
                ));
            }
        }
        (Ty::Unknown, t) | (t, Ty::Unknown) => t.clone(),
        (Ty::Numeric(_), Ty::Numeric(_)) => {
            if bop.is_comparison() {
                Ty::Known(DataType::Float8)
            } else {
                return Err(numeric_unsupported());
            }
        }
        (Ty::Numeric(_), Ty::Known(k)) | (Ty::Known(k), Ty::Numeric(_)) => match numeric_rank(*k) {
            Some(3) => Ty::Known(DataType::Float8),
            // integer vs numeric: compared as float8 (same answer for
            // literals within float8 precision); arithmetic is numeric.
            Some(_) if bop.is_comparison() => Ty::Known(DataType::Float8),
            Some(_) => return Err(numeric_unsupported()),
            None => return Err(no_op(&l.ty, &r.ty)),
        },
        (Ty::Known(a), Ty::Known(b)) => {
            let (a, b) = (*a, *b);
            if is_text(a) && is_text(b) {
                Ty::Known(DataType::Text)
            } else if let (Some(x), Some(y)) = (numeric_rank(a), numeric_rank(b)) {
                Ty::Known(if x >= y { a } else { b })
            } else if a == b {
                Ty::Known(a)
            } else {
                return Err(no_op(&l.ty, &r.ty));
            }
        }
    };
    let ty = comparison_type(ty)?;
    let valid = if bop.is_comparison() {
        true
    } else {
        match ty {
            DataType::Int4 | DataType::Int8 => true,
            DataType::Float8 => bop != BinOp::Mod,
            _ => false,
        }
    };
    if !valid {
        let shown = Ty::Known(ty);
        return Err(no_op(&shown, &shown));
    }
    let result = if bop.is_comparison() {
        DataType::Bool
    } else {
        ty
    };
    Ok(BExpr::known(
        mk(
            coerce(l, ty, Coercion::Implicit)?,
            coerce(r, ty, Coercion::Implicit)?,
        ),
        result,
    ))
}
